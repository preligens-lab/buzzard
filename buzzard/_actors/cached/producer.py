from buzzard._actors.message import Msg

import collections

class ActorProducer(object):
    """Actor that takes care of waiting for cache tiles reads and launching resamplings"""

    def __init__(self, raster):
        self._raster = raster
        self._alive = True

        self._produce_per_query = collections.defaultdict(dict) # type: Mapping[CachedQueryInfos, Mapping[int, _ProdArray]]
        self.address = '/Raster{}/Producer'.format(self._raster.uid)

        self._debug_stack = []

    def _debug_push(self, *args):
        self._debug_stack.append(args)
        self._debug_stack = self._debug_stack[-1000:]

    @property
    def alive(self):
        return self._alive

    # ******************************************************************************************* **
    def receive_make_this_array(self, qi, prod_idx):
        """Receive message: Start making this array

        Parameters
        ----------
        qi: _actors.cached.query_infos.QueryInfos
        prod_idx: int
        """
        msgs = []

        self._debug_push(
            qi, prod_idx, 'receive_make_this_array',
        )

        pi = qi.prod[prod_idx] # type: CacheProduceInfos
        assert pi.share_area is (len(pi.cache_fps) != 0)

        if pi.share_area:
            # If this prod_idx requires some cache file reads (this is the case most of the time)
            msgs += [Msg(
                'CacheExtractor', 'sample_those_cache_files_to_an_array', qi, prod_idx,
            )]

            self._debug_push(
                qi, prod_idx, 'receive_make_this_array',
                'sample_those_cache_files_to_an_array',
            )

        for resample_fp in pi.resample_fps:
            sample_fp = pi.resample_sample_dep_fp[resample_fp]
            if sample_fp is None:
                # Start the 'resampling' step of the resample_fp fully outside of raster
                assert (
                    resample_fp not in pi.resample_cache_deps_fps or
                    len(pi.resample_cache_deps_fps[resample_fp]) == 0
                )
                msgs += [Msg(
                    'Resampler', 'resample_and_accumulate',
                    qi, prod_idx, None, resample_fp, None,
                )]

                self._debug_push(
                    qi, prod_idx, 'receive_make_this_array',
                    'resample_and_accumulate', 'no share area', resample_fp,
                )

        self._produce_per_query[qi][prod_idx] = _ProdArray(pi)
        return msgs

    def receive_sampled_a_cache_file_to_the_array(self, qi, prod_idx, cache_fp, array):
        """Receive message: A cache file was read for that output array

        Parameters
        ----------
        qi: _actors.cached.query_infos.QueryInfos
        prod_idx: int
        cache_fp: Footprint
            The cache_fp that was just read by the reader
        array: ndarray
            The array onto which the reader fills rectangles one by one
        """
        msgs = []


        self._debug_push(
            qi, prod_idx, 'receive_sampled_a_cache_file_to_the_array', cache_fp,
        )

        try:
            pr = self._produce_per_query[qi][prod_idx]
        except:
            print()
            print('//////////////////////////////////////////////////////////////////////')
            print('error on', qi, prod_idx)
            print('len(self._produce_per_query):', len(self._produce_per_query))
            print('self._produce_per_query[qi]:', self._produce_per_query[qi])
            print('////////////////////')
            print(qi.prod)
            print('////////////////////')

            print('//////////////////////////////////////////////////////////////////////')
            for a, b, *c in self._debug_stack:
                if a is None:
                    print('===>', a, b, *c)
                elif a is qi:
                    if b is None:
                        print('===>', a, *c)
                    elif b == prod_idx:
                        print('===>', *c)
            print('//////////////////////////////////////////////////////////////////////')
            print()

            raise

        pi = qi.prod[prod_idx]

        # The constraints on `cache_fp` are now satisfied
        for resample_fp, cache_fps in pr.resample_needs.items():
            if cache_fp in cache_fps:
                cache_fps.remove(cache_fp)

        resample_ready = [
            resample_fp
            for resample_fp, cache_fps in pr.resample_needs.items()
            if len(cache_fps) == 0
        ]
        for resample_fp in resample_ready:
            del pr.resample_needs[resample_fp]
            subsample_fp = pi.resample_sample_dep_fp[resample_fp]
            assert subsample_fp is not None
            subsample_array = array[subsample_fp.slice_in(pi.sample_fp)]

            self._debug_push(
                qi, prod_idx, 'receive_sampled_a_cache_file_to_the_array', 'resample_ready', resample_fp,
            )

            assert subsample_array.shape[:2] == tuple(subsample_fp.shape), f"""

                                raster:
               self._raster.fp: {self._raster.fp}
               self._raster.fp: {repr(self._raster.fp)}
                    primitives: {list(self._raster.primitives_back)}
                           len: {len(self._raster)}
                         dtype: {self._raster.dtype}
                              : {self._raster.async_dict_path_of_cache_fp}

                                produce:
                         pi.fp: {pi.fp}
                         pi.fp: {repr(pi.fp)}
                  pi.sample_fp: {pi.sample_fp}
                  pi.sample_fp: {repr(pi.sample_fp)}
                   array.shape: {array.shape}

                                resample-step:
                   resample_fp: {resample_fp}
                   resample_fp: {repr(resample_fp)}
                  subsample_fp: {subsample_fp}
                  subsample_fp: {repr(subsample_fp)}
         subsample_array.shape: {subsample_array.shape}


                                pi.sample_fp.poly.contains(subsample_fp.poly)
                           res: {pi.sample_fp.poly.contains(subsample_fp.poly)}

                                subsample_fp.poly.within(pi.sample_fp.poly)
                           res: {subsample_fp.poly.within(pi.sample_fp.poly)}

                                subsample_fp.same_grid(pi.sample_fp)
                           res: {subsample_fp.same_grid(pi.sample_fp)}


                                self._raster.fp.poly.contains(subsample_fp.poly)
                           res: {self._raster.fp.poly.contains(subsample_fp.poly)}

                                subsample_fp.poly.within(self._raster.fp.poly)
                           res: {subsample_fp.poly.within(self._raster.fp.poly)}

                                subsample_fp.same_grid(self._raster.fp)
                           res: {subsample_fp.same_grid(self._raster.fp)}


                                self._raster.fp.poly.contains(pi.sample_fp.poly)
                           res: {self._raster.fp.poly.contains(pi.sample_fp.poly)}

                                pi.sample_fp.poly.within(self._raster.fp.poly)
                           res: {pi.sample_fp.poly.within(self._raster.fp.poly)}

                                pi.sample_fp.same_grid(self._raster.fp)
                           res: {pi.sample_fp.same_grid(self._raster.fp)}


        pi.sample_fp.poly.area: {pi.sample_fp.poly.area}
        subsample_fp.poly.area: {subsample_fp.poly.area}
                         inter: {(pi.sample_fp.poly & subsample_fp.poly).area}
                           a-b: {(pi.sample_fp.poly - subsample_fp.poly).area}
                           b-a: {(subsample_fp.poly - pi.sample_fp.poly).area}

                                subsample_fp.slice_in(pi.sample_fp)
                             A: {subsample_fp}
                             B: {pi.sample_fp}
                        slices: {subsample_fp.slice_in(pi.sample_fp)}

            """
            msgs += [Msg(
                'Resampler', 'resample_and_accumulate',
                qi, prod_idx, subsample_fp, resample_fp, subsample_array,
            )]

        return msgs

    def receive_made_this_array(self, qi, prod_idx, array):
        """Receive message: Done creating an output array"""
        self._debug_push(
            qi, prod_idx, 'receive_made_this_array', array.shape, array.dtype
        )
        del self._produce_per_query[qi][prod_idx]
        if len(self._produce_per_query[qi]) == 0:
            del self._produce_per_query[qi]
        return [Msg(
            'QueriesHandler', 'made_this_array', qi, prod_idx, array
        )]

    def receive_cancel_this_query(self, qi):
        """Receive message: One query was dropped

        Parameters
        ----------
        qi: _actors.cached.query_infos.QueryInfos
        """
        self._debug_push(
            qi, None, 'receive_cancel_this_query'
        )
        if qi in self._produce_per_query:
            del self._produce_per_query[qi]
        return []

    def receive_die(self):
        """Receive message: The raster was killed"""
        self._debug_push(
            None, None, 'receive_die',
        )
        assert self._alive
        self._alive = False

        self._produce_per_query.clear()
        self._raster = None
        return []

    # ******************************************************************************************* **

class _ProdArray(object):
    def __init__(self, pi):
        self.resample_needs = {
            resample_fp: set(cache_fps)
            for resample_fp, cache_fps in pi.resample_cache_deps_fps.items()
        }
