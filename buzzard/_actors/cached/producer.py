from buzzard._actors.message import Msg

import collections

class ActorProducer(object):
    """Actor that takes care of waiting for cache tiles reads and launching resamplings"""

    def __init__(self, raster):
        self._raster = raster
        self._alive = True

        self._produce_per_query = collections.defaultdict(dict) # type: Mapping[CachedQueryInfos, Mapping[int, _ProdArray]]
        self.address = '/Raster{}/Producer'.format(self._raster.uid)

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

        pi = qi.prod[prod_idx] # type: CacheProduceInfos
        assert pi.share_area is (len(pi.cache_fps) != 0)

        if pi.share_area:
            # If this prod_idx requires some cache file reads (this is the case most of the time)
            msgs += [Msg(
                'CacheExtractor', 'sample_those_cache_files_to_an_array', qi, prod_idx,
            )]

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

        pr = self._produce_per_query[qi][prod_idx]
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

            assert subsample_array.shape[:2] == tuple(subsample_fp.shape)
            msgs += [Msg(
                'Resampler', 'resample_and_accumulate',
                qi, prod_idx, subsample_fp, resample_fp, subsample_array,
            )]

        return msgs

    def receive_made_this_array(self, qi, prod_idx, array):
        """Receive message: Done creating an output array"""
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
        if qi in self._produce_per_query:
            del self._produce_per_query[qi]
        return []

    def receive_die(self):
        """Receive message: The raster was killed"""
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
