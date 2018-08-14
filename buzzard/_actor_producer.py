import collections

class ActorProducer(object):
    """Actor that takes care of producing queried data.

    Messages
    --------
    - Sends -ensure_cache_tiles_can_be_read- @ Caching (one per query)
      - will answer at -cache_tile_subset_can_be_read- (one or more per query)
    - Sends -schedule_one_read- @ Sampler (one per produce per cache tile)
      - will answer ar -done-one-sampling- (one per produce)
    - Sends -schedule_one_resampling- @ Resampler (one per produce)
    """

    def __init__(self, raster):
        self._raster = raster
        self._queries = {}

    # ******************************************************************************************* **
    def receive_produce_query(self, query_key, produce_fps, band_ids, dst_nodata, interpolation):
        msgs = []

        produce_fps_same_grid = [
            fp.same_grid(self._raster.fp)
            for fp in produce_fps
        ]
        sample_fps = []
        same_grid = []
        cache_fps_of_sample_fp = []
        for produce_fp in produce_fps:
            if not produce_fp.share_area(self._raster.fp):
                sample_fps.append(None)
                same_grid.append(True)
                cache_fps_of_sample_fp.append([])
                continue

            if produce_fp.same_grid(self._raster.fp):
                sample_fp = self._raster.fp & produce_fp
                same_grid.append(True)
            else:
                sample_fp =  self._raster.build_sampling_footprint_to_remap(produce_fp, interpolation)
                same_grid.append(False)
            sample_fps.append(sample_fp)
            cache_fps_of_sample_fp.append(list(
                self._raster.cache_fps_of_fp(sample_fp)
            ))

        cache_fps = collections.OrderedDict()
        for fps in cache_fps_of_sample_fp:
            for fp in fps:
                if fp not in cache_fps:
                    cache_fps[fp] = 42
        cache_fps = list(cache_fps.keys())

        q = _Query(
            query_key,
            produce_fps, band, dst_nodata, interpolation,
            sample_fps, same_grid, cache_fps_of_sample_fp, cache_fps
        )
        self._queries[query_key] = q

        if len(cache_fps) == 0:
            # If no cache tiles are required (e.g. a query outside of raster)
            pass
        else:
            msgs += [
                Msg('Raster::Caching', 'ensure_cache_tiles_can_be_read', query_key, cache_fps)
            ]
        return msgs

    def receive_cache_tile_subset_can_be_read(self, query_key, cache_fps):
        msgs = []
        q = self._queries[query_key]
        for cache_fp in cache_fps:
            produce_indices = q.produce_indices_of_cache_fp[cache_fp]
            for i in produce_indices:
                msgs += [
                    Msg('Raster::Sampler', 'schedule_one_read',
                        query_key, i, q.sample_fps[i], q.band_ids, cache_fp)
                ]

        return msgs

    def receive_done_one_sampling(self, query_key, produce_id, array):
        msgs = []

        q = self._queries[query_key]
        same_grid = q.same_grid[produce_id]
        sample_fp = q.sample_fps[produce_id]
        produce_fp = q.produce_fps[produce_id]

        if same_grid:
            array = self._raster.remap(
                sample_fp, produce_fp, array, None,
                self._raster.nodata, q.dst_nodata, 'erode', q.interpolation,
            )
            msgs += [
                Msg('Communicator::produce_array', query_key, produce_id, array)
            ]
        else:
            msgs += [
                Msg('Raster::Resampler', 'schedule_one_resample',
                )
            ]

        return msgs

    def receive_done_one_resampling(self, query_key, produce_id, array):
        return [Msg('Communicator::produce_array', query_key, produce_id, array)]

    def receive_query_dropped(self, query_key):
        del self._queries[query_key]

    # ******************************************************************************************* **

class _Query(object):
    def __init__(self, query_key,
                 produce_fps, band_ids, dst_nodata, interpolation,
                 sample_fps, same_grid, cache_fps_of_sample_fp, cache_fps):
        self.query_key = query_key
        self.produce_fps = produce_fps
        self.band_ids = band_ids
        self.dst_nodata = dst_nodata
        self.band_ids = band_ids
        self.dst_nodata = dst_nodata
        self.interpolation = interpolation
        self.sample_fps = sample_fps
        self.same_grid = same_grid
        self.cache_fps_of_sample_fp = cache_fps_of_sample_fp
        self.cache_fps = cache_fps

        self.unsure_cache_fps_of_sample_fp = [
            set(fps)
            for fpr in cache_fps_of_sample_fp
        ]

        self.produce_indices_of_cache_fp = collections.default_dict(set)
        for i, unsure_cache_fps in enumerate(self.unsure_cache_fps_of_sample_fp):
            self.produce_indices_of_cache_fp[unsure_cache_fps].add(i)
