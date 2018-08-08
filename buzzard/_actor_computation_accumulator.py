class ActorComputeAccumulator(object):
    def __init__(self):
        self._cache_tiles_accumulations = {}

    def receive_done_one_compute(self, raster, compute_fp, array):
        msgs = []
        for cache_fp in raster.cache_fp_of_compute_fp(comput_fp):
            key = (raster, cache_fp)
            if key in self._cache_tiles_accumulations:
                store = self._cache_tiles_accumulations[key]
            else:
                store = {'missing': raster.compute_fps_of_cache_fp(cache_fp), 'ready': {}}
                self._cache_tiles_accumulations[key] = store
            assert compute_fp in store['missing']
            del store['missing'][compute_fp]
            store['ready'][compute_fp] = array
            if len(store['missing']) == 0:
                msgs += [
                    Msg('Merge', 'schedule_one_merge', raster, cache_fp, store['ready'])
                ]
                del self._cache_tiles_accumulations[key]
        return msgs
