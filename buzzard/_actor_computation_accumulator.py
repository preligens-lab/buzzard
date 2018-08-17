
class ActorComputeAccumulator(object):
    """Actor that accumulates the results of the computation phase before merge phase

    TODO: (
       Avoid keeping a full computation tile in memory if not necessary.
    When a cache tile is completed, check that each computation tiles is not kept in memory if
    only a small fraction of it is required. If only a small fraction of it is required, copy
    the slices to collect the rest of it.
    )

    Messages
    --------
    - Sends -one_merge- @ Merge
    - Receives -done_one_compute- from Computer

    """
    def __init__(self, raster):
        self._raster = raster
        self._cache_tiles_accumulations = {}

    # ******************************************************************************************* **
    def receive_done_one_compute(self, compute_fp, array):
        msgs = []
        for cache_fp in self._raster.cache_fps_of_compute_fp(compute_fp):
            if cache_fp in self._cache_tiles_accumulations:
                store = self._cache_tiles_accumulations[cache_fp]
            else:
                store = {'missing': raster.compute_fps_of_cache_fp(cache_fp), 'ready': {}}
                self._cache_tiles_accumulations[cache_fp] = store
            assert compute_fp in store['missing']
            del store['missing'][compute_fp]
            slices = compute_fp.slice_in(cache_fp)
            store['ready'][compute_fp] = array[slices]
            if len(store['missing']) == 0:
                msgs += [
                    Msg('Merge', 'schedule_one_merge', cache_fp, store['ready'])
                ]
                del self._cache_tiles_accumulations[cache_fp]
        return msgs

    # ******************************************************************************************* **
