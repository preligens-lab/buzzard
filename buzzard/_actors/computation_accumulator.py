from buzzard._actors.message import Msg

class ActorComputationAccumulator(object):
    """Actor that takes care of accumulating computed slices needed
    to write 1 cache tile

    TODO Idea:
    If a computation tile A overlap with several cache tiles (a, b, c, d),
    when slicing the numpy array of A into chunks (a, b, c, d), no copy is performed,
    the 4 arrays internally point to A.
    The pointer to A is only released when all 4 cache footprints have been merged.
    In some cases, it might be a good choice to duplicate the slices of A to release
    memory.

    """

    def __init__(self, raster):
        self._raster = raster
        self._alive = True
        self._cache_tiles_accumulations = {}
        self.address = '/Raster{}/ComputationAccumulator'.format(self._raster.uid)

    @property
    def alive(self):
        return self._alive

    # ******************************************************************************************* **
    def receive_combine_this_array(self, compute_fp, array):
        msgs = []

        for cache_fp in self._raster.cache_fps_of_compute_fp[compute_fp]:

            # Fetch and update storage for that cache_fp
            if cache_fp in self._cache_tiles_accumulations:
                store = self._cache_tiles_accumulations[cache_fp]
            else:
                store = {
                    'missing': set(self._raster.compute_fps_of_cache_fp[cache_fp]),
                    'ready': {},
                }
                self._cache_tiles_accumulations[cache_fp] = store
            assert compute_fp in store['missing']
            store['missing'].remove(compute_fp)

            compute_fp_part = compute_fp & cache_fp
            # TODO Idea: Should cache_fp be dilated before the above intersection? This could be a
            #   parameters in facade constructor.
            #   This means also depending on computation_fp that only touch in the border.
            #   On the other hand overlap is a `computation concern`, overlap may not be a `merge concern`
            slices = compute_fp_part.slice_in(compute_fp)
            assert compute_fp_part not in store['ready']
            store['ready'][compute_fp_part] = array[slices]

            # Send news to merger
            if len(store['missing']) == 0:
                msgs += [
                    Msg('Merger', 'merge_those_arrays', cache_fp, store['ready'])
                ]
                del self._cache_tiles_accumulations[cache_fp]
        return msgs

    def receive_die(self):
        """Receive message: The raster was killed"""
        assert self._alive
        self._alive = False
        self._cache_tiles_accumulations.clear()
        self._raster = None
        return []

    # ******************************************************************************************* **
