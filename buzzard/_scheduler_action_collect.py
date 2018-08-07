
class ActionCollect():

    is_urgent = True

    def __init__(self, truc, raster, query, compute_fp):
        self._truc, self._raster, self._query = truc, raster, query
        self._compute_fp = compute_fp

    def schedule_computation(self, primitive_arrays_getter):
        action = ActionCompute(
            self._truc, self._raster, self._query,
            self._compute_fp, primitive_arrays_getter,
        )
        self._raster.computation_pool_manager.put_in_waiting_room(action)
