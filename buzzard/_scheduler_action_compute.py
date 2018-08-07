import functools

class ActionCompute():

    def __init__(self, truc, raster, query, compute_fp, primitive_arrays_getter):
        self._truc, self._raster, self._query = truc, raster, query
        self._primitive_arrays_getter = primitive_arrays_getter
        self._compute_fp = compute_fp
        self._first = True

    @property
    def is_urgent(self):
        return True or False

    def get_worker_fn(self):
        assert self._first
        self._first = False

        return functools.partial(
            self._raster.compute_array,
            self._primitive_arrays_getter(),
            ...
        )

    def done(self, result):
        """Find or create the ActionMerge objects related to this computation.
        If an ActionMerge object is ready, put in in the merge_pool_manager.
        """
        for cache_fp in self._raster._machin & self._compute_fp:
            if self._cache_fp in self._truc.merge_actions:
                action = self._truc.merge_actions(self._cache_fp)
            else:
                action = ActionMerge(
                    self._truc, self._raster, self._query,
                    self._cache_fp,
                )
                self._truc.merge_actions.append(action)
            action.save_result(self._compute_fp, result)
            if action.is_ready:
                self._truc.merge_pool_manager.put_in_waiting_room(action)
