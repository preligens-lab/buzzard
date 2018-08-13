class ActorMerge(object):
    """Actor that merges the computation tiles to cache tiles

    Messages
    --------
    - Sends -schedule_one_write- @ Write
    - Receives -one_merge- from ComputeAccumulator

    """
    def __init__(self, raster, pool_actor):
        self._raster = raster
        self._pool_actor = pool_actor

    def _schedule_one_merge(self, cache_fp, array_of_compute_fp):
        """This closure takes care of the lifetime of a computation tiles merging"""
        def _join_waiting_room():
            self._pool_actor._waiting += [
                (de_quoi_id_la_prio, _leave_waiting_room),
            ]
            return []

        def _leave_waiting_room():
            future = self._pool_actor.apply_async(
                self._raster.merge_arrays,
                cache_fp, array_of_compute_fp,
            )
            self._pool_actor._working += [
                (future, _work_done),
            ]
            return []

        def _work_done(array):
            return [
                Msg('Raster::Write', 'schedule_one_write', cache_fp, array),
            ]

        return _join_waiting_room()

    def receive_schedule_one_merge(self, cache_fp, array_of_compute_fp):
        return self._schedule_one_merge(cache_fp, array_of_compute_fp)
