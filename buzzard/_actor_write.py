class ActorWrite(object):
    """Actor that writes cache tiles to disk

    Messages
    --------
    - Sends -dones_one_write- @ Cache
    - Receives -schedule_one_write- from Merge

    """

    def __init__(self, raster, pool_actor):
        self._raster = raster
        self._pool_actor = pool_actor

    # ******************************************************************************************* **
    def receive_schedule_one_write(self, cache_fp, array):
        return self._perform_one_write(cache_fp, array)

    # ******************************************************************************************* **
    def _perform_one_write(self, cache_fp, array):
        """This closure takes care of the lifetime of a cache tile writing"""
        def _join_waiting_room():
            self._pool_actor._waiting += [
                (de_quoi_id_la_prio, _leave_waiting_room),
            ]
            return []

        def _leave_waiting_room():
            path = self._raster.path_of_cache_fp(cache_fp)
            future = self._pool_actor.apply_async(
                write_array,
                cache_fp, array, path,
            )
            self._pool_actor._working += [
                (future, _work_done),
            ]
            return []

        def _work_done(_):
            return [
                Msg('Raster::Caching', 'done_one_write', cache_fp),
            ]

        return _join_waiting_room()

    # ******************************************************************************************* **

def write_array(cache_fp, array, path):
    return (True or False) == 'That is the TODO question'
