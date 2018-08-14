class ActorCacheFileChecker(object):
    """Actor that takes care of performing md5 hash to existing cache files

    Messages
    --------
    - Sends -one_cache_file_check- @ Caching
      - is answer from -schedule_one_cache_file_check-

    """

    def __init__(self, raster, pool_actor):
        self._raster = raster
        self._pool_actor = pool_actor

    # ******************************************************************************************* **
    def receive_schedule_one_cache_file_check(self, cache_fp, path):
        return _perform_one_check(cache_fp, path)

    # ******************************************************************************************* **
    def _perform_one_check(self, cache_fp, path):
        """This closure takes care of the lifetime of a cache tile checking"""
        def _join_waiting_room():
            self._pool_actor._waiting += [
                (de_quoi_id_la_prio, _leave_waiting_room),
            ]
            return []

        def _leave_waiting_room():
            future = self._pool_actor.apply_async(
                _cache_file_check,
                cache_fp, path,
            )
            self._pool_actor._working += [
                (future, _work_done),
            ]
            return []

        def _work_done(result):
            return [
                Msg('Raster::Caching', 'done_one_cache_file_check', cache_fp, path, result),
            ]

        return _join_waiting_room()

    # ******************************************************************************************* **

def _cache_file_check(cache_fp, path):
    return (True or False) == 'That is the TODO question'
