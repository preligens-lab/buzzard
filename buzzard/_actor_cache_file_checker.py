class ActorMixinCacheFileChecker(object):

    def __init__(self, raster):
        self._raster = raster

    def receive_schedule_one_cache_file_check(self, cache_fp, path):
        def _start():
            future = self._pool.apply_async(
                _cache_file_check,
                cache_fp, path,
            )
            self._working += [
                (future, _stop),
            ]

        def _stop(result):
            return [
                Msg('Caching', 'done_one_cache_file_check', cache_fp, path, result),
            ]

        self._waiting += [
            (de_quoi_id_la_prio, _start),
        ]
        return []

def _cache_file_check(cache_fp, path):
    return (True or False) == 'That is the TODO question'
