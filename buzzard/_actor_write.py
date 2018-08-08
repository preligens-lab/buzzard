class ActorMixinWrite(object):
    def receive_schedule_one_write(self, raster, fp, array):
        def _start():
            future = self._pool.apply_async(
                _write_cache_tile,
                (fp, array, ...),
            )
            self._working += [
                (future, _stop),
            ]

        def _stop(_):
            return [
                Msg('Cache', 'done_one_write', raster, fp),
            ]

        self._waiting += [
            (de_quoi_id_la_prio, _start),
        ]
        return []
