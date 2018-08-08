class ActorMixinMerge(object):
    def receive_schedule_one_merge(self, raster, fp, computations):
        def _start():
            future = self._pool.apply_async(
                raster.merge_array,
                (fp, computations),
            )
            self._working += [
                (future, _stop),
            ]

        def _stop(array):
            return [
                Msg('Write', 'schedule_one_write', raster, fp, array),
            ]

        self._waiting += [
            (de_quoi_id_la_prio, _start),
        ]
        return []
