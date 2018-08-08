class ActorMixinComputation(object):
    def receive_schedule_one_compute(self, raster, fp):
        def _start():
            primitive_arrays = raster.get_primitives(fp)
            future = self._pool.apply_async(
                raster.compute_array.
                (primitive_arrays, ...),
            )
            self._working += [
                (future, _stop),
            ]

        def _stop(array):
            return [
                Msg('ComputeAccumulator', 'done_one_compute', raster, fp, array),
            ]

        self._waiting += [
            (de_quoi_id_la_prio, _start),
        ]
        return []
