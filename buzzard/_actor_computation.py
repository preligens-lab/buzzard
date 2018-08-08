class ActorMixinComputation(object):
    def receive_schedule_one_compute(self, raster, fp, primitive_fps, primitive_arrays_getter):
        def _start():
            primitive_arrays = primitive_arrays_getter()
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
