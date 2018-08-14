from buzzard._actor_pool import WaitingJob, WorkingJob

class ActorResampler(object):
    """Actor that resamples a sample tile to a produce tile

    Messages
    --------
    - Sends -done_one_resampling- @ Producer (one per produce on different grid)
      - is answer from `schedule_one_resample`
    """

    def __init__(self, raster, pool_actor):
        self._raster = raster
        self._pool_actor = pool_actor

    # ******************************************************************************************* **
    def receive_schedule_one_resample(self, query_key, produce_id, sample_fp, produce_fp, array,
                                     dst_nodata, interpolation):
        if self._raster.max_resampling_size is None:
            tiles = [produce_fp]
        else:
            rsize = np.maximum(produce_fp.rsize, sample_fp.rsize)
            countx, county = np.ceil(rsize / self._raster.max_resampling_size).astype(int)
            tiles = sample_fp.tile_count((countx, county), boundary_effect='shrink').flatten()

        r = _Resample(
            query_key, produce_id, sample_fp, produce_fp, dst_nodata, interpolation,
            len(tiles),
        )

        msgs = []
        for sub_produce_fp in tiles:
            sub_sample_fp =  self._raster.build_sampling_footprint_to_remap(
                sub_produce_fp, interpolation
            )
            assert sub_sample_fp.poly.within(sample_fp.poly)
            a = array[sub_sample_fp.slice_in(sample_fp)]
            msgs += self._perform_one_resample(r, sub_produce_fp, sub_sample_fp, a)
        return msgs

    def receive_query_dropped(self, query_key):
        self._pool_actor.discard_waitings(
            lambda job: isinstance(job, ResampleWaitingJob) and job.query_key == query_key
        )
        self._pool_actor.discard_workings(
            lambda job: isinstance(job, ResampleWorkingJob) and job.query_key == query_key
        )

    # ******************************************************************************************* **
    def _perform_one_resample(self, r, sub_sample_fp, sub_produce_fp, sub_sample_array):
        """This closure takes care of the lifetime of a resampling operation"""

        def _join_waiting_room():
            self._pool_actor.append_waiting(ResampleWaitingJob(
                r.query_key,
                de_quoi_id_la_prio=de_quoi_id_la_prio,
                callback=_leave_waiting_room,
            ))
            return []

        def _leave_waiting_room():
            future = self._pool_actor.pool.apply_async(
                self._raster.remap,
                (sub_sample_fp, sub_produce_fp, sub_sample_array, None,
                 self._raster.nodata, r.dst_nodata, 'erode', r.interpolation)
            )
            self._pool_actor.append_working(ResampleWorkingJob(
                r.query_key,
                future=future,
                callback=_work_done,
            ))
            return []

        def _work_done(sub_produce_array):
            msgs = []

            if r.to_burn_count == 1:
                assert r.produce_fp == sub_produce_fp
                r.produce_array = sub_produce_array
            else:
                if r.produce_array is None:
                    r.produce_array = np.empty(
                        np.r_[r.produce_fp.shape, sub_produce_array.shape[-1]],
                        self._raster.dtype,
                    )
                r.produce_array[sub_produce_fp.slice_in(r.produce_fp)] = sub_produce_array
            r.burned_count += 1
            if r.burned_count == r.to_burn_count:
                msgs += [
                    Msg('Raster::Producer', 'done_one_resampling',
                        r.query_key, r.produce_id, r.produce_array),
                ]

            return msgs

        return _join_waiting_room()

    # ******************************************************************************************* **

class ResampleWaitingJob(WaitingJob):
    def __init__(self, query_key, de_quoi_id_la_prio, callback):
        self.query_key = query_key
        super().__init__(de_quoi_id_la_prio=de_quoi_id_la_prio, callback=callback)

class ResampleWorkingJob(WorkingJob):
    def __init__(self, query_key, future, callback):
        self.query_key = query_key
        super().__init__(future=future, callback=callback)

class _Resample(object):
    def __init__(self, query_key, produce_id, sample_fp, produce_fp, dst_nodata, interpolation, to_burn_count):
        self.query_key = query_key
        self.produce_id = produce_id
        self.sample_fp = sample_fp
        self.produce_fp = produce_fp
        self.dst_nodata = dst_nodata
        self.interpolation = interpolation
        self.produce_array = None
        self.to_burn_count = to_burn_count

        self.burned_count = 0
