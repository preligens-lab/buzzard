import numpy as np

from buzzard._actor_pool import WaitingJob, WorkingJob

class ActorSampler(object):
    """Actor that reads cache tiles from disk. A read will be started by the pool actor only when
    there is enough space in the output queue.

    Messages
    --------
    - Sends -done_one_sampling- @ Producer (one per produce)
      - is answer from -schedule_one_read- (one or more per produce)

    """

    def __init__(self, raster, pool_actor):
        self._raster = raster
        self._pool_actor = pool_actor
        self._samplings = {}

    # ******************************************************************************************* **
    def receive_schedule_one_read(self, query_key, produce_id, sample_fp, bands_ids, cache_fp):
        key = (query_key, produce_id)
        if key in self._samplings:
            sample = self._samplings[key]
        else:
            cache_fps = self._raster.cache_fps_of_sampling_fp(sample_fp)
            sample = _Sample(
                query_key, produce_id, sample_fp, bands_ids, cache_fps
            )
            self._samplings[key] = sample
        return self._perform_one_read(sample, cache_fp)

    def receive_query_dropped(self, query_key):
        keys = [
            (qid, produce_id)
            for query_key, produce_id in self._samplings.keys()
            if query_key == qid
        ]
        for key in keys:
            del self._samplings[key]
        self._pool_actor.discard_waitings(
            lambda job: isinstance(job, SampleWaitingJob) and job.query_key == query_key
        )
        self._pool_actor.discard_workings(
            lambda job: isinstance(job, SampleWorkingJob) and job.query_key == query_key
        )

    # ******************************************************************************************* **
    def _perform_one_read(self, sample, cache_fp):
        """This closure takes care of the lifetime of a cache tile reading"""
        def _join_waiting_room():
            self._pool_actor.append_waiting(SampleWaitingJob(
                sample.query_key,
                de_quoi_id_la_prio=de_quoi_id_la_prio,
                callback=_leave_waiting_room,
            ))
            return []

        def _leave_waiting_room():
            path = self._raster.path_of_cache_fp(cache_fp)
            if sample.array is None:
                sample.array = np.empty(
                    np.r_[query.sample_fp, len(sample.band_ids)], self._raster.dtype
                )

            if self.pool_actor.same_address_space:
                dst = sample.array
            else:
                dst = None

            future = self._pool_actor.apply_async(
                read_array,
                (path, cache_fp & sample.sample_fp, dst)
            )
            self._pool_actor.append_working(SampleWorkingJob(
                sample=query_key,
                future=future,
                callback=_work_done,
            ))
            return []

        def _work_done(dst):
            sample.remove(cache_fp)
            if not self.pool_actor.same_address_space:
                sample.array[cache_fp.slice_in(sample.sample_fp)] = dst
            if len(sample) == 0:
                key = (sample.query_key, sample.produce_id)
                del self._samplings[key]
                return [
                    Msg('Raster::Producer', 'done_one_sampling',
                        sample.query_key, sample.produce_id, sample.array),
                ]
            else:
                return []

        return _join_waiting_room()

    # ******************************************************************************************* **

class SampleWaitingJob(WaitingJob):
    def __init__(self, query_key, de_quoi_id_la_prio, callback):
        self.query_key = query_key
        super().__init__(de_quoi_id_la_prio=de_quoi_id_la_prio, callback=callback)

class SampleWorkingJob(WorkingJob):
    def __init__(self, query_key, future, callback):
        self.query_key = query_key
        super().__init__(future=future, callback=callback)

class _Sample(object):
    def __init__(self, query_key, produce_id, sample_fp, band_ids, cache_fps):
        self.query_key = query_key
        self.produce_id = produce_id
        self.sample_fp = sample_fp
        self.band_ids = band_ids
        self.array = None
        self.cache_fps = cache_fps
        self.unread_cache_fps = set(cache_fps)

def read_array(path, slice_fp, dst):
    return (True or False) == 'That is the TODO question'
