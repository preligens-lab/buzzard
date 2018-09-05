import functools
import collections

import multiprocessing as mp
import multiprocessing.pool
import numpy as np

from buzzard._actors.message import Msg
from buzzard._actors.pool_job import CacheJobWaiting, PoolJobWorking

class ActorMerger(object):
    """Actor that takes care of merging several array into one fp
    TODO: in this state it is used only for cached
          aren't they merge operations even in not cached rasters?
    """

    def __init__(self, raster):
        self._raster = raster
        self._alive = True
        merge_pool = raster.merge_pool
        self._waiting_room_address = '/Pool{}/WaitingRoom'.format(id(merge_pool))
        self._working_room_address = '/Pool{}/WorkingRoom'.format(id(merge_pool))
        self._waiting_jobs = set()
        self._working_jobs = set()
        if isinstance(merge_pool, mp.ThreadPool):
            self._same_address_space = True
        elif isinstance(merge_pool, mp.Pool):
            self._same_address_space = False
        else:
            assert False, 'Type should be checked in facade'

        self.dst_array = None

    @property
    def address(self):
        return '/Raster{}/Merger'.format(self._raster.uid)

    @property
    def alive(self):
        return self._alive

    # ******************************************************************************************* **
    def receive_merge_those_arrays(self, qi, cache_fp, array_of_compute_fp):
        wait = Wait(self, qi, cache_fp, array_of_compute_fp)
        self._waiting_jobs.add(wait)
        return [
            Msg(self._waiting_room_address, 'schedule_job', wait)
        ]

    def receive_token_to_working_room(self, job, token):
        self._waiting_jobs.remove(job)

        # TODO: change that
        self.dst_array = np.empty(job.cache_fp.shape)

        work = Work(self, job.qi, job.cache_fp, job.array_of_compute_fp, self.dst_array)
        self._working_jobs.add(work)
        return [
            Msg(self._working_room_address, 'launch_job_with_token', work, token)
        ]

    def receive_job_done(self, job, result):
        if self._same_address_space:
            assert result is None
            array = self.dst_array
        else:
            array = result
        self._working_jobs.remove(job)

        # TODO: where to define path????
        return [
            Msg('Writer', 'write_this_array',
                job.cache_fp, array, job.path,
            )
        ]

    def receive_cancel_this_query(self, qi):
        msgs = []
        # TODO: find a way to link waiting merges to a set of qi's
        #       if there is no qi left in the set, set priority to np.inf
        #       else, set priority according to qi's left in the set after 
        #       the removal of `qi` (the parameter)
        return []


    def receive_die(self):
        """Receive message: The raster was killed"""
        assert self._alive
        self._alive = False
        # TODO: set the priority of waiting jobs to np.inf?
        return []

    # ******************************************************************************************* **

class Wait(CacheJobWaiting):
    # TODO: inherit not from CacheJobWaiting but from another thing?
    def __init__(self, actor, qi, cache_fp, array_of_compute_fp):
        self.qi = qi
        # TODO: rename in dst_fp? (for not cached merges)
        self.cache_fp = cache_fp
        self.array_of_compute_fp = array_of_compute_fp
        # TODO: set action priority other than 1
        # TODO: raster uid
        # TODO: fp = cache fp? (last parameter)
        super().__init__(actor.address, actor._raster.uid, self.cache_fp, 1, self.cache_fp)

class Work(PoolJobWorking):
    def __init__(self, actor, qi, cache_fp, array_of_compute_fp, dst_array):
        self.qi = qi
        self.cache_fp = cache_fp
        raster = actor._raster

        if actor._same_address_space:
            func = functools.partial(
                self._raster.merge_arrays,
                cache_fp, array_of_compute_fp, dst_array,
            )
        else:
            self._dst_array = dst_array
            func = functools.partial(
                self._raster.merge_arrays,
                cache_fp, array_of_compute_fp, None,
            )

        super().__init__(actor.address, func)
