import collections
import multiprocessing as mp
import multiprocessing.pool
import functools

import numpy as np

from buzzard._actors.message import Msg
from buzzard._actors.pool_job import ProductionJobWaiting, PoolJobWorking

class ActorComputer(object):
    """Actor that takes care of sheduling computations by using user's `compute_array` function"""

    def __init__(self, raster):
        self._raster = raster
        self._alive = True
        computation_pool = raster.computation_pool
        if computation_pool is not None:
            self._waiting_room_address = '/Pool{}/WaitingRoom'.format(id(computation_pool))
            self._working_room_address = '/Pool{}/WorkingRoom'.format(id(computation_pool))
            if isinstance(computation_pool, mp.pool.ThreadPool):
                self._same_address_space = True
            elif isinstance(computation_pool, mp.pool.Pool):
                self._same_address_space = False
            else: # pragma: no cover
                assert False, 'Type should be checked in facade'
        self._waiting_jobs_per_query = collections.defaultdict(set)
        self._working_jobs = set()

        self._performed_computations = set() # type: Set[Footprint]
        self.address = '/Raster{}/Computer'.format(self._raster.uid)

    @property
    def alive(self):
        return self._alive

    # ******************************************************************************************* **
    def receive_compute_this_array(self, qi, compute_idx):
        """Receive message: Start making this array"""
        msgs = []

        if self._raster.computation_pool is None:
            work = self._create_work_job(qi, compute_idx)
            compute_fp = qi.cache_computation.list_of_compute_fp[compute_idx]
            if compute_fp not in self._performed_computations:
                res = work.func()
                res = self._normalize_user_result(compute_fp, res)
                self._raster.debug_mngr.event('object_allocated', res)
                self._performed_computations.add(compute_fp)
                msgs += self._commit_work_result(work, res)

        else:
            wait = Wait(self, qi, compute_idx)
            self._waiting_jobs_per_query[qi].add(wait)
            msgs += [Msg(self._waiting_room_address, 'schedule_job', wait)]

        return msgs

    def receive_token_to_working_room(self, job, token):
        msgs = []

        self._waiting_jobs_per_query[job.qi].remove(job)
        if len(self._waiting_jobs_per_query[job.qi]) == 0:
            del self._waiting_jobs_per_query[job.qi]

        work = self._create_work_job(job.qi, job.compute_idx)

        compute_fp = job.qi.cache_computation.list_of_compute_fp[job.compute_idx]
        if compute_fp not in self._performed_computations:
            msgs += [Msg(self._working_room_address, 'launch_job_with_token', work, token)]
            self._performed_computations.add(compute_fp)
            self._working_jobs.add(work)
        else:
            msgs += [Msg(self._working_room_address, 'salvage_token', token)]

        return msgs

    def receive_job_done(self, job, result):
        result = self._normalize_user_result(job.compute_fp, result)
        self._raster.debug_mngr.event('object_allocated', result)
        self._working_jobs.remove(job)
        return self._commit_work_result(job, result)

    def receive_cancel_this_query(self, qi):
        """Receive message: One query was dropped

        Parameters
        ----------
        qi: _actors.cached.query_infos.QueryInfos
        """
        msgs = []
        for job in self._waiting_jobs_per_query[qi]:
            msgs += [Msg(self._waiting_room_address, 'unschedule_job', job)]
        del self._waiting_jobs_per_query[qi]
        return msgs

    def receive_die(self):
        """Receive message: The raster was killed"""
        assert self._alive
        self._alive = False

        msgs = []
        msgs += [
            Msg(self._waiting_room_address, 'unschedule_job', job)
            for jobs in self._waiting_jobs_per_query.values()
            for job in jobs
        ]
        self._waiting_jobs_per_query.clear()

        msgs += [
            Msg(self._working_room_address, 'cancel_job', job)
            for job in self._working_jobs
        ]
        self._working_jobs.clear()

        self._raster = None
        return msgs

    # ******************************************************************************************* **
    def _create_work_job(self, qi, compute_idx):
        return Work(
            self, qi, compute_idx,
        )

    def _commit_work_result(self, work_job, res):
        return [Msg('ComputationAccumulator', 'combine_this_array', work_job.compute_fp, res)]

    def _normalize_user_result(self, compute_fp, res):
        if not isinstance(res, np.ndarray): # pragma: no cover
            raise ValueError("Result of recipe's `compute_array` have type {}, it should be ndarray".format(
                type(res)
            ))
        res = np.atleast_3d(res)
        y, x, c = res.shape
        if (y, x) != tuple(compute_fp.shape): # pragma: no cover
            raise ValueError("Result of recipe's `compute_array` have shape `{}`, should start with {}".format(
                res.shape,
                compute_fp.shape,
            ))
        if c != len(self._raster): # pragma: no cover
            raise ValueError("Result of recipe's `compute_array` have shape `{}`, should have {} bands".format(
                res.shape,
                len(self._raster),
            ))
        res = res.astype(self._raster.dtype, copy=False)
        return res

    # ******************************************************************************************* **

class Wait(ProductionJobWaiting):

    def __init__(self, actor, qi, compute_idx):
        self.qi = qi
        self.compute_idx = compute_idx
        qicc = qi.cache_computation

        compute_fp = qicc.list_of_compute_fp[compute_idx]
        prod_idx = qicc.dict_of_min_prod_idx_per_compute_fp[compute_fp]
        super().__init__(actor.address, qi, prod_idx, 4, compute_fp)

class Work(PoolJobWorking):
    def __init__(self, actor, qi, compute_idx):
        qicc = qi.cache_computation
        assert qicc.collected_count == compute_idx, (qicc.collected_count, compute_idx)

        compute_fp = qicc.list_of_compute_fp[compute_idx]

        self.compute_fp = compute_fp

        primitive_arrays = {}
        primitive_footprints = {}
        for prim_name, queue in qicc.primitive_queue_per_primitive.items():
            primitive_arrays[prim_name] = queue.get_nowait()
            primitive_footprints[prim_name] = qicc.primitive_fps_per_primitive[prim_name][compute_idx]

        qicc.collected_count += 1

        if actor._raster.computation_pool is None or actor._same_address_space:
            func = functools.partial(
                actor._raster.compute_array,
                compute_fp,
                primitive_footprints,
                primitive_arrays,
                actor._raster.facade_proxy
            )
        else:
            func = functools.partial(
                actor._raster.compute_array,
                compute_fp,
                primitive_footprints,
                primitive_arrays,
                None,
            )
        actor._raster.debug_mngr.event('object_allocated', func)

        super().__init__(actor.address, func)
