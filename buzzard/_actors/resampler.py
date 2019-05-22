import functools
import collections
import multiprocessing as mp
import multiprocessing.pool

import numpy as np

from buzzard._actors.message import Msg
from buzzard._actors.pool_job import ProductionJobWaiting, PoolJobWorking
from buzzard._a_source_raster_remap import ABackSourceRasterRemapMixin

class ActorResampler(object):
    """Actor that takes care of resampling sample tiles, and wait for all
    resamplings to be performed for a production array.
    """

    def __init__(self, raster):
        self._raster = raster
        self._alive = True
        resample_pool = raster.resample_pool
        if resample_pool is not None:
            self._waiting_room_address = '/Pool{}/WaitingRoom'.format(id(resample_pool))
            self._working_room_address = '/Pool{}/WorkingRoom'.format(id(resample_pool))
            if isinstance(resample_pool, mp.pool.ThreadPool):
                self._same_address_space = True
            elif isinstance(resample_pool, mp.pool.Pool):
                self._same_address_space = False
            else: # pragma: no cover
                assert False, 'Type should be checked in facade'
        self._waiting_jobs = set()
        self._working_jobs = set()

        self._prod_infos = (
            collections.defaultdict(dict)
        ) # type: Mapping[CachedQueryInfos, Mapping[int, _ProdArray]]

        self.address = '/Raster{}/Resampler'.format(self._raster.uid)

    @property
    def alive(self):
        return self._alive

    # ******************************************************************************************* **
    def receive_resample_and_accumulate(self, qi, prod_idx, sample_fp, resample_fp, subsample_array):
        """Receive message: A resampling operation is ready to be performed

        Parameters
        ----------
        qi: _actors.cached.query_infos.QueryInfos
        prod_idx: int
        sample_fp: None or Footprint of shape (Y, X)
        resample_fp: Footprint of shape (Y', X')
        subsample_array: None or ndarray of shape (Y, X)
        """
        msgs = []

        pi = qi.prod[prod_idx]

        if prod_idx not in self._prod_infos[qi]:
            self._prod_infos[qi][prod_idx] = _ProdArray(pi)
        pr = self._prod_infos[qi][prod_idx]

        is_tile_alone = len(pi.resample_fps) == 1
        is_tile_outside = sample_fp is None
        is_interpolation_needed = not is_tile_outside and not pi.same_grid
        is_interpolation_defered = self._raster.resample_pool is not None

        if is_tile_alone:
            assert pr.arr is None

        if is_tile_alone and is_tile_outside:
            # Case 1: production footprint is fully outside of raster
            assert sample_fp is None
            assert subsample_array is None
            pr.arr = np.full(
                np.r_[resample_fp.shape, len(qi.channel_ids)],
                qi.dst_nodata, self._raster.dtype,
            )
            pr.commit(resample_fp)
            pr.is_post_processed = True

        elif is_tile_alone and sample_fp.almost_equals(pi.fp):
            # Case 2: production footprint is aligned and fully inside raster
            assert subsample_array.shape[:2] == tuple(resample_fp.shape)
            pr.arr = subsample_array
            if self._raster.nodata is not None and self._raster.nodata != qi.dst_nodata:
                pr.arr[pr.arr == self._raster.nodata] = qi.dst_nodata
            pr.arr = pr.arr.astype(self._raster.dtype, copy=False)
            pr.arr = _reorder_channels(qi, pr.arr)

            pr.commit(resample_fp)
            pr.is_post_processed = True

        elif is_tile_alone and not is_interpolation_needed:
            # Case 3: production footprint is aligned and is both inside and outside raster
            pr.arr = np.full(
                np.r_[resample_fp.shape, len(qi.unique_channel_ids)],
                qi.dst_nodata, self._raster.dtype,
            )
            slices = sample_fp.slice_in(resample_fp)
            pr.arr[slices] = subsample_array
            if self._raster.nodata is not None and self._raster.nodata != qi.dst_nodata:
                pr.arr[slices][pr.arr[slices] == self._raster.nodata] = qi.dst_nodata
            pr.arr = _reorder_channels(qi, pr.arr)
            pr.commit(resample_fp)
            pr.is_post_processed = True

        elif not is_tile_alone and is_tile_outside:
            # Case 4: production footprint is fully outside
            pr.commit(resample_fp)

        elif not is_tile_alone and not is_interpolation_needed:
            assert False, 'Without interpolation, there should be only one tile'

        elif is_interpolation_needed and not is_interpolation_defered:
            # Case 5: production footprint not aligned, interpolation on scheduler
            job = self._create_interpolation_work_job(
                qi, prod_idx, sample_fp, resample_fp, subsample_array,
            )
            job.func()
            self._commit_interpolation_work_result(job, None)

        elif is_interpolation_needed and is_interpolation_defered:
            # Case 6: production footprint not aligned, interpolation on pool
            wait = Wait(self, qi, prod_idx, sample_fp, resample_fp, subsample_array)
            self._waiting_jobs.add(wait)
            msgs += [
                Msg(self._waiting_room_address, 'schedule_job', wait)
            ]

        else:
            assert False, """
            is_tile_alone = {}
            is_tile_outside = {}
            is_interpolation_needed = {}
            is_interpolation_defered = {}
            """.format(
                is_tile_alone, is_tile_outside, is_interpolation_needed, is_interpolation_defered
            )

        msgs += self._push_if_done(qi, prod_idx)

        return msgs

    def receive_token_to_working_room(self, job, token):
        """Receive message: Waiting job can proceed to the working room"""
        self._waiting_jobs.remove(job)

        work = self._create_interpolation_work_job(
            job.qi, job.prod_idx, job.sample_fp, job.resample_fp, job.subsample_array,
        )
        self._working_jobs.add(work)

        return [
            Msg(self._working_room_address, 'launch_job_with_token', work, token)
        ]

    def receive_job_done(self, job, result):
        self._working_jobs.remove(job)
        self._commit_interpolation_work_result(job, result)
        return self._push_if_done(job.qi, job.prod_idx)

    def receive_cancel_this_query(self, qi):
        """Receive message: One query was dropped

        Parameters
        ----------
        qi: _actors.cached.query_infos.QueryInfos
        """
        msgs = []

        # Cancel waiting jobs
        jobs_to_kill = [
            job
            for job in self._waiting_jobs
            if job.qi == qi
        ]
        for job in jobs_to_kill:
            msgs += [Msg(self._waiting_room_address, 'unschedule_job', job)]
            self._waiting_jobs.remove(job)

        # Cancel working jobs
        jobs_to_kill = [
            job
            for job in self._working_jobs
            if job.qi == qi
        ]
        for job in jobs_to_kill:
            msgs += [Msg(self._working_room_address, 'cancel_job', job)]
            self._working_jobs.remove(job)

        if qi in self._prod_infos:
            del self._prod_infos[qi]

        return msgs

    def receive_die(self):
        """Receive message: The raster was killed"""
        assert self._alive
        self._alive = False

        msgs = []
        for job in self._waiting_jobs:
            msgs += [Msg(self._waiting_room_address, 'unschedule_job', job)]
        for job in self._working_jobs:
            msgs += [Msg(self._working_room_address, 'cancel_job', job)]
        self._waiting_jobs.clear()
        self._working_jobs.clear()
        self._prod_infos.clear()
        self._raster = None
        return msgs

    # ******************************************************************************************* **
    def _create_interpolation_work_job(self, qi, prod_idx, sample_fp, resample_fp, subsample_array):
        pi = qi.prod[prod_idx]
        pr = self._prod_infos[qi][prod_idx]

        if pr.arr is None:
            pr.arr = np.full(
                np.r_[pi.fp.shape, len(qi.channel_ids)],
                qi.dst_nodata, self._raster.dtype,
            )

        return Work(
            self, qi, prod_idx,
            sample_fp, resample_fp,
            subsample_array, pr.arr
        )

    def _commit_interpolation_work_result(self, work_job, res):
        qi = work_job.qi
        prod_idx = work_job.prod_idx
        resample_fp = work_job.resample_fp
        pr = self._prod_infos[qi][prod_idx]

        pr.commit(resample_fp)

        if self._raster.resample_pool is not None and not self._same_address_space:
            work_job.dst_array_slice[:] = res
        else:
            assert res is None

    def _push_if_done(self, qi, prod_idx):
        msgs = []

        pr = self._prod_infos[qi][prod_idx]
        if pr.done:
            assert pr.arr is not None
            if not pr.is_post_processed:
                pr.arr = _reorder_channels(qi, pr.arr)
                pr.is_post_processed
            msgs += [
                Msg('Producer', 'made_this_array', qi, prod_idx, pr.arr)
            ]
            del self._prod_infos[qi][prod_idx]
            if len(self._prod_infos[qi]) == 0:
                del self._prod_infos[qi]

        return msgs

    # ******************************************************************************************* **

class _ProdArray(object):
    def __init__(self, pi):
        self.arr = None
        self.is_post_processed = False
        self._missing_resample_fps = set(pi.resample_fps)

    def commit(self, resample_fp):
        self._missing_resample_fps.remove(resample_fp)

    @property
    def done(self):
        return len(self._missing_resample_fps) == 0

def _reorder_channels(qi, arr):
    if qi.channel_ids != qi.unique_channel_ids:
        indices = [
            qi.unique_channel_ids.index(bi)
            for bi in qi.channel_ids
        ]
        arr = arr[..., indices]
    return arr

class Wait(ProductionJobWaiting):

    def __init__(self, actor, qi, prod_idx, sample_fp, resample_fp, subsample_array):
        self.qi = qi
        self.prod_idx = prod_idx
        self.sample_fp = sample_fp
        self.resample_fp = resample_fp
        self.subsample_array = subsample_array
        super().__init__(actor.address, qi, prod_idx, 0, self.resample_fp)

class Work(PoolJobWorking):
    def __init__(self, actor, qi, prod_idx, sample_fp, resample_fp, subsample_array, dst_array):
        self.qi = qi
        self.prod_idx = prod_idx
        self.resample_fp = resample_fp
        produce_fp = qi.prod[prod_idx].fp

        dst_array_slice = dst_array[resample_fp.slice_in(produce_fp)]

        if actor._raster.resample_pool is None or actor._same_address_space:
            func = functools.partial(
                _resample_subsample_array,
                sample_fp, resample_fp, subsample_array,
                actor._raster.nodata, qi.dst_nodata,
                qi.interpolation, dst_array_slice,
            )
        else:
            self.dst_array_slice = dst_array_slice
            func = functools.partial(
                _resample_subsample_array,
                sample_fp, resample_fp, subsample_array,
                actor._raster.nodata, qi.dst_nodata,
                qi.interpolation, None,
            )
        actor._raster.debug_mngr.event('object_allocated', func)

        super().__init__(actor.address, func)

def _resample_subsample_array(sample_fp, resample_fp, subsample_array, src_nodata, dst_nodata, interpolation, dst_opt):
    """
    Parameters
    ----------
    sample_fp: Footprint of shape (Y, X)
        source footprint (before resampling)
    resample_fp: Footprint of shape (Y', X')
        destination footprint
    subsample_array: np.ndarray of shape (Y, X)
        source array (sould match sample_fp)
    src_nodata: None or nbr
    dst_nodata: nbr
    interpolation: str
    dst_opt: None or np.ndarray
        optional destination for resample

    Returns
    -------
    None or np.ndarray of shape (Y', X')
    """
    # TODO: Inplace remap
    res = ABackSourceRasterRemapMixin.remap(
        src_fp=sample_fp, dst_fp=resample_fp,
        array=subsample_array, mask=None,
        src_nodata=src_nodata, dst_nodata=dst_nodata,
        mask_mode='dilate', interpolation=interpolation,
    )
    if dst_opt is not None:
        dst_opt[:] = res
        return None
    else:
        return res
