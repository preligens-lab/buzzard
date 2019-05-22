import functools
import collections
import multiprocessing as mp
import multiprocessing.pool
import contextlib

import numpy as np

from buzzard._actors.message import Msg
from buzzard._actors.pool_job import ProductionJobWaiting, PoolJobWorking
from buzzard import _tools
from buzzard._gdal_file_raster import BackGDALFileRaster

class ActorReader(object):
    """Actor that takes care of reading cache tiles"""

    def __init__(self, raster):
        self._raster = raster
        self._back_ds = raster.back_ds
        self._alive = True
        io_pool = raster.io_pool
        if io_pool is not None:
            self._waiting_room_address = '/Pool{}/WaitingRoom'.format(id(io_pool))
            self._working_room_address = '/Pool{}/WorkingRoom'.format(id(io_pool))
            if isinstance(io_pool, mp.pool.ThreadPool):
                self._same_address_space = True
            elif isinstance(io_pool, mp.pool.Pool):
                self._same_address_space = False
            else: # pragma: no cover
                assert False, 'Type should be checked in facade'
        self._waiting_jobs = set()
        self._working_jobs = set()

        self._sample_array_per_prod_tile = (
            collections.defaultdict(dict)
        ) # type: Mapping[CachedQueryInfos, Mapping[int, np.ndarray]]
        self._missing_cache_fps_per_prod_tile = (
            collections.defaultdict(dict)
        ) # type: Mapping[CachedQueryInfos, Mapping[int, Set[Footprint]]]
        self.address = '/Raster{}/Reader'.format(self._raster.uid)

    @property
    def alive(self):
        return self._alive

    # ******************************************************************************************* **
    def receive_sample_cache_file_to_unique_array(self, qi, prod_idx, cache_fp, path):
        msgs = []

        if self._raster.io_pool is None:
            work = self._create_work_job(qi, prod_idx, cache_fp, path)
            work.func()
            msgs += self._commit_work_result(work, None)
        else:
            wait = Wait(self, qi, prod_idx, cache_fp, path)
            self._waiting_jobs.add(wait)
            msgs += [Msg(self._waiting_room_address, 'schedule_job', wait)]

        return msgs

    def receive_token_to_working_room(self, job, token):
        self._waiting_jobs.remove(job)
        work = self._create_work_job(job.qi, job.prod_idx, job.cache_fp, job.path)
        self._working_jobs.add(work)
        return [
            Msg(self._working_room_address, 'launch_job_with_token', work, token)
        ]

    def receive_job_done(self, job, result):
        self._working_jobs.remove(job)
        return self._commit_work_result(job, result)

    def receive_cancel_this_query(self, qi):
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

        # Clean datastructures
        if qi in self._sample_array_per_prod_tile:
            del self._sample_array_per_prod_tile[qi]
            del self._missing_cache_fps_per_prod_tile[qi]

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

        self._sample_array_per_prod_tile.clear()
        self._missing_cache_fps_per_prod_tile.clear()
        self._raster = None
        self._back_ds = None
        return msgs

    # ******************************************************************************************* **
    def _create_work_job(self, qi, prod_idx, cache_fp, path):
        if prod_idx not in self._sample_array_per_prod_tile[qi]:
            # Allocate sample array
            # If no interpolation or nodata conversion is necessary, this is the array that will be
            # returned in the output queue
            full_sample_fp = qi.prod[prod_idx].sample_fp
            self._sample_array_per_prod_tile[qi][prod_idx] = np.empty(
                np.r_[full_sample_fp.shape, len(qi.unique_channel_ids)],
                self._raster.dtype,
            )
            self._raster.debug_mngr.event(
                'object_allocated',
                self._sample_array_per_prod_tile[qi][prod_idx]
            )
            self._missing_cache_fps_per_prod_tile[qi][prod_idx] = set(qi.prod[prod_idx].cache_fps)

        dst_array = self._sample_array_per_prod_tile[qi][prod_idx]
        return Work(self, qi, prod_idx, cache_fp, path, dst_array)

    def _commit_work_result(self, job, result):
        if self._raster.io_pool is None or self._same_address_space:
            assert result is None
        else:
            job.dst_array_slice[:] = result

        dst_array = self._sample_array_per_prod_tile[job.qi][job.prod_idx]
        self._missing_cache_fps_per_prod_tile[job.qi][job.prod_idx].remove(job.cache_fp)

        # Perform fine grain garbage collection
        if len(self._missing_cache_fps_per_prod_tile[job.qi][job.prod_idx]) == 0:
            # Done reading for that `(qi, prod_idx)`
            del self._missing_cache_fps_per_prod_tile[job.qi][job.prod_idx]
            del self._sample_array_per_prod_tile[job.qi][job.prod_idx]

        if len(self._missing_cache_fps_per_prod_tile[job.qi]) == 0:
            # Not reading for that `qi`
            del self._missing_cache_fps_per_prod_tile[job.qi]
            del self._sample_array_per_prod_tile[job.qi]

        return [
            Msg('CacheExtractor', 'sampled_a_cache_file_to_the_array',
                job.qi, job.prod_idx, job.cache_fp, dst_array,
            )
        ]

    # ******************************************************************************************* **

class Wait(ProductionJobWaiting):
    def __init__(self, actor, qi, prod_idx, cache_fp, path):
        self.qi = qi
        self.prod_idx = prod_idx
        self.cache_fp = cache_fp
        self.sample_fp = cache_fp & qi.prod[prod_idx].sample_fp
        self.path = path
        super().__init__(actor.address, qi, prod_idx, 1, self.sample_fp)

class Work(PoolJobWorking):
    def __init__(self, actor, qi, prod_idx, cache_fp, path, dst_array):
        self.qi = qi
        self.prod_idx = prod_idx
        self.cache_fp = cache_fp
        raster = actor._raster
        full_sample_fp = qi.prod[prod_idx].sample_fp
        sample_fp = full_sample_fp & cache_fp

        dst_array_slice = dst_array[sample_fp.slice_in(full_sample_fp)]

        if actor._raster.io_pool is None or actor._same_address_space:
            func = functools.partial(
                _cache_file_read,
                path, cache_fp, actor._raster.dtype, qi.unique_channel_ids, sample_fp, dst_array_slice,
                actor._back_ds,
            )
        else:
            self.dst_array_slice = dst_array_slice
            func = functools.partial(
                _cache_file_read,
                path, cache_fp, actor._raster.dtype, qi.unique_channel_ids, sample_fp, None, None,
            )
        actor._raster.debug_mngr.event('object_allocated', func)
        super().__init__(actor.address, func)

def _cache_file_read(path, cache_fp, dtype, channel_ids, sample_fp, dst_opt, back_ds_opt):
    """
    Parameters
    ----------
    path: str
    cache_fp: Footprint
        Should be the Footprint of the cache file
    dtype: np.dtype
        Should be the dtype of the cache file
    channel_ids: sequence of int
    sample_fp: Footprint
        Rect of `cache_fp` to read
    dst_opt: None or np.ndarray
        optional destination for read
    """

    allocator = lambda: BackGDALFileRaster.open_file(path, 'GTiff', [], 'r')
    with contextlib.ExitStack() as stack:
        if back_ds_opt is None:
            gdal_ds = allocator()
        else:
            gdal_ds = stack.enter_context(back_ds_opt.acquire_driver_object(path, allocator))

        # Check raster
        if gdal_ds is None: # pragma: no cover
            raise RuntimeError("Could not open {path}, what happend to it?".format(
                path
            ))
        if (gdal_ds.RasterXSize, gdal_ds.RasterYSize) != tuple(cache_fp.rsize): # pragma: no cover
            raise RuntimeError('{} was expected to have rsize {}, not {}'.format(
                path,
                tuple(cache_fp.rsize),
                (gdal_ds.RasterXSize, gdal_ds.RasterYSize),
            ))
        stored_dtype = _tools.conv.dtype_of_gdt_downcast(gdal_ds.GetRasterBand(1).DataType)
        if dtype != stored_dtype: # pragma: no cover
            raise RuntimeError('{} was expected to have dtype {}, not {}'.format(
                path,
                dtype,
                stored_dtype,
            ))

        # Allocate if ProcessPool
        if dst_opt is None:
            dst = np.empty(np.r_[sample_fp.shape, len(channel_ids)], dtype)
            ret = dst
        else:
            dst = dst_opt
            ret = None

        # Perform read
        rtlx, rtly = cache_fp.spatial_to_raster(sample_fp.tl)
        for i, ci in enumerate(channel_ids):
            b = gdal_ds.GetRasterBand(ci + 1)
            a = b.ReadAsArray(
                int(rtlx),
                int(rtly),
                int(sample_fp.rsizex),
                int(sample_fp.rsizey),
                buf_obj=dst[..., i],
            )
            del b
            if a is None: # pragma: no cover
                raise RuntimeError('Could not read channel_id {}'.format(ci))
    del gdal_ds

    # Return
    return ret
