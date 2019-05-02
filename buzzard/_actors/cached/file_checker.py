import logging
import functools
import os
import contextlib
import multiprocessing as mp
import multiprocessing.pool

import numpy as np

from buzzard._actors.message import Msg
from buzzard._actors.pool_job import MaxPrioJobWaiting, PoolJobWorking
from buzzard._gdal_file_raster import BackGDALFileRaster
from buzzard._tools import conv
from buzzard._footprint import Footprint

LOGGER = logging.getLogger(__name__)

class ActorFileChecker(object):
    """Actor that takes care of performing various checks on a cache file from a pool"""

    def __init__(self, raster):
        self._raster = raster
        self._back_ds = raster.back_ds
        self._alive = True
        io_pool = raster.io_pool
        if io_pool is not None:
            if isinstance(io_pool, mp.pool.ThreadPool):
                self._same_address_space = True
            elif isinstance(io_pool, mp.pool.Pool):
                self._same_address_space = False
            else: # pragma: no cover
                assert False, 'Type should be checked in facade'
            self._waiting_room_address = '/Pool{}/WaitingRoom'.format(id(io_pool))
            self._working_room_address = '/Pool{}/WorkingRoom'.format(id(io_pool))
        self._waiting_jobs = set()
        self._working_jobs = set()
        self.address = '/Raster{}/FileChecker'.format(self._raster.uid)

    @property
    def alive(self):
        return self._alive

    # ******************************************************************************************* **
    def receive_infer_cache_file_status(self, cache_fp, path):
        msgs = []

        if self._raster.io_pool is None:
            work = Work(self, cache_fp, path)
            status = work.func()
            msgs += [Msg(
                'CacheSupervisor', 'inferred_cache_file_status', cache_fp, path, status
            )]
        else:
            wait = Wait(self, cache_fp, path)
            self._waiting_jobs.add(wait)
            msgs += [Msg(self._waiting_room_address, 'schedule_job', wait)]

        return msgs

    def receive_token_to_working_room(self, job, token):
        self._waiting_jobs.remove(job)
        work = Work(self, job.cache_fp, job.path)
        self._working_jobs.add(work)
        return [
            Msg(self._working_room_address, 'launch_job_with_token', work, token)
        ]

    def receive_job_done(self, job, status):
        self._working_jobs.remove(job)
        return [
            Msg('CacheSupervisor', 'inferred_cache_file_status', job.cache_fp, job.path, status)
        ]

    def receive_die(self):
        assert self._alive
        self._alive = False
        msgs = []
        for job in self._waiting_jobs:
            msgs += [Msg(self._waiting_room_address, 'unschedule_job', job)]
        for job in self._working_jobs:
            msgs += [Msg(self._working_room_address, 'cancel_job', job)]
        self._waiting_jobs.clear()
        self._working_jobs.clear()
        self._raster = None
        self._back_ds = None
        return msgs

    # ******************************************************************************************* **

class Wait(MaxPrioJobWaiting):
    def __init__(self, actor, cache_fp, path):
        self.cache_fp = cache_fp
        self.path = path
        super().__init__(actor.address)

class Work(PoolJobWorking):
    def __init__(self, actor, cache_fp, path):
        self.cache_fp = cache_fp
        self.path = path
        if actor._raster.io_pool is None or actor._same_address_space:
            func = functools.partial(
                _cache_file_check,
                cache_fp, path, len(actor._raster), actor._raster.dtype,
                actor._back_ds
            )
        else:
            func = functools.partial(
                _cache_file_check,
                cache_fp, path, len(actor._raster), actor._raster.dtype,
                None,
            )
        actor._raster.debug_mngr.event('object_allocated', func)
        super().__init__(actor.address, func)

def _checksum(fname, buffer_size=512 * 1024, dtype='uint64'):
    # https://github.com/airware/buzzard/pull/39/#discussion_r239071556
    dtype = np.dtype(dtype)
    dtypesize = dtype.itemsize
    assert buffer_size % dtypesize == 0
    assert np.issubdtype(dtype, np.unsignedinteger)

    acc = dtype.type(0)
    with open(fname, "rb") as f:
        with np.warnings.catch_warnings():
            np.warnings.filterwarnings('ignore', r'overflow encountered')

            for chunk in iter(lambda: f.read(buffer_size), b""):
                head = np.frombuffer(chunk, dtype, count=len(chunk) // dtypesize)
                head = np.add.reduce(head, dtype=dtype, initial=acc)
                acc += head

                tailsize = len(chunk) % dtypesize
                if tailsize > 0:
                    # This should only be needed for file's tail
                    tail = chunk[-tailsize:] + b'\0' * (dtypesize - tailsize)
                    tail = np.frombuffer(tail, dtype)
                    acc += tail
        return '{:016x}'.format(np.asscalar(acc))

def _cache_file_check(cache_fp, path, channel_count, dtype, back_ds_opt):
    checksum = path
    checksum = checksum.split('.')[-2]
    checksum = checksum.split('_')[-1]
    new_checksum = _checksum(path)
    if new_checksum != checksum:
        if back_ds_opt is not None:
            back_ds_opt.deactivate(path)
        LOGGER.warning('Removing {} because invalid checksum ({} instead of {})'.format(
            path, new_checksum, checksum,
        ))
        os.remove(path)
        return False

    allocator = lambda: BackGDALFileRaster.open_file(path, 'GTiff', [], 'r') # This may raise
    with contextlib.ExitStack() as stack:
        try:
            if back_ds_opt is None:
                gdal_ds = allocator()
            else:
                gdal_ds = stack.enter_context(back_ds_opt.acquire_driver_object(path, allocator))

            file_fp = Footprint(
                gt=gdal_ds.GetGeoTransform(),
                rsize=(gdal_ds.RasterXSize, gdal_ds.RasterYSize),
            )
            file_dtype = conv.dtype_of_gdt_downcast(gdal_ds.GetRasterBand(1).DataType)
            file_len = gdal_ds.RasterCount
            if file_fp != cache_fp: # pragma: no cover
                raise RuntimeError('invalid Footprint of {}({} instead of {})'.format(
                    path, file_fp, cache_fp
                ))
            if file_dtype != dtype: # pragma: no cover
                raise RuntimeError('invalid dtype of {}({} instead of {})'.format(
                    path, file_dtype, dtype
                ))
            if file_len != channel_count: # pragma: no cover
                raise RuntimeError('invalid channel_count of {}({} instead of {})'.format(
                    path, file_len, channel_count
                ))
            del gdal_ds
        except Exception:
            # Those exceptions should not trigger a cache file removal, because it might originate
            # from a mistake in the code that does not mean that those files are corrupted. For exemple:
            # - Maximum number of file descriptors reach
            # - Mismatch in cache directories path
            back_ds_opt.deactivate(path)
            raise

    return True
