import logging
import functools
import os
import hashlib

from buzzard._actors.message import Msg
from buzzard._actors.pool_job import MaxPrioJobWaiting, PoolJobWorking

open_raster = None # Lazy import
LOGGER = logging.getLogger(__name__)

class ActorFileChecker(object):
    """Actor that takes care of performing various checks on a cache file from a pool"""

    def __init__(self, raster):
        self._raster = raster
        self._alive = True
        io_pool = raster.io_pool
        if io_pool is not None:
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
            work = Work(cache_fp, path)
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
        func = functools.partial(
            _cache_file_check,
            cache_fp, path, len(actor._raster), actor._raster.dtype,
        )
        actor._raster.debug_mngr.event('object_allocated', func)
        super().__init__(actor.address, func)

def _md5(fname):
    """https://stackoverflow.com/a/3431838/4952173"""
    hash_md5 = hashlib.md5()
    with open(fname, "rb") as f:
        for chunk in iter(lambda: f.read(4096), b""):
            hash_md5.update(chunk)
    return hash_md5.hexdigest()

def _cache_file_check(cache_fp, path, band_count, dtype):
    exn = None
    try:
        _is_ok(cache_fp, path, band_count, dtype)
    except Exception as e:
        valid = False
        exn = e
    else:
        valid = True

    if not valid:
        m = 'Removing {}'.format(path)
        m += ' because {}'.format(exn)
        LOGGER.warn(m)
        os.remove(path)

    return valid

def _is_ok(cache_fp, path, band_count, dtype):
    global open_raster
    if open_raster is None:
        from buzzard import open_raster

    with open_raster(path).close as r:
        if r.fp != cache_fp:
            raise RuntimeError('invalid Footprint ({} instead of {})'.format(r.fp, cache_fp))
        if r.dtype != dtype:
            raise RuntimeError('invalid dtype ({} instead of {})'.format(r.dtype, dtype))
        if len(r) != band_count:
            raise RuntimeError('invalid band_count ({} instead of {})'.format(len(r), band_count))

    md5 = path
    md5 = md5.split('.')[-2]
    md5 = md5.split('_')[-1]
    new_md5 = _md5(path)
    if new_md5 != md5:
        raise RuntimeError('invalid md5 ({} instead of {})'.format(new_md5, md5))
    return True
