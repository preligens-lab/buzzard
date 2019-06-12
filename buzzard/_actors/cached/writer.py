import os
import uuid
import functools

import numpy as np

from buzzard._actors.message import Msg
from buzzard._actors.pool_job import CacheJobWaiting, PoolJobWorking

create_raster = None # lazy import

class ActorWriter(object):
    """Actor that takes care of writing to disk a cache tile that has been computed and merged."""

    def __init__(self, raster):
        self._raster = raster
        self._alive = True
        io_pool = raster.io_pool
        if io_pool is not None:
            self._waiting_room_address = '/Pool{}/WaitingRoom'.format(id(io_pool))
            self._working_room_address = '/Pool{}/WorkingRoom'.format(id(io_pool))
        self._waiting_jobs = set()
        self._working_jobs = set()
        self.address = '/Raster{}/Writer'.format(self._raster.uid)

    @property
    def alive(self):
        return self._alive

    # ******************************************************************************************* **
    def receive_write_this_array(self, cache_fp, array):
        """Receive message: Please write this array to disk.

        Parameters
        ----------
        cache_fp: Footprint of shape (Y, X)
        array: ndarray of shape (Y, X, C)
        """
        msgs = []

        if self._raster.io_pool is None:
            # No `io_pool` provided by user, perform write operation right now on this thread.
            work = Work(self, cache_fp, array)
            path = work.func()
            msgs += [Msg('CacheSupervisor', 'cache_file_written', cache_fp, path)]
        else:
            # Enqueue job in the `Pool/WaitingRoom` actor
            wait = Wait(self, cache_fp, array)
            self._waiting_jobs.add(wait)
            msgs += [Msg(self._waiting_room_address, 'schedule_job', wait)]
        return msgs

    def receive_token_to_working_room(self, job, token):
        """Receive message: It is your turn to use the `Pool/WorkingRoom` actor

        Parameters
        ----------
        job: Wait
        token: pool_waiting_room._PoolToken
        """
        self._waiting_jobs.remove(job)
        work = Work(self, job.cache_fp, job.array)
        self._working_jobs.add(work)
        return [
            Msg(self._working_room_address, 'launch_job_with_token', work, token)
        ]

    def receive_job_done(self, job, result):
        """Receive message: Writing operation is complete

        Parameters
        ----------
        job: Work
        result: str
            Path to the written file
        """
        self._working_jobs.remove(job)
        return [Msg('CacheSupervisor', 'cache_file_written', job.cache_fp, result)]

    def receive_die(self):
        """Receive message: The raster was killed (collect by gc or closed by user)"""
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

        return msgs

    # ******************************************************************************************* **

class Wait(CacheJobWaiting):
    """Job to be fed to a PoolWaitingRoom actor"""
    def __init__(self, actor, cache_fp, array):
        self.cache_fp = cache_fp
        self.array = array
        super().__init__(actor.address, actor._raster.uid, self.cache_fp, 2, self.cache_fp)

class Work(PoolJobWorking):
    """Job to be fed to a PoolWorkingRoom actor"""
    def __init__(self, actor, cache_fp, array):
        self.cache_fp = cache_fp

        func = functools.partial(
            _cache_file_write,
            array,
            actor._raster.cache_dir,
            actor._raster.fname_prefix_of_cache_fp(cache_fp),
            '.tif',
            cache_fp,
            {'nodata': actor._raster.nodata},
            actor._raster.wkt_stored,
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

def _cache_file_write(array,
                      dir_path, filename_prefix, filename_suffix,
                      cache_fp, channels_schema, sr):
    """Write this ndarray to disk.

    It can't use the dataset's activation pool because the file must be closed after
    writing to:
    1. flush to disk
    2. checksum
    3. be renamed

    Parameters
    ----------
    array: ndarray of shape (Y, X, C)
        What to write in the cache file
    dir_path: str
        Directory where to create the file
    filename_prefix: str
        First third of the file name
    filename_suffix: str
        Last thirf of the file name
    cache_fp: Footprint of shape (Y, X)
        Footprint of the file
    channels_schema: dict
        Band schema given by user when creating the cached recipe
    sr: str or None
        Spatial reference given by user when creating the cached recipe
    """
    # Step 0. Lazily import buzzard to avoid circular dependencies
    global create_raster
    if create_raster is None:
        from buzzard import create_raster

    # Step 1. Create/close file with a temporary name
    src_path = os.path.join(
        dir_path, 'tmp_' + filename_prefix + str(uuid.uuid4()) + filename_suffix
    )

    # TODO: Allow the user to choose the type and options of cache files
    # TODO: Use driver-object allocator
    options = [
        "TILED=YES",
        "BLOCKXSIZE=256", "BLOCKYSIZE=256",
        "SPARSE_OK=TRUE",
    ]
    assert array.ndim == 3
    with create_raster(src_path, cache_fp, array.dtype, array.shape[-1], channels_schema,
                       sr=sr, options=options).close as r:
        r.set_data(array, channels=None)

    # Step 2. checksum hash file
    checksum = _checksum(src_path)

    # Step 3. move file to its final location
    dst_path = os.path.join(dir_path, filename_prefix + '_' + checksum + filename_suffix)

    # TODO: Undefined if it exists, but it will most likely work
    # TODO: chmod to remove write access?
    os.rename(src_path, dst_path)

    return dst_path
