import functools

from buzzard._actors.message import Msg
from buzzard._actors.pool_job import CacheJobWaiting, PoolJobWorking

class ActorWriter(object):
    """Actor that takes care of writing cache tiles"""

    def __init__(self, raster):
        self._raster = raster
        self._alive = True
        io_pool = raster.io_pool
        self._waiting_room_address = '/Pool{}/WaitingRoom'.format(id(io_pool))
        self._working_room_address = '/Pool{}/WorkingRoom'.format(id(io_pool))
        self._waiting_jobs = set()
        self._working_jobs = set()

    @property
    def address(self):
        return '/Raster{}/Writer'.format(self._raster.uid)

    @property
    def alive(self):
        return self._alive

    # ******************************************************************************************* **
    def receive_write_this_array(self, cache_fp, array, path):
        wait = Wait(self, cache_fp, array, path)
        self._waiting_jobs.add(wait)
        return [
            Msg(self._waiting_room_address, 'schedule_job', wait)
        ]

    def receive_token_to_working_room(self, job, token):
        self._waiting_jobs.remove(job)

        work = Work(self, job.cache_fp, job.array, job.path)
        self._working_jobs.add(work)
        return [
            Msg(self._working_room_address, 'launch_job_with_token', work, token)
        ]

    def receive_job_done(self, job, _result):
        # Result is None: write doesn't return an array
        return [
            Msg('CacheSupervisor', 'cache_file_written',
                job.cache_fp, job.path,
            )
        ]

    def receive_cancel_this_query(self, qi):
        msgs = []
        # TODO: find a way to link waiting writes to a set of qi's
        #       if there is no qi left in the set, set priority to np.inf
        #       else, set priority according to qi's left in the set after 
        #       the removal of `qi` (the parameter)
        return []


    def receive_die(self):
        """Receive message: The raster was killed"""
        assert self._alive
        self._alive = False
        # TODO: set the priority of waiting jobs to np.inf
        return []

    # ******************************************************************************************* **

class Wait(CacheJobWaiting):

    def __init__(self, actor, cache_fp, array, path):
        self.cache_fp = cache_fp
        self.array = array
        self.path = path
        # TODO: set action priority other than 1 
        # TODO: raster uid
        # TODO: fp = cache fp? (last parameter)
        super().__init__(actor.address, actor._raster.uid, self.cache_fp, 1, self.cache_fp)

class Work(PoolJobWorking):
    def __init__(self, actor, cache_fp, array, path):
        self.cache_fp = cache_fp

        func = functools.partial(
            _cache_file_write,
            cache_fp, array, path,
        )

        super().__init__(actor.address, func)

def _cache_file_write(cache_fp, array, path):
    """
    Parameters
    ----------
    cache_fp: Footprint
        Footprint of the cache file
    array: np.array
    path: str
    """
    assert (True or False) == 'That is the TODO question'