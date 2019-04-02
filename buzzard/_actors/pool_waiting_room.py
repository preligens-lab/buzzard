from typing import Set, Dict, Tuple
import itertools
import operator
import functools
import logging
import uuid # For mypy

import sortedcontainers
import numpy as np

from buzzard._footprint import Footprint # For mypy
from buzzard._actors.message import Msg
from buzzard._actors.pool_job import PoolJobWaiting, MaxPrioJobWaiting, ProductionJobWaiting, CacheJobWaiting
from buzzard._actors.priorities import dummy_priorities, Priorities
from buzzard._actors.cached.query_infos import CachedQueryInfos

LOGGER = logging.getLogger(__name__)
OVERLOAD = 2

class ActorPoolWaitingRoom(object):
    """Actor that takes care of prioritizing jobs waiting for spots in a thread/process pool.

    It gives out tokens to allow jobs to enter the `ActorPoolWorkingRoom`. There are as many tokens
    as spots in the underlying thread/process pool.

    It accepts 3 types of `PoolJobWaiting`
    - `MaxPrioJobWaiting`
      - Rank 0 job, has priority over the other jobs.
      - Stored in a set
      - Used by `cached.FileChecker`
    - `ProductionJobWaiting`
      - Rank 1 job
      - Stored in many data structures
      - Used by `Reader`, `Resampler`, `cached.Computer`
    - `CacheJobWaiting`
      - Rank 1 job
      - Stored in many data structures
      - Used by `cached.Merger`, `cached.Writer`

    """

    def __init__(self, pool):
        """
        Parameters
        ----------
        pool: multiprocessing.pool.Pool (or the multiprocessing.pool.ThreadPool subclass)
        """
        self._alive = True

        # `global_priorities` contains all the methods necessary to establish the priority of a
        # `prod_job` or a `cache_job`. This object is updated by
        # `receive_global_priorities_update` as soon as there is an update.
        self._global_priorities = dummy_priorities # type: Priorities

        # Tokens *****************************************************
        pool_id = id(pool)
        self._pool_id = pool_id
        self._token_count = pool._processes + OVERLOAD
        short_id = short_id_of_id(pool_id)
        self._tokens = {
            # This has no particular meaning, the only hard requirement is just to have
            # different tokens in a pool.
            _PoolToken(short_id * 1000 + i)
            for i in range(self._token_count)
        }
        self._all_tokens = set(self._tokens)

        # Rank 0 jobs ************************************************
        self._jobs_maxprio = set() # type: Set[MaxPrioJobWaiting]

        # Rank 1 jobs ************************************************
        # For low complexity operations
        self._jobs_prod = set() # type: Set[ProductionJobWaiting]
        self._jobs_cache = set() # type: Set[CacheJobWaiting]

        self._dict_of_prio_per_r1job = {} # type: Dict[PoolJobWaiting, Tuple[int, ...]]
        self._sset_of_prios = sortedcontainers.SortedSet()
        self._dict_of_r1jobs_per_prio = {} # type: Dict[Tuple[int], Set[PoolJobWaiting]]

        self._prod_jobs_of_query = {} # type: Dict[CachedQueryInfos, Set[ProductionJobWaiting]]
        self._cache_jobs_of_cache_fp = {} # type: Dict[Tuple[uuid.UUID, Footprint], Set[CacheJobWaiting]]

        # Shortcuts **************************************************
        # For fast iteration / cleanup
        self._job_sets = [self._jobs_maxprio, self._jobs_prod, self._jobs_cache]
        self._data_structures = self._job_sets + [
            self._dict_of_prio_per_r1job,
            self._sset_of_prios,
            self._dict_of_r1jobs_per_prio,
            self._prod_jobs_of_query,
            self._cache_jobs_of_cache_fp,
        ]
        self.address = '/Pool{}/WaitingRoom'.format(self._pool_id)

    @property
    def alive(self):
        return self._alive

    # ******************************************************************************************* **
    def receive_schedule_job(self, job):
        """Receive message: Schedule this job someday

        Parameters
        ----------
        job: _actors.pool_job.PoolJobWaiting
        """
        if len(self._tokens) != 0:
            # If job can be started straight away, do so.
            assert self._job_count == 0
            return [
                Msg(job.sender_address, 'token_to_working_room', job, self._tokens.pop())
            ]
        else:
            # Store job for later invocation
            self._store_job(job)
        return []

    def receive_unschedule_job(self, job):
        """Receive message: Forget about this waiting job

        Parameters
        ----------
        job: _actors.pool_job.PoolJobWaiting
        """
        self._unstore_job(job)
        return []

    def receive_global_priorities_update(self, global_priorities, query_updates, cache_tile_updates):
        """Receive message: Update your jobs priorities

        Parameters
        ----------
        global_priorities: Priorities
        query_updates: set of CachedQueryInfos
        cache_tile_updates: set of (raster_uid, Footprint)
        """
        # Update the version of `global_priorities`
        self._global_priorities = global_priorities

        # Update the production jobs
        for qi in query_updates & self._prod_jobs_of_query.keys():
            for job in list(self._prod_jobs_of_query[qi]):
                # Update priority
                self._unstore_job(job)
                self._store_job(job)

        # Update the cache jobs
        for raster_uid, cache_fp in cache_tile_updates & self._cache_jobs_of_cache_fp.keys():
            key = (raster_uid, cache_fp)
            for job in list(self._cache_jobs_of_cache_fp[key]):
                # Update priority
                self._unstore_job(job)
                self._store_job(job)

        return []

    def receive_salvage_token(self, token):
        """Receive message: A Job is done/cancelled, allow some other jobs

        Parameters
        ----------
        token: _PoolToken
        """
        assert token in self._all_tokens, 'Received a token that is not owned by this waiting room'
        assert token not in self._tokens, 'Received a token that is already here'
        self._tokens.add(token)

        job_count = self._job_count
        if job_count == 0:
            return []
        else:
            token_count = len(self._tokens)
            assert token_count == 1, """The way this class is designed, this point in code should only
            reached if token_count == 1"""

            job = self._unstore_most_urgent_job()

        return [Msg(
            job.sender_address, 'token_to_working_room', job, self._tokens.pop()
        )]

    def receive_die(self):
        """Receive message: The wrapped pool is no longer used"""
        assert self._alive
        self._alive = False
        if self._job_count:
            LOGGER.warning('Killing an ActorPoolWaitingRoom with {} waiting jobs'.format(
                self._job_count,
            ))

        # Clear attributes *****************************************************
        self._prios = dummy_priorities
        for ds in self._data_structures:
            ds.clear()

        return []

    # ******************************************************************************************* **
    # Misc *********************************************************************
    @property
    def _job_count(self):
        return sum(map(len, self._job_sets))

    # Job storage operations ***************************************************
    def _store_job(self, job):
        """Compute the priority of a job and register it in the right objects"""
        assert all(
            job not in set_
            for set_ in self._job_sets
        )
        if isinstance(job, MaxPrioJobWaiting):
            self._jobs_maxprio.add(job)
        else:
            if isinstance(job, ProductionJobWaiting):
                self._jobs_prod.add(job)
                if job.qi not in self._prod_jobs_of_query:
                    self._prod_jobs_of_query[job.qi] = set()
                self._prod_jobs_of_query[job.qi].add(job)
                prio = self._global_priorities.prio_of_prod_tile(job.qi, job.prod_idx)
                cx, cy = job.fp.c
                prio += (-cy, +cx, job.action_priority,)

            elif isinstance(job, CacheJobWaiting):
                self._jobs_cache.add(job)
                key = job.raster_uid, job.cache_fp
                if key not in self._cache_jobs_of_cache_fp:
                    self._cache_jobs_of_cache_fp[key] = set()
                self._cache_jobs_of_cache_fp[key].add(job)
                prio = self._global_priorities.prio_of_cache_tile(job.raster_uid, job.cache_fp)
                cx, cy = job.fp.c
                prio += (-cy, +cx, job.action_priority,)

            else: # pragma: no cover
                assert False

            self._dict_of_prio_per_r1job[job] = prio
            if prio in self._dict_of_r1jobs_per_prio:
                self._dict_of_r1jobs_per_prio[prio].add(job)
            else:
                self._dict_of_r1jobs_per_prio[prio] = {job}
                self._sset_of_prios.add(prio)

    def _unstore_job(self, job):
        """Unregister a job from the right objects"""
        if isinstance(job, MaxPrioJobWaiting):
            self._jobs_maxprio.remove(job)
        else:
            if isinstance(job, ProductionJobWaiting):
                self._jobs_prod.remove(job)
                self._prod_jobs_of_query[job.qi].remove(job)
                if len(self._prod_jobs_of_query[job.qi]) == 0:
                    del self._prod_jobs_of_query[job.qi]

            elif isinstance(job, CacheJobWaiting):
                self._jobs_cache.remove(job)
                key = job.raster_uid, job.cache_fp
                self._cache_jobs_of_cache_fp[key].remove(job)
                if len(self._cache_jobs_of_cache_fp[key]) == 0:
                    del self._cache_jobs_of_cache_fp[key]
            else: # pragma: no cover
                assert False

            prio = self._dict_of_prio_per_r1job.pop(job)
            self._dict_of_r1jobs_per_prio[prio].remove(job)
            if len(self._dict_of_r1jobs_per_prio[prio]) == 0:
                del self._dict_of_r1jobs_per_prio[prio]
                self._sset_of_prios.remove(prio)

    def _unstore_most_urgent_job(self):
        assert self._job_count > 0

        # Unstore a rank 0 job
        if len(self._jobs_maxprio) > 0:
            return self._jobs_maxprio.pop() # Pop an arbitrary one

        # Unstore a rank 1 job
        prio = self._sset_of_prios[0]
        job = next(iter(self._dict_of_r1jobs_per_prio[prio])) # Pop an arbitrary one

        self._unstore_job(job)
        return job

    # ******************************************************************************************* **

class _PoolToken(int):
    pass

def grouper(iterable, n, fillvalue=None):
    """itertools recipe: Collect data into fixed-length chunks or blocks
    grouper('ABCDEFG', 3, 'x') --> ABC DEF Gxx
    """
    args = [iter(iterable)] * n
    return itertools.zip_longest(*args, fillvalue=fillvalue)

def short_id_of_id(id, max_digit_count=3):
    """Shorten an integer
    Group of digits are xor'd together
    """
    id = int(id)
    it = reversed(str(id))
    it = grouper(it, max_digit_count, '0')
    it = (
        int(''.join(reversed(char_list)))
        for char_list in it
    )
    short_id = functools.reduce(operator.xor, it) % 10 ** max_digit_count
    return short_id
