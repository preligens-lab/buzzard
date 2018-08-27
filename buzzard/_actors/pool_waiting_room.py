import itertools
import operator
import functools
import logging

from buzzard._actors.message import Msg
from buzzard._actors.priorities import dummy_priorities

LOGGER = logging.getLogger(__name__)

class ActorPoolWaitingRoom(object):
    """Actor that takes care of prioritizing jobs waiting for space in a thread/process pool"""

    def __init__(self, pool):
        """
        Parameter
        ---------
        pool: multiprocessing.Pool (or multiprocessing.pool.ThreadPool superclass)
        """
        pool_id = id(pool)
        self._pool_id = pool_id
        self._token_count = pool._processes
        short_id = short_id_of_id(pool_id)
        self._tokens = {
            # This has no particular meaning, the only hard requirement is just to have
            # different tokens in a pool.
            _PoolToken(short_id * 1000 + i)
            for i in range(pool._processes)
        }
        self._all_tokens = set(self._tokens)
        self._jobs = {}
        self._prios = dummy_priorities
        self._alive = True

    @property
    def address(self):
        return '/Pool{}/WaitingRoom'.format(self._pool_id)

    @property
    def alive(self):
        return self._alive

    # ******************************************************************************************* **
    def receive_schedule_job(self, job):
        """Receive message: Schedule this job someday

        Parameters
        ----------
        job: _caching.pool_job.PoolJobWaiting
        """
        self._jobs[job] = 42
        return self._schedule_jobs()

    def receive_unschedule_job(self, job):
        """Receive message: Forget about this waiting job

        Parameters
        ----------
        job: _caching.pool_job.PoolJobWaiting
        """
        del self._jobs[job]
        return []

    def receive_global_priorities_update(self, prios):
        """Receive message: Update your heursitic data used to prioritize jobs

        Parameters
        ----------
        job: _caching.priorities.Priorities
        """
        self._prios = prios
        return []

    def receive_salvage_token(self, token):
        """Receive message: A Job is done/cancelled, allow some other job

        Parameters
        ----------
        token: _PoolToken
        """
        assert token in self._all_tokens, 'Received a token that is not owned by this waiting room'
        assert token not in self._tokens, 'Received a token that is already here'
        self._tokens.add(token)
        return self._schedule_jobs()

    def receive_die(self):
        """Receive message: The wrapped pool is no longer used"""
        assert self._alive
        self._alive = False
        if len(self._jobs) > 0:
            LOGGER.warn('Killing an ActorPoolWaitingRoom with {} waiting jobs'.format(
                len(self._jobs)
            ))

        # Clear attributes *****************************************************
        self._jobs.clear()
        self._prios = dummy_priorities

        return []

    # ******************************************************************************************* **
    def _schedule_jobs(self):
        if not self._tokens or not self._jobs:
            return []
        msgs = []
        prios = self._prios
        send_count = min(len(self._tokens), len(self._jobs))

        for _ in range(send_count):
            most_urgent_job = ...
            del self._jobs[most_urgent_job]
            msgs += [Msg(
                most_urgent_job.sender_address,
                'token_to_working_room',
                most_urgent_job,
                self._tokens.pop(),
            )]
        return msgs

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
