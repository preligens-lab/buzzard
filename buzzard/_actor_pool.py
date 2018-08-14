import multiprocessing as mp
import multiprocessing.pool

from _actor_computation import ActorMixinComputation
from _actor_merge import ActorMixinMerge
from _actor_write import ActorMixinWrite

class ActorPool(object):
    """This actor may be used multiple times as virtually different actors. This way the waiting
    room is shared amongst different actors.

    Works are not scheduled right away in the pool to avoid massive trafic jams. They instead
    wait in a waiting room and are scheduled one by one.
    """

    def __init__(self, pool):
        self._pool = pool
        self._working_jobs = []
        self._waiting_jobs = []
        if isinstance(pool, mp.ThreadPool):
            self._same_address_space = True
        elif isinstance(pool, mp.Pool):
            self._same_address_space = False
        else:
            raise NameError("Unknown pool type {}".format(type(pool)))

    # ******************************************************************************************* **
    def receive_nothing(self):
        msgs = []
        return msgs

    # ******************************************************************************************* **
    @property
    def same_address_space(self):
        return self._same_address_space

    @property
    def pool(self):
        return self._pool

    def append_waiting(self, job):
        self._waiting_jobs.append(job)

    def append_working(self, job):
        self._working_jobs.append(job)

    def discard_waitings(self, predicate):
        kill_indices = [
            i
            for i, job in enumerate(self._pool_actor.waiting)
            if predicate(job)
        ]
        for i in reversed(kill_indices):
            self._pool_actor.waiting.pop(i)

    def discard_workings(self, predicate):
        kill_indices = [
            i
            for i, job in enumerate(self._pool_actor.working)
            if predicate(job)
        ]
        for i in reversed(kill_indices):
            self._pool_actor.working.pop(i)

    # ******************************************************************************************* **

class WaitingJob(object):

    def __init__(self, de_qui_id_la_prio, callback):
        self.callback = callback

class WorkingJob(object):

    def __init__(self, future, callback):
        self.future = future
        self.callback = callback
