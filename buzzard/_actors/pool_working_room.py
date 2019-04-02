import logging

from buzzard._actors.message import Msg

LOGGER = logging.getLogger(__name__)

class ActorPoolWorkingRoom(object):
    """Actor that takes care of starting/collecting jobs on/off a thread/process pool"""

    def __init__(self, pool):
        """
        Parameter
        ---------
        pool: multiprocessing.pool.Pool (or the multiprocessing.pool.ThreadPool subclass)
        """
        self._pool = pool
        self._jobs = {}
        self._alive = True
        self.address = '/Pool{}/WorkingRoom'.format(id(self._pool))

    @property
    def alive(self):
        return self._alive

    # ******************************************************************************************* **
    def receive_launch_job_with_token(self, job, token):
        """Receive message: Launch this job that has a token from your WaitingRoom

        Parameters
        ----------
        job: _actors.pool_job.PoolJobWorking
        token: _actors.pool_waiting_room._PoolToken (superclass of int)
        """
        assert job not in self._jobs

        future = self._pool.apply_async(job.func)
        self._jobs[job] = (future, token)

        return []

    def receive_salvage_token(self, token):
        """Receive message: Your WaitingRoom allowed a job, but the job does not need to be perfomed
        any more.

        Parameters
        ----------
        token: _actors.pool_waiting_room._PoolToken (superclass of int)
        """
        return [Msg('WaitingRoom', 'salvage_token', token)]

    def receive_cancel_job(self, job):
        """Receive message: A Job you launched can be discarded. Lose the reference to the future

        Parameters
        ----------
        job: _actors.pool_job.PoolJobWorking
        """
        _, token = self._jobs.pop(job)
        return [Msg('WaitingRoom', 'salvage_token', token)]

    def ext_receive_nothing(self):
        """Receive message sent by something else than an actor, still treated synchronously: What's
        up?
        Did a Job finished? Check all futures
        """
        msgs = []

        finished_jobs = [
            job
            for job, (future, _) in self._jobs.items()
            if future.ready()
        ]
        for job in finished_jobs:
            future, token = self._jobs.pop(job)
            res = future.get()
            msgs += [
                Msg(job.sender_address, 'job_done', job, res),
                Msg('WaitingRoom', 'salvage_token', token),
            ]

        return msgs

    def receive_die(self):
        """Receive message: The wrapped pool is no longer used"""
        assert self._alive
        self._alive = False
        if len(self._jobs) > 0:
            LOGGER.warning('Killing an ActorPoolWorkingRoom with {} ongoing jobs'.format(
                len(self._jobs)
            ))

        # Clear attributes *****************************************************
        self._jobs.clear()
        self._pool = None

        return []

    # ******************************************************************************************* **
