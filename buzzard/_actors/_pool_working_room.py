import multiprocessing as mp
import multiprocessing.pool

class ActorPoolWorkingRoom(object):
    """Actor that takes care of starting/collecting jobs off a thread/process pool"""

    def __init__(self, pool):
        pool_id = id(pool)
        self._pool = pool
        self._jobs = {}
        self._waiting_room_address = '/Pool{}/WaitingRoom'.format(id(self._pool))

        if isinstance(pool, mp.pool.ThreadPool):
            self._same_address_space = True
        elif isinstance(pool, mp.Pool):
            self._same_address_space = False
        else:
            assert False, 'Unknown pool type'

    @property
    def address(self):
        return '/Pool{}/WorkingRoom'.format(id(self._pool))

    # ******************************************************************************************* **
    def receive_launch_job_with_token(self, job, token):
        """Receive message: Launch this job that has a token from your WaitingRoom"""
        assert job not in self._jobs

        # apply_async(func, args=(), kwds={}, callback=None, error_callback=None)
        future = self._pool.apply_async(
            job.fn,
            job.args,
            job.kwds,
        )
        self._jobs[job] = (future, token)
        return []

    def receive_salvage_token(self, token):
        """Receive message: Your WaitingRoom allowed a job, but the job does not need to be perfomed
        any more.
        """
        return [Msg(self._waiting_room_address, 'salvage_token', token)]

    def receive_cancel_job(self, job):
        """Receive message: A Job you launched can be discarded. Loose the reference to the future
        """
        _, token = self._jobs.pop(job)
        return [Msg(self._waiting_room_address, 'salvage_token', token)]

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
                Msg(self._waiting_room_address, 'salvage_token', token),
            ]

        return msgs

    # ******************************************************************************************* **
