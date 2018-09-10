from buzzard._actors.message import Msg
from buzzard._actors.pool_job import CacheJobWaiting, PoolJobWorking

import numpy as np


class ActorComputer(object):
    """Actor that takes care of waiting for cache tiles reads and launching resamplings"""

    def __init__(self, raster):
        self._raster = raster
        self._alive = True


    @property
    def address(self):
        return '/Raster{}/Producer'.format(self._raster.uid)

    @property
    def alive(self):
        return self._alive

    # ******************************************************************************************* **
    def receive_compute_this_array(self, qi, prod_idx):
        """Receive message: Start making this array"""
        msgs = []
        # TODO
        return msgs

    def ext_receive_nothing(self):
        """Receive message sent by something else than an actor, still treated synchronously: What's
        up?
        Was an output queue sinked?
        Was an output queue collected by gc?
        """
        msgs = []
        # TODO
        return msgs

    def receive_token_to_working_room(self, job, token):
        self._waiting_jobs.remove(job)
        # TODO

    def receive_job_done(self, job, result):
        if self._same_address_space:
            assert result is None
            array = self.dst_array
        else:
            array = result
        self._working_jobs.remove(job)

        # TODO: where to define path????
        return [
            Msg('Writer', 'write_this_array',
                job.cache_fp, array, job.path,
            )
        ]

    def receive_cancel_this_query(self, qi):
        """Receive message: One query was dropped

        Parameters
        ----------
        qi: _actors.cached.query_infos.QueryInfos
        """
        # TODO
        return []

    def receive_die(self):
        """Receive message: The raster was killed"""
        assert self._alive
        self._alive = False

        # TODO
        return []

    # ******************************************************************************************* **

class Wait(CacheJobWaiting):
    # TODO: inherit not from CacheJobWaiting but from another thing?
    def __init__(self, actor, qi, cache_fp, array_of_compute_fp):
        # TODO
        super().__init__(actor.address, actor._raster.uid, self.cache_fp, 1, self.cache_fp)

class Work(PoolJobWorking):
    def __init__(self, actor, qi, cache_fp, array_of_compute_fp, dst_array):
        # TODO
        pass
        # super().__init__(actor.address, func)