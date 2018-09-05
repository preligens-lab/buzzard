import functools
import collections

import multiprocessing as mp
import multiprocessing.pool
import numpy as np

from buzzard._actors.message import Msg
from buzzard._actors.pool_job import CacheJobWaiting, PoolJobWorking

class ActorComputationAccumulator(object):
    """Actor that takes care of accumulating computed slices needed
    to write 1 cache tile"""

    def __init__(self, raster):
        self._raster = raster
        self._alive = True
        self._cache_tiles_accumulations = {}

    @property
    def address(self):
        return '/Raster{}/ComputationAccumulator'.format(self._raster.uid)

    @property
    def alive(self):
        return self._alive

    # ******************************************************************************************* **
    def receive_combine_this_array(self, compute_fp, array):
        msgs = []
        for cache_fp in self._raster.cache_fps_of_compute_fp(compute_fp):
            if cache_fp in self._cache_tiles_accumulations:
                store = self._cache_tiles_accumulations[cache_fp]
            else:
                store = {'missing': self._raster.compute_fps_of_cache_fp(cache_fp), 'ready': {}}
                self._cache_tiles_accumulations[cache_fp] = store
            assert compute_fp in store['missing']
            del store['missing'][compute_fp]
            slices = compute_fp.slice_in(cache_fp)
            store['ready'][compute_fp] = array[slices]
            if len(store['missing']) == 0:
                msgs += [
                    Msg('Merge', 'schedule_one_merge', cache_fp, store['ready'])
                ]
                del self._cache_tiles_accumulations[cache_fp]
        return msgs

    def receive_cancel_this_query(self, qi):
        # TODO: check if cache_fp linked with other queries
        #       if no, delete. if yes do nothing?
        return []


    def receive_die(self):
        """Receive message: The raster was killed"""
        assert self._alive
        self._alive = False

        for store in self._cache_tiles_accumulations.values():
            store.clear()
        self._cache_tiles_accumulations.clear()
        return []

    # ******************************************************************************************* **
