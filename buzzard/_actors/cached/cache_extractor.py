from typing import Set, Mapping

import collections

from buzzard._actors.message import Msg
from buzzard._footprint import Footprint
from buzzard._actors.cached.query_infos import CachedQueryInfos

class ActorCacheExtractor(object):
    """Actor that takes care of delaying reading operations according to cache state"""

    def __init__(self, raster):
        self._raster = raster
        self._alive = True

        self._path_of_cache_files_ready = {} # type: Mapping[Footprint, str]
        self._reads_waiting_for_cache_fp = (
            collections.defaultdict(lambda: collections.defaultdict(set))
        ) # type: Mapping[Footprint, Mapping[CachedQueryInfos, Set[int]]]
        self.address = '/Raster{}/CacheExtractor'.format(self._raster.uid)

    @property
    def alive(self):
        return self._alive

    # ******************************************************************************************* **
    def receive_sample_those_cache_files_to_an_array(self, qi, prod_idx):
        """Receive message: An array is needed soon as it requires one or more read operations.
        Please perform those reads when the cache files are ready.
        """
        msgs = []

        cache_fps = qi.prod[prod_idx].cache_fps
        available_cache_fps = self._path_of_cache_files_ready.keys() & cache_fps
        missing_cache_fps = cache_fps - available_cache_fps

        for cache_fp in available_cache_fps:
            msgs += [Msg(
                'Reader', 'sample_cache_file_to_unique_array',
                qi, prod_idx, cache_fp, self._path_of_cache_files_ready[cache_fp],
            )]
        for cache_fp in missing_cache_fps:
            self._reads_waiting_for_cache_fp[cache_fp][qi].add(prod_idx)

        return msgs

    def receive_cache_files_ready(self, path_of_cache_files_ready):
        """Receive message: A cache file is ready, you might already know it.

        Parameters:
        path_of_cache_files_ready: dict from Footprint to str
        """
        msgs = []

        new_cache_fps = path_of_cache_files_ready.keys() - self._path_of_cache_files_ready.keys()
        self._path_of_cache_files_ready.update(path_of_cache_files_ready)

        for cache_fp in new_cache_fps:
            # TODO Idea: Send a external message to the facade to expose the set of path to cache files with a mutex
            for qi, prod_idxs in self._reads_waiting_for_cache_fp[cache_fp].items():
                for prod_idx in prod_idxs:
                    msgs += [Msg(
                        'Reader', 'sample_cache_file_to_unique_array',
                        qi, prod_idx, cache_fp, self._path_of_cache_files_ready[cache_fp]
                    )]
            del self._reads_waiting_for_cache_fp[cache_fp]


        return msgs

    def receive_sampled_a_cache_file_to_the_array(self, qi, prod_idx, cache_fp, array):
        """Receive message: A cache file was read for that output array"""
        return [Msg(
            'Producer', 'sampled_a_cache_file_to_the_array', qi, prod_idx, cache_fp, array,
        )]

    def receive_cancel_this_query(self, qi):
        """Receive message: One query was dropped

        Parameters
        ----------
        qi: _actors.cached.query_infos.QueryInfos
        """
        # Perform fine grain garbage collection
        for cache_fp in self._reads_waiting_for_cache_fp.keys() & qi.list_of_cache_fp:
            if qi in self._reads_waiting_for_cache_fp[cache_fp]:
                del self._reads_waiting_for_cache_fp[cache_fp][qi]
                if len(self._reads_waiting_for_cache_fp[cache_fp]) == 0:
                    del self._reads_waiting_for_cache_fp[cache_fp]
        return []

    def receive_die(self):
        """Receive message: The raster was killed"""
        assert self._alive
        self._alive = False
        self._reads_waiting_for_cache_fp.clear()
        self._raster = None
        return []

    # ******************************************************************************************* **
