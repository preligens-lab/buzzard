from typing import *

import numpy as np

from buzzard._footprint import Footprint

class ActorProducer(object):
    """Actor that takes care of producing queried data."""

    def __init__(self, raster):
        self._raster = raster
        self._queries = {}

    @property
    def address(self):
        return '/Raster{}/Producer'.format(self._raster.uid)

    # ******************************************************************************************* **
    def receive_make_those_arrays(self, query_key: int, produce_fps: int,
                                  band_ids: Sequence[int],
                                  dst_nodata: Union[float, int],
                                  interpolation: Union[None, str],
                                  max_queue_size: int):
        """Receive message: Take care of that new query"""
        msgs = []

        # Save the query details ***********************************************
        query_infos = QueryInfos(self._raster, produce_fps, band_ids, dst_nodata, interpolation,
                                 max_queue_size)
        q = _Query(query_key, query_infos)
        self._queries[query_key] = q

        # Order the creation of output arrays **********************************
        msgs += [Msg(
            'BuilderBedroom', 'build_those_arrays_when_needed_soon',
            query_key, query_infos,
        )]

        # Ask the CacheHandler to make cache tiles available *******************
        if len(cache_fps) != 0:
            msgs += [
                Msg('CacheHandler', 'may_i_read_those_cache_tiles', query_key, cache_fps)
            ]

        return msgs

    def receive_you_may_read_this_subset_of_cache_tiles(self, query_key: int,
                                                        cache_fps: Set[Footprint]):
        msgs = []
        q = self._queries[query_key]
        for cache_fp in cache_fps:
            q.missing_cache_fps.remove(cache_fp)

        return [Msg(
            'BuilderBedroom', 'those_cache_tiles_are_ready',
            query_key, cache_fps
        )]

    def receive_built_this_array(self, query_key: int, produce_id: int, array: np.ndarray):
        q = self._queries[query_key]
        assert produce_id not in q.built
        q.built.add(produce_id)
        if len(q.built) == q.infos.produce_count:
            del self._queries[query_key]
        return [
            Msg('QueriesHandler', 'made_this_array', query_key, produce_id, array)
        ]

    def receive_kill_this_query(self, query_key: int):
        del self._queries[query_key]

    def receive_die(self):
        self._queries.clear()

    # ******************************************************************************************* **

class _Query(object):
    def __init__(self, query_key, infos):
        self.infos = infos
        self.built = set()
        self.missing_cache_fps = set(infos.list_of_cache_fp)
