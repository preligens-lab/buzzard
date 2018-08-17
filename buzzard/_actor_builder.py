from typing import *

import numpy as np

from buzzard._footprint import Footprint

class ActorBuilder(object):
    """Actor that schedule cache tiles reading and resamplings.
    Readings are scheduled as soon as cache tiles are ready
    """

    def __init__(self, raster):
        self._raster = raster
        self._queries = {}

    @property
    def address(self):
        return '/Raster{}/Builder'.format(self._raster.uid)

    # ******************************************************************************************* **
    def receive_build_this_array(self, query_key: int, query_infos, produce_id: int):
        """Receive message: Start building that array"""
        if query_key not in self._queries:
            self._queries[query_key] = _Query(
                query_infos,
            )
        q = self._queries[query_key]
        assert produce_id not in q.set_of_prod_id_producing
        assert produce_id not in q.set_of_prod_id_produced
        q.set_of_prod_id_producing.add(produce_id)


    def receive_those_cache_tiles_are_ready(self, query_key: int, cache_fps: Set[Footprint]):
        """Receive message: Those cache tiles are ready for that query

        Store the information
        """
        q = self._queries[query_key]
        assert q.set_of_cache_fp_ready.isdisjoint(cache_fps)
        q.set_of_cache_fp_ready.update(cache_fps)

        for cache_fp in cache_fps:
            produce_ids = q.infos.dict_of_cache_prod_ids[cache_fp]
            assert produce_ids.isdisjoint(q.set_of_prod_id_produced)

            produce_ids &= self.set_of_prod_id_producing
            for produce_id in produce_ids:
                pass

    # ******************************************************************************************* **

class _Produce(object):

    def __init__(self):
        self.missing_read_fps = set()

class _Query(object):

    def __init__(self, infos):
        self.infos = infos
        self.set_of_cache_fp_ready = set()
        self.set_of_prod_id_producing = set()
        self.set_of_prod_id_produced = set()
