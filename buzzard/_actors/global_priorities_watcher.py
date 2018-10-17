from typing import (
    Set, Dict, Tuple,
)
import uuid # For mypy

import numpy as np
import sortedcontainers

from buzzard._footprint import Footprint # For mypy
from buzzard._actors.priorities import Priorities
from buzzard._actors.message import Msg

class ActorGlobalPrioritiesWatcher(object):
    """Actor that takes care of memorizing priority informations between all sub-tasks in all
    ongoing queries. Everytime a priority changes all `ActorPoolWaitingRoom` are notified.
    """

    def __init__(self):
        self._alive = True
        self._pulled_count_per_query = {}
        self.db_version = 0

        self._sorted_prod_tiles_per_cache_tile = {} # type: Dict[Tuple[uuid.UUID, Footprint], sortedcontainers.SortedListWithKey]
        self._pulled_count_per_query = {} # type: Dict[CachedQueryInfos, int]
        self._cache_fp_per_query = {} # type: Dict[CachedQueryInfos, Set[Footprint]]

    @property
    def address(self):
        return '/Global/GlobalPrioritiesWatcher'

    @property
    def alive(self):
        return self._alive

    # ******************************************************************************************* **
    def receive_a_query_need_those_cache_tiles(self, raster_uid, qi, cache_fps):
        """Receive message: A query started its optional collection phase and require those cache
        tiles.

        Priorities for those cache tiles should be updated if necessary.
        """
        msgs = []

        # Data structures shortcuts ********************************************
        ds0 = self._sorted_prod_tiles_per_cache_tile
        ds2 = self._cache_fp_per_query

        # Checks ***************************************************************
        assert qi not in ds2, 'received two collection phases for that query, should be 1 or 0'

        # Insert in `ds2` ******************************************************
        ds2[qi] = cache_fps

        # Insert in `ds0` and check for prio updates ***************************
        cache_tile_updates = set()
        for cache_fp in cache_fps:
            cache_tile_key = (raster_uid, cache_fp)
            prod_tile_key = (qi, qi.dict_of_min_prod_idx_per_cache_fp[cache_fp])
            if cache_tile_key not in ds0:
                ds0[cache_tile_key] = (
                    sortedcontainers.SortedListWithKey(
                        [prod_tile_key],
                        key=lambda k: self.prio_of_prod_tile(*k)
                    )
                )
                cache_tile_updates.add(cache_tile_key)
            else:
                prev_prio = self.prio_of_prod_tile(*ds0[cache_tile_key][0])
                ds0[cache_tile_key].add(prod_tile_key)
                new_prio = self.prio_of_prod_tile(*ds0[cache_tile_key][0])
                if prev_prio != new_prio:
                    cache_tile_updates.add(cache_tile_key)

        # Emmit messages *******************************************************
        if len(cache_tile_updates) != 0:
            self.db_version += 1
            new_prio = Priorities(self, self.db_version)
            msgs += [Msg(
                '/Pool*/WaitingRoom', 'global_priorities_update',
                new_prio, frozenset(), frozenset(cache_tile_updates)
            )]

        return msgs

    def receive_cancel_this_query(self, raster_uid, qi):
        """Receive message: A query was cancelled, update

        If this query started a collection phase, those cache tiles should be updated if necessary.
        """
        cache_tile_updates = self._get_rid_of_query(raster_uid, qi)
        if cache_tile_updates:
            self.db_version += 1
            new_prio = Priorities(self, self.db_version)
            return [Msg(
                '/Pool*/WaitingRoom', 'global_priorities_update',
                new_prio, frozenset(), frozenset(cache_tile_updates)
            )]
        else:
            return []

    def receive_output_queue_update(self, raster_uid, qi, produced_count, queue_size):
        """Receive message: The output queue of a query changed in size.

        If the number of array pulled from this queue changed:
          - Update the query priority
          - If this query started a collection phase, those cache tiles should be updated if
            necessary.
        """
        msgs = []
        query_updates = set()
        cache_tile_updates = set()

        # Data structures shortcuts ********************************************
        ds0 = self._sorted_prod_tiles_per_cache_tile
        ds1 = self._pulled_count_per_query
        ds2 = self._cache_fp_per_query

        if produced_count == qi.produce_count:
            # Query is done ****************************************************
            cache_tile_updates |= self._get_rid_of_query(raster_uid, qi)

        else:
            # Query is ongoing *************************************************
            pulled_count = queue_size - produced_count

            if qi not in ds1:
                old_pulled_count = 0
            else:
                old_pulled_count = ds1[qi]

            if old_pulled_count != pulled_count:
                query_updates.add(qi)

                # If a `new_collection_phase` was received for that query
                if qi not in ds2:
                    ds1[qi] = pulled_count
                else:
                    for cache_fp in ds2[qi]:
                        cache_tile_key = (raster_uid, cache_fp)
                        prod_tile_key = (qi, qi.dict_of_min_prod_idx_per_cache_fp[cache_fp])
                        prev_min_prio = self.prio_of_prod_tile(*ds0[cache_tile_key][0])

                        # `ds0` depends of `ds1`, updating `ds1` alone would corrupt `ds0`
                        ds0[cache_tile_key].remove(prod_tile_key)
                        ds1[qi] = pulled_count
                        ds0[cache_tile_key].add(prod_tile_key)

                        new_min_prio = self.prio_of_prod_tile(*ds0[cache_tile_key][0])
                        if prev_min_prio != new_min_prio:
                            cache_tile_updates.add(cache_tile_key)

        if query_updates or cache_tile_updates:
            self.db_version += 1
            new_prio = Priorities(self, self.db_version)
            return [Msg(
                '/Pool*/WaitingRoom', 'global_priorities_update',
                new_prio, frozenset(query_updates), frozenset(cache_tile_updates)
            )]
        else:
            return []

        return msgs

    def receive_die(self):
        """Receive message: The DataSource is closing"""
        assert self._alive
        self._alive = False

        self._sorted_prod_tiles_per_cache_tile.clear()
        self._pulled_count_per_query.clear()
        self._cache_fp_per_query.clear()

        return []

    # ******************************************************************************************* **
    def prio_of_prod_tile(self, qi, prod_idx):
        # Data structures shortcuts
        ds1 = self._pulled_count_per_query

        if qi not in ds1:
            query_pulled_count = 0
        else:
            query_pulled_count = ds1[qi]

        return (
            # Priority on `produced arrays` needed soon
            prod_idx - query_pulled_count,
        )

    def prio_of_cache_tile(self, raster_uid, cache_fp):
        # Data structures shortcuts
        ds0 = self._sorted_prod_tiles_per_cache_tile

        cache_tile_key = (raster_uid, cache_fp)
        if cache_tile_key not in ds0:
            return (np.inf,)
        prod_tile_key = ds0[cache_tile_key][0]
        prio = self.prio_of_prod_tile(*prod_tile_key)
        return prio

    def _get_rid_of_query(self, raster_uid, qi):
        msgs = []
        cache_tile_updates = set()

        # Data structures shortcuts
        ds0 = self._sorted_prod_tiles_per_cache_tile
        ds1 = self._pulled_count_per_query
        ds2 = self._cache_fp_per_query

        # If a `new_collection_phase` was received for that query
        # `ds0` depends of `ds1`, deleting in `ds1` before deleting in `ds0` would corrupt `ds0`
        if qi in ds2:
            for cache_fp in ds2[qi]:
                cache_tile_key = (raster_uid, cache_fp)
                prod_tile_key = (qi, qi.dict_of_min_prod_idx_per_cache_fp[cache_fp])
                prev_min_prio = self.prio_of_prod_tile(*ds0[cache_tile_key][0])
                ds0[cache_tile_key].remove(prod_tile_key)
                if len(ds0[cache_tile_key]) == 0:
                    del ds0[cache_tile_key]
                    cache_tile_updates.add(cache_tile_key)
                else:
                    new_min_prio = self.prio_of_prod_tile(*ds0[cache_tile_key][0])
                    if prev_min_prio != new_min_prio:
                        cache_tile_updates.add(cache_tile_key)
            del ds2[qi]

        # If the output queue was pulled at least once
        if qi in ds1:
            del ds1[qi]

        return cache_tile_updates

    # ******************************************************************************************* **
