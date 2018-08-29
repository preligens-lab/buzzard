
from buzzard._actors.priorities import Priorities

class ActorGlobalPrioritiesWatcher(object):
    """Actor that takes care of memorizing priority informations between all sub-tasks in all
    ongoing queries. Everytime a priority changes all `ActorPoolWaitingRoom` are notified.

    """

    def __init__(self):
        self._alive = True
        self._pulled_count_per_query = {} # type: Dict[uuid.UUID, int]
        # self._pulled_count_per_query_per_raster = {} # type: Dict[uuid.UUID, Dict[CachedQueryInfos, int]]
        self._db_version = 0

    @property
    def address(self):
        return '/GlobalPrioritiesWatcher'

    @property
    def alive(self):
        return self._alive

    # ******************************************************************************************* **
    def receive_new_collection_phase(self, raster_uid, qi, cache_fps):
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
                cache_tile_updates.add(cache_fp)
            else:
                prev_prio = self.prio_of_prod_tile(*ds0[cache_tile_key][0])
                ds0[cache_tile_key].add(prod_tile_key)
                new_prio = self.prio_of_prod_tile(*ds0[cache_tile_key][0])
                if prev_prio != new_prio:
                    cache_tile_updates.add(cache_fp)

        # Emmit messages *******************************************************
        if len(cache_tile_updates) != 0:
            self._db_version += 1
            new_prio = Priorities(self, self._db_version)
            msgs += [Msg(
                '/Pool*/WaitingRoom', 'global_priorities_update',
                new_prio, frozenset(), frozenset(cache_tile_updates)
            )]

        return msgs

    def receive_output_queue_update(self, raster_uid, qi, produced_count, queue_size):
        msgs = []

        # Data structures shortcuts ********************************************
        ds0 = self._sorted_prod_tiles_per_cache_tile
        ds1 = self._pulled_count_per_query
        ds2 = self._cache_fp_per_query

        if produced_count == qi.produce_count:
            # Query is done ****************************************************
            msgs += self._get_rid_of_query(raster_uid, uid)

        else:
            # Query is ongoing *************************************************
            pulled_count = queue_size - produced_count
            query_updates = set()
            cache_tile_updates = set()

            # Update `ds0` and look for updates
            if qi in ds2:
                for cache_fp in

            # Update `ds1` and look for updates
            if qi not in ds1:
                ds1[qi] = pulled_count
            else:
                old_pulled_count = ds1[qi]
                ds1[qi] = pulled_count
                if old_pulled_count != pulled_count:
                    query_updates.add(qi)


        return msgs

    def receive_die(self):
        assert self._alive
        self._alive = False

        self._pulled_count_per_query_per_raster.clear()
        return []

    # ******************************************************************************************* **
    def prio_of_prod_tile(qi, prod_idx):
        # Data structures shortcuts
        ds1 = self._pulled_count_per_query

        if qi not in ds1:
            query_pulled_count = 0
        else:
            query_pulled_count = ds1[qi]

        prod_fp = qi.prod[prod_idx].fp
        cx, cy = np.around(prod_fp.c).astype(int)

        return (
            # Priority on `produced arrays` needed soon
            prod_idx - query_pulled_count,

            # Priority on top-most and smallest `produced arrays`
            -cy,

            # Priority on left-most and smallest `produced arrays`
            cx,
        )

    def prio_of_cache_tile(raster_uid, cache_fp):
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

        # Data structures shortcuts
        ds0 = self._sorted_prod_tiles_per_cache_tile
        ds1 = self._pulled_count_per_query
        ds2 = self._cache_fp_per_query

        if qi in ds2:
            # If a `new_collection_phase` was received for that query
            cache_tile_updates = set()
            for cache_fp in ds2[qi]:
                cache_tile_key = (raster_uid, cache_fp)
                prod_tile_key = (qi, qi.dict_of_min_prod_idx_per_cache_fp[cache_fp])
                prev_prio = self.prio_of_prod_tile(*ds0[cache_tile_key][0])
                ds0[cache_tile_key].remove(prod_tile_key)
                if len(ds0[cache_tile_key]) == 0:
                    del ds0[cache_tile_key]
                    cache_tile_updates.add(cache_fp)
                else:
                    new_prio = self.prio_of_prod_tile(*ds0[cache_tile_key][0])
                    if prev_prio != new_prio:
                        cache_tile_updates.add(cache_fp)
            del ds2[qi]

            if len(cache_tile_updates) > 0:
                msgs += [Msg(
                    '/Pool*/WaitingRoom', 'global_priorities_update',
                    new_prio, frozenset(), frozenset(cache_tile_updates)
                )]

        if qi in ds1:
            # If an `output_queue_update` was received for that query
            del ds1[qi]


        return msgs

    # def receive_output_queue_update(self, raster_uid, qi, produced_count, queue_size):
    #     pulled_count = queue_size - produced_count
    #     old_pulled_count = self._pulled_count_per_query_per_raster[raster_uid][qi]
    #     if pulled_count != old_pulled_count:
    #         self._pulled_count_per_query_per_raster[raster_uid][qi] = pulled_count

    # def receive_cancel_this_query(self, raster_uid, qi):
    #     del self._pulled_count_per_query_per_raster[raster_uid][qi]

    # ******************************************************************************************* **
    # ******************************************************************************************* **
