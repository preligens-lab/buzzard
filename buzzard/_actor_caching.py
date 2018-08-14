import enum
import os
import collections

class ActorCaching(object):
    """Actor that takes care of computations caching

    Messages
    --------
    - Sends -schedule_one_cache_file_check- @ CacheFileChecker
      - will answer at -done_one_cache_file_check-
    - Sends -cache_tile_subset_can_be_read- @ Production (one or more per query)
      - is answer from -ensure_cache_tiles_can_be_read- (one per query)
    - Sends -schedule_collection- @ Computer
      - will answer at -done_one_write-
    - Receives -query_dropped- from QueryManager

    """
    def __init__(self, raster):
        self._raster = raster
        self._cache_fps_status = {
            cache_fp: _CacheTileStatus.unknown
            for cache_fp in raster.cache_fps
        }
        self._queries = {}

    # ******************************************************************************************* **
    def receive_ensure_cache_tiles_can_be_read(self, query_key, cache_fps):
        """Receive message: Ensure that those cache tiles can be read, if necessary create the
        missing ones, but launch at most one collection process. If several are missing, compute
        those in the same order as in the given list.
        """
        assert len(cache_fps) == len(set(cache_fps))

        msgs = []
        query = _Query(query_key, cache_fps)
        self._queries[query_key] = query

        for cache_fp in cache_fps:
            status = self._cache_fps_status[cache_fp]

            if status == _CacheTileStatus.ready:
                query.cache_fps_ensured.add(cache_fp)

            elif status == _CacheTileStatus.checking:
                query.cache_fps_checking.add(cache_fp)

            elif status == _CacheTileStatus.absent:
                query.cache_fps_to_compute.add(cache_fp)

            elif status == _CacheTileStatus.unknown:
                path_candidates = self._list_cache_path_candidates(cache_fp)

                if len(path_candidates) == 1:
                    # Schedule a md5hash of the cache file to compare it against its name
                    self._cache_fps_status[cache_fp] = _CacheTileStatus.checking
                    query.cache_fps_checking.add(cache_fp)
                    msgs += [
                        Msg('Raster::CacheFileChecker', 'schedule_one_cache_file_check', cache_fp, path_candidates[0])
                    ]
                else:
                    self._cache_fps_status[cache_fp] = _CacheTileStatus.absent
                    for path in path_candidates:
                        os.remove(path)
                    query.cache_fps_to_compute.add(cache_fp)
            else:
                assert False

        if len(query.cache_fps_ensured) != 0:
            # Notify the `Production` that those cache tiles are already ready
            msgs += [
                Msg('Raster::Producer', 'cache_tile_subset_can_be_read',
                    query_key, list(query.cache_fps_ensured))
            ]
        if len(query.cache_fps_checking) == 0 and len(query.cache_fps_to_compute) > 0:
            # Some tiles need to be computed and none need to be checked, launching collection right
            # now
            msgs += self._query_collection_ready(query)
        if query.is_done:
            # All cache tiles of query are now ensured to be ready
            del self._queries[query_key]
        return msgs

    def receive_done_one_cache_file_check(self, cache_fp, path, result):
        """Receive message: One cache tile was hashed and compared against its file name"""
        msgs = []

        # assertions
        assert self._cache_fps_status[cache_fp] == _CacheTileStatus.checking
        for query in self._queries.values():
            assert cache_fp not in query.cache_fps_ensured
            assert cache_fp not in query.cache_fps_to_compute

        if result:
            # This cache tile is OK to be read
            # - notify all waiting `Productions`
            # - launch collections if necessary
            self._cache_fps_status[cache_fp] = _CacheTileStatus.ready
            for query_key, query in self._queries.items():
                if cache_fp in query.cache_fps_checking:
                    query.cache_fps_checking.remove(cache_fp)
                    query.cache_fps_ensured.add(cache_fp)
                    msgs += [
                        Msg('Raster::Producer', 'cache_tile_subset_can_be_read',
                            query_key, [cache_fp])
                    ]
                    if len(query.cache_fps_checking) == 0 and len(query.cache_fps_to_compute) > 0:
                        msgs += self._query_collection_ready(query)

        else:
            # This cache tile was corrupted and removed
            # - launch collections if necessary
            self._cache_fps_status[cache_fp] = _CacheTileStatus.absent

            for query in self._queries.values():
                if cache_fp in query.cache_fps_checking:
                    query.cache_fps_checking.remove(cache_fp)
                    query.cache_fps_to_compute.add(cache_fp)
                    if len(query.cache_fps_checking) == 0 and len(query.cache_fps_to_compute) > 0:
                        msgs += self._query_collection_ready(query)

        for query_key in [k for k, v in self._queries.items() if v.is_done]:
            # All cache tiles of query are now ensured to be ready
            del self._queries[query_key]

        return msgs

    def receive_done_one_write(self, cache_fp):
        """Receive message: One cache tile was just written"""
        msgs = []
        assert self._cache_fps_status[cache_fp] == _CacheTileStatus.absent
        for query_key, query in self._queries.items():
            assert cache_fp not in query.cache_fps_ensured
            assert cache_fp not in query.cache_fps_checking

            if cache_fp in query.cache_fps_to_compute:
                query.cache_fps_to_compute.remove(cache_fp)
                query.cache_fps_ensured.add(cache_fp)
                msgs += [
                    Msg('Raster::Producer', 'cache_tile_subset_can_be_read',
                        query_key, [cache_fp])
                ]

        for query_key in [k for k, v in self._queries.items() if v.is_done]:
            # All cache tiles of query are now ensured to be ready
            del self._queries[query_key]

        return msgs

    def receive_query_dropped(self, query_key):
        """Receive message: One query was dropped"""
        if query_key in self._queries:
            del self._queries[query_key]

    # ******************************************************************************************* **
    def _query_collection_ready(self, query):
        assert len(query.cache_fps_checking) == 0
        assert len(query.cache_fps_to_compute) > 0

        cache_fps = [
            cache_fp
            for cache_fp in cache_fps
            if cache_fp in query.cache_fps_to_compute
        ]
        compute_fps = collections.OrderedDict()
        for cache_fp in cache_fps:
            for compute_fp in self.raster.compute_fps_of_cache_fp(cache_fp):
                if compute_fp not in compute_fps:
                    compute_fps[compute_fp] = 42
        return [Msg('Raster::Computer', 'schedule_collection',
                    query.query_key, list(compute_fps.keys()))]


    # ******************************************************************************************* **

class _CacheTileStatus(enum.Enum):
    unknown = 0
    checking = 1
    absent = 2
    ready = 3

class _Query(object):
    def __init__(self, query_key, cache_fps):
        self.query_key = query_key
        self.cache_fps = tuple(cache_fps)
        self.cache_fps_checking = set()
        self.cache_fps_ensured = set()
        self.cache_fps_to_compute = set()

    @property
    def is_done(self):
        a = len(self.cache_fps)
        b = len(self.cache_fps_checking)
        c = len(self.cache_fps_ensured)
        d = len(self.cache_fps_to_compute)

        assert b + c + d == a
        return b + d == 0
