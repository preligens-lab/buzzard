import enum
import os
import collections

from buzzard._actors.message import Msg
from buzzard._actors.cached.query_infos import CacheComputationInfos

class ActorCacheSupervisor(object):
    """Actor that takes care of tracking, checking and schedule computation of cache files"""

    def __init__(self, raster):
        """
        Parameter
        ---------
        raster: _a_recipe_raster.ABackRecipeRaster
        """
        self._raster = raster
        self._cache_fps_status = {
            cache_fp: _CacheTileStatus.unknown
            for cache_fp in raster.cache_fps
        }
        self._path_of_cache_fp = {}
        self._queries = {}

    # ******************************************************************************************* **
    def receive_make_those_cache_files_available(self, qi):
        """Receive message: Ensure that the cache files for this query can be read, if necessary
        create the missing ones, but launch at most one collection process. If several are missing,
        compute those in the same order as the query needs it.

        Parameters
        ----------
        qi: _actors.cached.query_infos.QueryInfos
        """
        msgs = []
        cache_fps = qi.list_of_cache_fp

        query = _Query()
        self._queries[qi] = query

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
                    self._cache_fps_status[cache_fp] = _CacheTileStatus.checking
                    query.cache_fps_checking.add(cache_fp)
                    msgs += [
                        Msg('FileChecker', 'infer_cache_file_status', cache_fp, path_candidates[0])
                    ]
                else:
                    self._cache_fps_status[cache_fp] = _CacheTileStatus.absent
                    for path in path_candidates:
                        os.remove(path)
                    query.cache_fps_to_compute.add(cache_fp)
            else:
                assert False

        if len(query.cache_fps_ensured) != 0:
            # Notify the production pipeline that those cache tiles are already ready
            msgs += [
                Msg('CacheExtractor', 'cache_files_ready', {
                    fp: self._path_of_cache_fp[fp]
                    for fp in query.cache_fps_ensured
                })
            ]
        if len(query.cache_fps_checking) == 0:
            # CacheSupervisor is now done working on this query
            del self._queries[qi]

            if len(query.cache_fps_to_compute) > 0:
                # Some tiles need to be computed and none need to be checked, launching collection right
                # now
                msgs += self._query_start_collection(qi, query)

        return msgs

    def receive_inferred_cache_file_status(self, cache_fp, path, status):
        """Receive message: One cache tile was checked

        Parameters
        ----------
        cache_fp: Footprint
        path: str
        status: bool
        """
        msgs = []

        # assertions
        assert self._cache_fps_status[cache_fp] == _CacheTileStatus.checking
        for query in self._queries.values():
            assert cache_fp not in query.cache_fps_ensured
            assert cache_fp not in query.cache_fps_to_compute

        if status:
            # This cache tile is OK to be read
            # - notify the production pipeline
            self._path_of_cache_fp[cache_fp] = path
            self._cache_fps_status[cache_fp] = _CacheTileStatus.ready
            msgs += [
                Msg('CacheExtractor', 'cache_files_ready', {cache_fp: path})
            ]
        else:
            # This cache tile was corrupted and removed
            self._cache_fps_status[cache_fp] = _CacheTileStatus.absent

        queries_treated = []
        for qi, query in self._queries.items():
            if cache_fp in query.cache_fps_checking:
                query.cache_fps_checking.remove(cache_fp)
                if status:
                    query.cache_fps_ensured.add(cache_fp)
                else:
                    query.cache_fps_to_compute.add(cache_fp)

                if len(query.cache_fps_checking) == 0:
                    # CacheSupervisor is now done working on this query
                    queries_treated.append(qi)

                    if len(query.cache_fps_to_compute) > 0:
                        # Some tiles need to be computed and none need to be checked, launching collection right
                        # now
                        msgs += self._query_start_collection(qi, query)

        for qi in queries_treated:
            del self._queries[qi]

        return msgs

    def receive_cache_file_written(self, cache_fp, path):
        """Receive message: One cache file was just written to disk

        Parameters
        ----------
        cache_fp: Footprint
        path: str
        """
        msgs = []
        assert self._cache_fps_status[cache_fp] == _CacheTileStatus.absent

        self._path_of_cache_fp[cache_fp] = path
        self._cache_fps_status[cache_fp] = _CacheTileStatus.ready
        msgs += [
            Msg('CacheExtractor', 'cache_files_ready', {cache_fp: path})
        ]
        return msgs

    def receive_query_dropped(self, qi):
        """Receive message: One query was dropped

        Parameters
        ----------
        qi: _actors.cached.query_infos.QueryInfos
        """
        if qi in self._queries:
            del self._queries[qi]

    def receive_die(self):
        """Receive message: The raster was killed"""
        assert self._alive
        self._alive = False

        self._queries.clear()

    # ******************************************************************************************* **
    def _query_start_collection(self, qi, query):
        assert len(query.cache_fps_checking) == 0
        assert len(query.cache_fps_to_compute) > 0
        cache_fps = [
            fp
            for fp in qi.list_of_cache_fp
            if fp in query.cache_fps_to_compute
        ]
        assert qi.cache_computation is None
        qi.cache_computation = CacheComputationInfos(self._raster, cache_fps)
        return [Msg('ComputationGate', 'compute_those_cache_files', qi)]

    # ******************************************************************************************* **

class _CacheTileStatus(enum.Enum):
    unknown = 0
    checking = 1
    absent = 2
    ready = 3

class _Query(object):
    def __init__(self):
        self.cache_fps_checking = set()
        self.cache_fps_ensured = set()
        self.cache_fps_to_compute = set()
