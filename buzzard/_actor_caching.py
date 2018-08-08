import enum
import os
import collections

class ActorCaching(object):

    def __init__(self, raster):
        self._raster = raster
        self._cache_fps_status = {
            cache_fp: _CacheTileStatus.unknown
            for cache_fp in raster.cache_fps
        }
        self._queries = {}

    def receive_ensure_cache_tiles_can_be_read(self, query_key, cache_fps):
        assert len(cache_fps) == len(set(cache_fps))
        msgs = []
        query = _Query(cache_fps)

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
                        Msg('Raster::CacheFileChecker', 'schedule_one_cache_file_check', cache_fp, path_candidates[0])
                    ]
                else:
                    self._cache_fps_status[cache_fp] = _CacheTileStatus.absent
                    for path in path_candidates:
                        os.remove(path)
                    query.cache_fps_to_compute.add(cache_fp)

        if len(query.cache_fps_ensured) != 0:
            msgs += [
                Msg('Production' + query_key, 'cache_tiles_can_be_read', list(query.cache_fps_ensured))
            ]
            if len(self.cache_fps_checking) == 0:
                msgs += [self._make_query_collection_message(query)]

        return msgs

    def _make_query_collection_message(self, query):
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
        return Msg('Raster::Collection', 'schedule_collection', list(compute_fps.keys()))

    def receive_done_one_cache_file_check(self, cache_fp, path, result):
        msgs = []

        assert self._cache_fps_status[cache_fp] == _CacheTileStatus.checking
        if result:
            self._cache_fps_status[cache_fp] = _CacheTileStatus.ready
        else:
            self._cache_fps_status[cache_fp] = _CacheTileStatus.absent

        for query in self._queries:
            if cache_fp in query.cache_fps_checking:
                query.cache_fps_checking.remove(cache_fp)
                if result:
                    msgs += [
                        Msg('Production' + query_key, 'cache_tiles_can_be_read', list(query.cache_fps_ensured))
                    ]
                else:
                    query.cache_fps_to_compute.add(cache_fp)

                if len(self.cache_fps_checking) == 0:
                    msgs += [self._make_query_collection_message(query)]
        return msgs

class _CacheTileStatus(enum.Enum):
    unknown = 1
    checking = 2
    absent = 3
    ready = 4

class _Query(object):
    def __init__(self, cache_fps):
        self.cache_fps = cache_fps
        self.cache_fps_checking = set()
        self.cache_fps_ensured = set()
        self.cache_fps_to_compute = set()
