import functools

class ActionChecksum():
    """"""
    is_urgent = True

    def __init__(self, truc, raster, query, cache_fp):
        self._truc, self._raster, self._query = truc, raster, query
        self._cache_fp = cache_fp

    def get_worker_fn(self):
        """Function that performs md5hash on cached file"""
        return functools.partial(
            _is_checksum_valid,
        )

    def done(self, is_valid):
        """
        Should trigger reads/computations/collections
        """
        self._cache_tile_is_readable[cache_fp] = is_valid

def _is_checksum_valid():
    pass
