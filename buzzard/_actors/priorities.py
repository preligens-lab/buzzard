
class Priorities(object):
    """Small class that represent a single version of the priorities among tasks. A new one is
    instanciated and broadcasted as soon as the priorities changes.

    """
    __slots__ = ['_prio_actor', '_db_version']

    def __init__(self, prio_actor, db_version):
        self._prio_actor = prio_actor
        self._db_version = db_version

    def prio_of_prod_tile(qi, prod_idx):
        assert self._db_version == self._prio_actor.db_version, 'Failed to fetch latest priorities object'
        return self._prio_actor.prio_of_prod_tile(raster_uid, cache_fp)

    def prio_of_cache_tile(raster_uid, cache_fp):
        assert self._db_version == self._prio_actor.db_version, 'Failed to fetch latest priorities object'
        return self._prio_actor.prio_of_cache_tile(raster_uid, cache_fp)
