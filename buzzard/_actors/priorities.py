
class Priorities(object):
    """Small class that represents a single version of the priorities among tasks. A new one is
    instantiated and broadcasted as soon as the priorities change.

    """
    __slots__ = ['_prio_actor', '_db_version']

    def __init__(self, prio_actor, db_version):
        self._prio_actor = prio_actor
        self._db_version = db_version

    def prio_of_prod_tile(self, qi, prod_idx):
        if self._prio_actor is None:
            return (prod_idx,)
        else:
            assert self._db_version == self._prio_actor.db_version, 'Failed to fetch latest priorities object'
            return self._prio_actor.prio_of_prod_tile(qi, prod_idx)

    def prio_of_cache_tile(self, raster_uid, cache_fp):
        if self._prio_actor is None:
            return (0,)
        else:
            assert self._db_version == self._prio_actor.db_version, 'Failed to fetch latest priorities object'
            return self._prio_actor.prio_of_cache_tile(raster_uid, cache_fp)

dummy_priorities = Priorities(None, None)
