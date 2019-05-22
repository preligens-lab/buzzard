import threading
import collections
import multiprocessing as mp
import multiprocessing.pool

class PoolsContainer(object):
    """Manages thread/process pools and aliases for a Dataset"""

    def __init__(self):
        self._aliases_per_pool = collections.defaultdict(set)
        self._aliases = {}
        self._managed_pools = set()
        self._lock = threading.Lock()

    def alias(self, key, pool_or_none):
        """Register the given pool under the given key in this Dataset. The key can then be
        used to refer to that pool from within the async raster constructors.

        Parameters
        ----------
        key: hashable (like a string)
        pool_or_none: multiprocessing.pool.Pool or multiprocessing.pool.ThreadPool or None
        """
        with self._lock:
            if key in self._aliases: # pragma: no cover
                raise ValueError('Key `{}` is already bound to `{}`'.format(
                    key, self._aliases[key]
                ))
            self._aliases_per_pool[pool_or_none].add(key)
            self._aliases[key] = pool_or_none

    def manage(self, pool):
        """Add the given pool to the list of pools that must be terminated upon Dataset closing.
        """
        if not isinstance(pool, (mp.pool.Pool, mp.pool.ThreadPool)): # pragma: no cover
            raise TypeError('Can only manage pools')
        with self._lock:
            self._managed_pools.add(pool)

    def __len__(self):
        """Number of pools registered in this Dataset"""
        with self._lock:
            return len(
                p
                for p in self._aliases_per_pool.keys()
                if p is not None
            )

    def __iter__(self):
        """Generator of pools registered in this Dataset"""
        for p in self._aliases_per_pool.keys():
            if p is not None:
                yield p

    def __getitem__(self, key):
        """Pool or none getter from alias"""
        return self._aliases[key]

    def __contains__(self, key):
        """Is pool or alias registered in this Dataset"""
        with self._lock:
            return key in self._aliases or key in self._aliases_per_pool

    # Private interface with Dataset ********************************************************* **
    def _close(self):
        for pool in self._managed_pools:
            pool.terminate()
        for pool in self._managed_pools:
            pool.join()
        self._aliases.clear()
        self._aliases_per_pool.clear()
        self._managed_pools.clear()

    def _normalize_pool_parameter(self, pool_param, param_name):
        if isinstance(pool_param, (mp.pool.Pool, mp.pool.ThreadPool)):
            return pool_param
        if pool_param is None:
            return None
        if not (hasattr(pool_param, '__hash__') and hasattr(pool_param, '__eq__')): # pragma: no cover
            types = [
                'multiprocessing.pool.Pool',
                'multiprocessing.pool.ThreadPool',
                'None', 'hashable',
            ]
            raise TypeError('`{}` parameter should be one of {}'.format(
                param_name, ', '.join(types)
            ))
        with self._lock:
            if pool_param not in self._aliases:
                p = mp.pool.ThreadPool(mp.cpu_count())
                self._aliases[pool_param] = p
                self._aliases_per_pool[p] = [pool_param]
                self._managed_pools.add(p)
        return self._aliases[pool_param]
