import multiprocessing as mp
import multiprocessing.pool

class BackDataSourcePools(object):

    def __init__(self, **kwargs):
        self._pool_cache = {}
        super().__init__(**kwargs)

    def normalize_pool_parameter(self, pool_param, param_name):
        """Check and transform a `*_pool` parameter given by user"""
        if isinstance(pool_param, (mp.pool.Pool, mp.pool.ThreadPool)):
            return pool_param
        if pool_param is None:
            return None
        if not (hasattr(pool_param, '__hash__') and hasattr(pool_param, '__eq__')):
            types = ['multiprocessing.pool.Pool',
                     'multiprocessing.pool.ThreadPool',
                     'None', 'hashable',
            ]
            raise TypeError('`{}` parameter should be one of {{{}}}'.format(
                name, ', '.join(types)
            ))
        if pool_param not in self._pool_cache:
            self._pool_cache[pool_param] = mp.pool.ThreadPool(mp.cpu_count())
            self._debug_mngr.event('object_allocated', self._pool_cache[pool_param])
        return self._pool_cache[pool_param]

    def join_all_pools(self):
        for pool in self._pool_cache.values():
            if pool is not None:
                pool.close()
        for pool in self._pool_cache.values():
            if pool is not None:
                pool.join()
        self._pool_cache.clear()
