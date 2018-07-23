import collections
import threading

import numpy as np

from buzzard._tools import MultiOrderedDict

_ERR_FMT = 'DataSource is configured for a maximum of {} simultaneous active driver objects \
but there are already {} idle objects and {} used objects'

class BackDataSourceActivationPoolMixin(object):
    """Private mixin for the DataSource class containing subroutines for proxies' driver
    objects pooling"""

    def __init__(self, max_active, **kwargs):
        self.max_active = max_active
        self._ap_lock = threading.Lock()
        self._ap_idle = MultiOrderedDict()
        self._ap_used = collections.Counter()
        super(BackDataSourceActivationPoolMixin, self).__init__(**kwargs)

    def activate(self, uuid, allocator):
        """Make sure at least one driver object is idle or used for uuid"""
        with self._ap_lock:
            if self._ap_used[uuid] == 0 and uuid not in self._ap_idle:
                self._ensure_one_slot()
                self._ap_idle.push_front(uuid, allocator())

    def deactivate(self, uuid):
        """Flush all occurrences of uuid from _ap_idle. Raises an exception if uuid is in _ap_used
        """
        with self._ap_lock:
            if self._ap_used[uuid] > 0:
                raise ValueError('Attempting to deactivate a proxy currently used')
            self._ap_idle.pop_all_occurrences(uuid)

    def active_count(self, uuid):
        """Count how many driver objects exist for uuid"""
        with self._ap_lock:
            return self._ap_idle.count(uuid) + self._ap_used[uuid]

    def acquire_driver_object(self, uuid, allocator):
        """Return a context manager to acquire a driver object

        Example
        -------
        >>> with back_ds.acquire(uuid) as gdal_obj:
        ...     pass
        """
        @contextlib.contextmanager
        def _acquire():
            with self._ap_lock:
                if uuid in self._ap_idle:
                    obj = self._ap_idle.pop_front_occurrence(uuid)
                    allocate = False
                else:
                    self._ensure_one_slot()
                    allocate = True
                self._ap_used[uuid] += 1

            if allocate:
                try:
                    obj = allocator()
                except:
                    self._ap_used[uuid] -= 1
                    raise

            yield obj

            with self._ap_lock:
                self._ap_used[uuid] -= 1
                assert self._ap_used[uuid] >= 0
                self._ap_idle.push_front(uuid, obj)

        return _acquire()

    def _ensure_one_slot(self):
        total = sum(self._ap_used.values()) + len(self._ap_idle)
        assert total <= self.max_active
        if total == self.max_active:
            if len(self._ap_idle) == 0:
                raise RuntimeError(_ERR_FMT.format(
                    self._max_active,
                    len(self._ap_idle),
                    sum(self._ap_used.values()),
                ))
            self._ap_idle.pop_back()
