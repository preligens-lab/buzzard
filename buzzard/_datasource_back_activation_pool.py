import collections
import threading
import contextlib

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

    def activate(self, uid, allocator):
        """Make sure at least one driver object is idle or used for uid"""
        with self._ap_lock:
            if self._ap_used[uid] == 0 and uid not in self._ap_idle:
                self._ensure_one_slot()
                self._ap_idle.push_front(uid, allocator())

    def deactivate(self, uid):
        """Flush all occurrences of uid from _ap_idle. Raises an exception if uid is in _ap_used
        """
        with self._ap_lock:
            if self._ap_used[uid] > 0:
                raise ValueError('Attempting to deactivate a proxy currently used')
            self._ap_idle.pop_all_occurrences(uid)

    def used_count(self, uid=None):
        """Count how many driver objects exist for uid"""
        with self._ap_lock:
            if uid is None:
                return sum(self._ap_used.values())
            else:
                return self._ap_used[uid]

    def idle_count(self, uid=None):
        """Count how many driver objects exist for uid"""
        with self._ap_lock:
            if uid is None:
                return len(self._ap_idle)
            else:
                return self._ap_idle.count(uid)

    def active_count(self, uid=None):
        """Count how many driver objects exist for uid"""
        with self._ap_lock:
            if uid is None:
                return len(self._ap_idle) + sum(self._ap_used.values())
            else:
                return self._ap_idle.count(uid) + self._ap_used[uid]

    def acquire_driver_object(self, uid, allocator):
        """Return a context manager to acquire a driver object

        Example
        -------
        >>> with back_ds.acquire(uid) as gdal_obj:
        ...     pass
        """
        @contextlib.contextmanager
        def _acquire():
            with self._ap_lock:
                if uid in self._ap_idle:
                    obj = self._ap_idle.pop_first_occurrence(uid)
                    allocate = False
                else:
                    self._ensure_one_slot()
                    allocate = True
                self._ap_used[uid] += 1

            if allocate:
                try:
                    obj = allocator()
                except:
                    with self._ap_lock:
                        self._ap_used[uid] -= 1
                    raise

            try:
                yield obj
            finally:
                with self._ap_lock:
                    self._ap_used[uid] -= 1
                    assert self._ap_used[uid] >= 0
                    self._ap_idle.push_front(uid, obj)

        return _acquire()

    def _ensure_one_slot(self):
        total = sum(self._ap_used.values()) + len(self._ap_idle)
        assert total <= self.max_active
        if total == self.max_active:
            if len(self._ap_idle) == 0:
                raise RuntimeError(_ERR_FMT.format(
                    self.max_active,
                    len(self._ap_idle),
                    sum(self._ap_used.values()),
                ))
            self._ap_idle.pop_back()
