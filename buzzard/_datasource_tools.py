""">>> help(DataSourceToolsMixin)"""

import collections

import numpy as np

_ERR_FMT = 'DataSource is configured for a maximum of {} simultaneous activated sources \
but already {} proxies are requesting to stay activated. (You might be iterating on many \
vector files at the same time)'

class DataSourceToolsMixin(object):
    """Private mixin for the DataSource class containing subroutines for proxies:
    - registration
    - activation
    """

    def __init__(self, max_activated):
        self._keys_of_proxy = {}
        self._proxy_of_key = {}
        self._max_activated = max_activated
        self._activation_queue = collections.OrderedDict()
        self._locked_activations = collections.Counter()

    def _validate_key(self, key):
        hash(key)
        if key in self.__dict__:
            raise ValueError('key `{}` is already bound'.format(key)) # pragma: no cover

    def _register(self, keys, prox):
        self._keys_of_proxy[prox] = keys
        for key in keys:
            self._proxy_of_key[key] = prox
            self.__dict__[key] = prox

    def _unregister(self, proxy):
        for key in self._keys_of_proxy[proxy]:
            del self.__dict__[key]
            del self._proxy_of_key[key]
        del self._keys_of_proxy[proxy]

    # Activation mechanisms ********************************************************************* **
    def _assert_states_logic(self, case, proxy):
        """Assert that a proxy state is valid"""
        if case == (True, True, True):
            assert self._locked_activations[proxy] > 0
            assert proxy not in self._activation_queue
        elif case == (True, True, False):
            assert self._locked_activations[proxy] == 0
            assert proxy in self._activation_queue
        elif case == (True, False, True):
            assert self._locked_activations[proxy] > 0
            assert not self._activation_queue
        elif case == (True, False, False):
            assert self._locked_activations[proxy] == 0
            assert not self._activation_queue
        elif case == (False, True, True):
            assert False, 'lock activated but not activated' # pragma: no cover
        elif case == (False, True, False):
            assert self._locked_activations[proxy] == 0
            assert proxy not in self._activation_queue
        elif case == (False, False, True):
            assert False, 'lock activated but not activated' # pragma: no cover
        elif case == (False, False, False):
            assert self._locked_activations[proxy] == 0
            assert not self._activation_queue
        else:
            assert False, 'unreachable' # pragma: no cover

    @property
    def _queued_count(self):
        """Used for unit tests"""
        return len(self._activation_queue)

    @property
    def _locked_count(self):
        """Used for unit tests"""
        return sum(v > 0 for v in self._locked_activations.values())

    def _ensure_enough_room(self):
        """Make room in self._activation_queue if necessary"""
        if self._locked_count >= self._max_activated:
            raise RuntimeError(_ERR_FMT.format(
                self._max_activated, self._locked_count
            ))
        if self._locked_count + len(self._activation_queue) >= self._max_activated:
            other_proxy, _ = self._activation_queue.popitem(False)
            other_proxy._deactivate()

    def _register_new_activated(self, proxy):
        """Register a proxy that was created in the activated state"""
        assert proxy.activated
        if not self._max_activated != np.inf:
            return
        if not proxy.deactivable:
            return
        self._ensure_enough_room()
        self._activation_queue[proxy] = 42

    def _activate(self, proxy):
        """Activate a proxy"""
        is_activated = proxy.activated
        use_queue = self._max_activated != np.inf
        is_lock_activated = self._locked_activations[proxy] != 0
        case = (is_activated, use_queue, is_lock_activated)

        self._assert_states_logic(case, proxy)

        if case == (False, True, False):
            self._ensure_enough_room()
            proxy._activate()
            self._activation_queue[proxy] = 42
        elif case == (False, False, False):
            proxy._activate()

    def _deactivate(self, proxy):
        """Deactivate a proxy"""
        is_activated = proxy.activated
        use_queue = self._max_activated != np.inf
        is_lock_activated = self._locked_activations[proxy] != 0
        case = (is_activated, use_queue, is_lock_activated)

        self._assert_states_logic(case, proxy)

        if case == (True, True, False):
            proxy._deactivate()
            del self._activation_queue[proxy]
        elif case == (True, False, False):
            proxy._deactivate()

    def _lock_activate(self, proxy):
        """Lock this proxy in the activated state, this lock is recursive"""
        is_activated = proxy.activated
        use_queue = self._max_activated != np.inf
        is_lock_activated = self._locked_activations[proxy] != 0
        case = (is_activated, use_queue, is_lock_activated)

        self._assert_states_logic(case, proxy)

        if case == (True, True, True):
            self._locked_activations[proxy] += 1
        elif case == (True, True, False):
            del self._activation_queue[proxy]
            self._locked_activations[proxy] = 1
        elif case == (True, False, True):
            self._locked_activations[proxy] += 1
        elif case == (True, False, False):
            self._locked_activations[proxy] = 1
        elif case == (False, True, False):
            self._ensure_enough_room()
            proxy._activate()
            self._locked_activations[proxy] = 1
        elif case == (False, False, False):
            proxy._activate()
            self._locked_activations[proxy] = 1

    def _unlock_activate(self, proxy):
        """Unlock this proxy from the activated state, this lock is recursive"""
        is_activated = proxy.activated
        use_queue = self._max_activated != np.inf
        is_lock_activated = self._locked_activations[proxy] != 0
        case = (is_activated, use_queue, is_lock_activated)

        self._assert_states_logic(case, proxy)

        if case == (True, True, True):
            self._locked_activations[proxy] -= 1
            if self._locked_activations[proxy] == 0:
                self._activation_queue[proxy] = 42
        elif case == (True, True, False):
            assert False, 'unlocking but not locked' # pragma: no cover
        elif case == (True, False, True):
            self._locked_activations[proxy] -= 1
        elif case == (True, False, False): # pragma: no cover
            assert False, 'unlocking but not locked'
        elif case == (False, True, False): # pragma: no cover
            assert False, 'unlocking but not locked'
        elif case == (False, False, False): # pragma: no cover
            assert False, 'unlocking but not locked'

    def _is_locked_activate(self, proxy):
        return self._locked_activations[proxy] > 0
