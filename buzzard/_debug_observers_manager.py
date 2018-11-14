import collections

class DebugObserversManager(object):
    """Delivers the callbacks to the observers provided by user in the `debug_observers` parameters.
    """
    def __init__(self, debug_observers):
        self._obs = debug_observers
        self._to_call_per_ename = _ToCallPerEventName(debug_observers)

    def event(self, ename, *args):
        for method in self._to_call_per_ename[ename]:
            method(*args)

class _ToCallPerEventName(dict):
    def __init__(self, debug_observers):
        self._obs = debug_observers

    def __missing__(self, ename):
        method_name = 'on_{}'.format(ename)
        return [
            getattr(o, method_name)
            for o in self._obs
            if hasattr(o, method_name)
        ]
