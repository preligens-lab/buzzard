""">>> help(CallOrContext)
>>> help(Singleton)
"""

class CallOrContext(object):
    """Private helper class to provide a common behaviour both on call and on exit"""
    def __init__(self, obj, routine):
        self._obj = obj
        self._routine = routine

    def __call__(self):
        self._routine()

    def __enter__(self):
        return self._obj

    def __exit__(self, *args, **kwargs):
        self._routine()

class _Singleton(type):
    """ A metaclass that creates a Singleton base class when called.
    works in Python 2 & 3
    https://stackoverflow.com/questions/6760685/creating-a-singleton-in-python
    """
    _instances = {}
    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            cls._instances[cls] = super(_Singleton, cls).__call__(*args, **kwargs)
        return cls._instances[cls]

class Singleton(_Singleton('SingletonMeta', (object,), {})):
    pass

class _Any(object):
    """Helper for pattern matching"""
    def __eq__(self, _):
        return True

ANY = _Any()
