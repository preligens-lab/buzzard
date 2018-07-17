""">>> help(CallOrContext)
>>> help(Singleton)
"""
import weakref

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


class DebugCtxMngmnt(object):
    def __init__(self, functions, raster):
        self._functions = functions
        self.raster = raster

    def __call__(self, string, **kwargs):
        self.string = string
        self.kwargs = kwargs
        return self

    def __enter__(self):
        if self._functions is not None:
            for function in self._functions:
                function(self.string + "::before", self.raster, **self.kwargs)

    def __exit__(self, _, __, ___):
        if self._functions is not None:
            for function in self._functions:
                function(self.string + "::after", self.raster, **self.kwargs)


class GetDataWithPrimitive(object):
    """Used to retrieve the context of a get_multi_data_queue function"""
    def __init__(self, obj, function):
        self._primitive = obj
        self._function = function

    def __call__(self, fp_iterable, band=-1, queue_size=5):
        return self._function(fp_iterable, band, queue_size)

    @property
    def primitive(self):
        """
        Returns the primitive raster
        """
        return self._primitive


class Query(object):
    """
    Query used by the Raster class
    """
    def __init__(self, q, bands, is_flat):
        self.to_produce = []
        self.produced = weakref.ref(q)

        self.to_collect = {}
        self.collected = {}

        self.to_compute = []

        self.collect_to_discard = {}
        self.compute_to_discard = set()

        self.bands = bands

        # Used in cached rasters
        self.to_check = []
        self.checking = []

        self.is_flat = is_flat

        self.was_included_in_graph = False
