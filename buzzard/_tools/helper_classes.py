""">>> help(CallOrContext)
>>> help(Singleton)
"""

from osgeo import gdal

class GDALErrorCatcher:
    """Wrap a call to a gdal/ogr/osr function to streamline the behavior of gdal no matter the
    global states:
    - `gdal.*UseException` functions modify global states,
    - `gdal.*ErrorHandler` functions modify thread-local states.

    The function should not return `None`, an assert prevents it (it may be removed if necessary).

    Using this wrapper makes gdal errors thread-safe.

    A related update was included in `gdal>=2.3.3` (https://github.com/OSGeo/gdal/pull/1117) but
    the python bindings were not updated. To be safe, buzzard should constrain `gdal>=2.3.3`.

    Examples
    --------
    >>> gdal.UseExceptions()
    ... dr = gdal.GetDriverByName('mem')
    ... success, payload = GDALErrorCatcher(dr.Create)('', 0, 0)
    ... print(success, payload)
    False (3, 1, 'Attempt to create 0x0 dataset is illegal,sizes must be larger than zero.')

    >>> gdal.DontUseExceptions()
    ... dr = gdal.GetDriverByName('mem')
    ... success, payload = GDALErrorCatcher(dr.Create)('', 0, 0)
    ... print(success, payload)
    False (3, 1, 'Attempt to create 0x0 dataset is illegal,sizes must be larger than zero.')

    >>> dr = gdal.GetDriverByName('mem')
    ... success, payload = GDALErrorCatcher(dr.Create)('', 1, 1)
    ... print(success, payload)
    True <osgeo.gdal.Dataset; proxy of <Swig Object of type 'GDALDatasetShadow *' at 0x> >

    """
    def __init__(self, fn):
        self._fn = fn

    def __call__(self, *args, **kwargs):
        errs, res = None, None

        def error_handler(err_level, err_no, err_msg):
            nonlocal errs
            if err_level >= gdal.CE_Failure:
                errs = err_level, err_no, err_msg

        gdal.PushErrorHandler(error_handler)
        try:
            res = self._fn(*args, **kwargs)
        except Exception as e:
            if errs is None:
                # This is not a gdal error
                raise
            else:
                # This is a gdal error, and `gdal.GetUseExceptions()` is True
                # Read problems from the `errs` variable, the details stored in this exception
                # are not reliable.
                pass
        finally:
            gdal.PopErrorHandler()

        assert (errs is None) is not (res is None), (errs, res)
        if errs:
            return False, errs
        else:
            return True, res

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
