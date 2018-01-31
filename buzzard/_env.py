""">>> help(buzz.env)
>>> help(buzz.Env)
"""

import threading
from collections import namedtuple

import cv2
from osgeo import gdal, ogr, osr
from buzzard._tools import conv

try:
    from collections import ChainMap
except:
    # https://pypi.python.org/pypi/chainmap
    from chainmap import ChainMap

# Sanitization ********************************************************************************** **
_RASTER_DRIVERS = {gdal.GetDriver(i).GetDescription() for i in range(gdal.GetDriverCount())}
def _sanitize_raster_driver(val):
    if isinstance(val, gdal.Driver):
        return val
    val = str(val)
    if val not in _RASTER_DRIVERS:
        raise ValueError('Unknown raster driver')
    return gdal.GetDriverByName(val)

_VECTOR_DRIVERS = {ogr.GetDriver(i).GetDescription() for i in range(ogr.GetDriverCount())}
def _sanitize_vector_driver(val):
    if isinstance(val, ogr.Driver):
        return val
    val = str(val)
    if val not in _RASTER_DRIVERS:
        raise ValueError('Unknown vector driver')
    return ogr.GetDriverByName(val)

_CV2_INTERPOLATIONS = [
    (cv2.INTER_NEAREST, 'nearest'),
    (cv2.INTER_LINEAR, 'linear'),
    (cv2.INTER_AREA, 'area'),
    (cv2.INTER_CUBIC, 'cubic'),
    (cv2.INTER_LANCZOS4, 'lanczos4'),
]
def _sanitize_raster_interpolation(val):
    for v, s in _CV2_INTERPOLATIONS:
        if val == v or val == s:
            return v
    raise ValueError('Unknown cv2 interpolation')

_INDEX_DTYPES = list(conv.DTYPE_OF_NAME.keys())
def _sanitize_index_dtype(val):
    val = conv.dtype_of_any_downcast(val)
    if val not in _INDEX_DTYPES:
        raise ValueError('%s cannot be used as an index dtype' % val)
    return val

def _sanitize_significant(val):
    val = float(val)
    if val <= 0:
        raise ValueError('Significant should be greater than 0')
    return val

# Set up **************************************************************************************** **
def _set_up_osgeo_use_exception(new, _):
    if new:
        gdal.UseExceptions()
        osr.UseExceptions()
        ogr.UseExceptions()
    else:
        gdal.DontUseExceptions()
        osr.DontUseExceptions()
        ogr.DontUseExceptions()

def _set_up_check_with_invert_proj(new, _):
    if new:
        gdal.SetConfigOption('CHECK_WITH_INVERT_PROJ', 'ON')
    else:
        gdal.SetConfigOption('CHECK_WITH_INVERT_PROJ', 'OFF')

def _set_up_buzz_trusted(new, _):
    conf = gdal.GetConfigOption('GDAL_VRT_PYTHON_TRUSTED_MODULES') or ''
    conf = conf.split(',')
    conf = [elt for elt in conf if elt != 'buzzard._raster_recipe']
    if new:
        conf.append('buzzard._raster_recipe')
        gdal.SetConfigOption('GDAL_VRT_PYTHON_TRUSTED_MODULES', ','.join(conf))
    else:
        gdal.SetConfigOption('GDAL_VRT_PYTHON_TRUSTED_MODULES', ','.join(conf))

# Options declaration *************************************************************************** **
_EnvOption = namedtuple('_Option', 'sanitize, set_up, bottom_value')
_OPTIONS = {
    'significant': _EnvOption(_sanitize_significant, None, 8.0),
    'default_index_dtype': _EnvOption(_sanitize_index_dtype, None, 'int32'),
    'warnings': _EnvOption(bool, None, True),
    'allow_complex_footprint': _EnvOption(bool, None, False),

    '_osgeo_use_exceptions': _EnvOption(bool, _set_up_osgeo_use_exception, gdal.GetUseExceptions()),
    '_gdal_trust_buzzard': _EnvOption(bool, _set_up_buzz_trusted, False),

    # 'check_with_invert_proj': _EnvOption(
    #     bool, _set_up_check_with_invert_proj,
    #     gdal.GetConfigOption('CHECK_WITH_INVERT_PROJ') == 'ON'
    # ),
    # 'default_raster_driver': _EnvOption(_sanitize_raster_driver, None, 'GTiff'),
    # 'default_vector_driver': _EnvOption(_sanitize_vector_driver, None, 'ESRI Shapefile'),
    # 'raster_interpolation': _EnvOption(_sanitize_raster_interpolation, None, 'area'),
}

# Storage *************************************************************************************** **
class _GlobalMapStack(object):
    """ChainMap updated to behave like a singleton stack"""

    _main_storage = None

    def __init__(self, bottom=None):
        if bottom is not None:
            self._mapping = ChainMap(bottom)
            self.__class__._main_storage = self
        else:
            # Copying _mapping to be immune from updates on the main side while thread is running,
            # is it really possible?
            self._mapping = self._main_storage._mapping.copy()

    def push(self, mapping):
        self._mapping = self._mapping.new_child(mapping)

    def remove_top(self):
        assert len(self._mapping.parents) > 1
        self._mapping = self._mapping.parents

    def __getitem__(self, k):
        return self._mapping[k]

class _Storage(threading.local):
    """Thread local storage for the GlobalMapStack instance"""
    def __init__(self):
        if threading.current_thread().__class__.__name__ == '_MainThread':
            self._mapstack = _GlobalMapStack({
                k: v.sanitize(v.bottom_value) for (k, v) in _OPTIONS.items()
            })
        else:
            self._mapstack = _GlobalMapStack()
        threading.local.__init__(self)

_LOCAL = _Storage()

# Env update ************************************************************************************ **
class Env(object):
    """Context manager to update buzzard states

    Parameters
    ----------
    significant: int
        Number of significant digits for floating point comparisons
        Initialized to `8.0`
        see: https://github.com/airware/buzzard/wiki/Precision-system
        see: https://github.com/airware/buzzard/wiki/Floating-Point-Considerations
    default_index_dtype: convertible to np.dtype
        Default numpy return dtype for array indices.
        Initialized to `np.int32` (signed to allow negative indices by default)
    allow_complex_footprint: bool
        Whether to allow non north-up / west-left Footprints
        Initialized to `False`
    warnings: bool
        Initialized to `True`

    Example
    -------
    >>> import buzzard as buzz
    >>> with buzz.Env(default_index_dtype='uint64'):
            ds = buzz.DataSource()
            dsm = ds.open_araster('dsm', 'path/to/dsm.tif')
            x, y = dsm.meshgrid_raster
            print(x.dtype)
    numpy.uint64

    """

    def __init__(self, **kwargs):
        self._mapping = {}
        for k, v in kwargs.items():
            if k not in _OPTIONS:
                raise ValueError('Unknown env key')
            v = _OPTIONS[k].sanitize(v)
            self._mapping[k] = v

    def __enter__(self):
        for k, newv in self._mapping.items():
            if _OPTIONS[k].set_up is not None:
                oldv = _LOCAL._mapstack[k]
                _OPTIONS[k].set_up(newv, oldv)
        _LOCAL._mapstack.push(self._mapping)

    def __exit__(self, exc_type=None, exc_val=None, exc_tb=None):
        _LOCAL._mapstack.remove_top()
        for k, oldv in self._mapping.items():
            if _OPTIONS[k].set_up is not None:
                newv = _LOCAL._mapstack[k]
                _OPTIONS[k].set_up(newv, oldv)

# Value retrieval ******************************************************************************* **
class _ThreadMapStackGetter(object):
    """Getter for env attribute"""
    def __init__(self, key):
        self.key = key

    def __call__(self, self2):
        return _LOCAL._mapstack[self.key]

class _CurrentEnv(object):
    """Namespace to access current values of buzzard's environment variable (see buzz.Env)

    Example
    -------
    >>> buzz.env.significant
    8.0

    """
    _instance = None

    def __new__(cls):
        if cls._instance is None:
            for k in _OPTIONS.keys():
                setattr(cls, k, property(_ThreadMapStackGetter(k)))
            cls._instance = object.__new__(cls)
            return cls._instance
        else:
            assert False

env = _CurrentEnv() # pylint: disable=invalid-name
