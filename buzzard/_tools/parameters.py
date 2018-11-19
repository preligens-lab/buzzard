"""Private tools to normalize functions parameters"""

import numbers
import collections
import logging
import functools
import numbers

import six
import numpy as np

from .helper_classes import Singleton
from . import conv

Footprint, AAsyncRaster = None, None # Lazy import

BAND_SCHEMA_PARAMS = frozenset({
    'nodata', 'interpretation', 'offset', 'scale', 'mask',
})

def _coro_parameter_0or1dim(val, clean_fn, name):
    """Normalize a parameter that can be an sequence or not.

    Parameters
    ----------
    val: User input
    clean_fn: Function to clean a value
    name: Name given to the values for exceptions

    Yield values
    ------------
    First value: bool
        Is flat
    """

    attempt = clean_fn(val)
    if attempt is not None:
        yield True
        yield attempt
        return
    if isinstance(val, str) or not isinstance(val, collections.Iterable):
        raise TypeError(
            'Expecting a `{}` or an `sequence of `{}`, found a `{}`'.format(name, name, type(val))
        )
    yield False
    for elt in val:
        attempt = clean_fn(elt)
        if attempt is None:
            fmt = 'Expecting a `{}` or an `sequence of `{}`, found an `sequence of {}`'
            raise TypeError(fmt.format(name, name, type(elt)))
        yield attempt

def normalize_band_parameter(band, band_count, shared_mask_index):
    """
    band: int or complex or iterator over int/complex
        if int == -1: All bands
        if int >= 1: Single band index
        if complex == -1j: All bands' mask
        if complex == 0j: Shared mask band
        if complex >= 1j: Single band index (return mask)
        if iterator: Any combination of the above numbers
    """

    def _normalize_value(nbr):
        if isinstance(nbr, numbers.Integral):
            nbr = int(nbr)
            if not (nbr == -1 or 1 <= nbr <= band_count):
                raise ValueError('band index should be -1 or between 1 and {}'.format(band_count))
            return nbr
        elif np.iscomplexobj(nbr) and not isinstance(nbr, collections.Iterable):
            nbr = complex(nbr)
            if nbr.real != 0:
                raise ValueError('imaginary band index should have a real part equal to 0')
            if not -1 <= nbr.imag <= band_count:
                raise ValueError('band index should be between 1 and {}'.format(band_count))
            if not nbr.imag.is_integer():
                raise ValueError('band index should be a whole number')
            return nbr
        elif isinstance(nbr, numbers.Real):
            nbr = float(nbr)
            if nbr == -1.:
                return -1
            if not 0 < nbr <= band_count:
                raise ValueError('band index should be between 1 and {}'.format(band_count))
            if not nbr.is_integer():
                raise ValueError('band index should be a whole number')
            return int(nbr)
        return None

    def _index_generator(clean_indices_generator):
        for index in clean_indices_generator:
            if not isinstance(index, int):
                index = index.imag
                if index == -1:
                    for i in range(1, band_count + 1):
                        yield i * 1j + 0
                elif index == -0j:
                    yield shared_mask_index
                else:
                    yield index * 1j + 0
            else:
                if index == -1:
                    for i in range(1, band_count + 1):
                        yield i
                else:
                    yield index

    gen = _coro_parameter_0or1dim(band, _normalize_value, 'band index')
    is_flat = next(gen)
    indices = list(_index_generator(gen))
    if len(indices) == 0:
        raise ValueError('Empty list of band index')
    elif len(indices) > 1:
        is_flat = False
    return indices, is_flat

def sanitize_band_schema(band_schema, band_count):
    """Used on file/recipe creation"""
    ret = {}

    def _test_length(val, name):
        count = len(val)
        if count > band_count: # pragma: no cover
            raise ValueError('Too many values provided for %s (%d instead of %d)' % (
                name, count, band_count
            ))
        elif count < band_count: # pragma: no cover
            raise ValueError('Not enough values provided for %s (%d instead of %d)' % (
                name, count, band_count
            ))

    if band_schema is None:
        return {}
    diff = set(band_schema.keys()) - BAND_SCHEMA_PARAMS
    if diff: # pragma: no cover
        raise ValueError('Unknown band_schema keys `%s`' % diff)

    def _normalize_multi_layer(name, val, type_, cleaner, default):
        if val is None:
            for _ in range(band_count):
                yield default
        elif isinstance(val, type_):
            val = cleaner(val)
            for _ in range(band_count):
                yield val
        else:
            _test_length(val, name)
            for elt in val:
                if elt is None:
                    yield default
                elif isinstance(elt, type_):
                    yield cleaner(elt)
                else: # pragma: no cover
                    raise ValueError('`{}` cannot use value `{}`'.format(name, elt))

    if 'nodata' in band_schema:
        ret['nodata'] = list(_normalize_multi_layer(
            'nodata',
            band_schema['nodata'],
            numbers.Number,
            lambda val: float(val),
            None,
        ))

    if 'interpretation' in band_schema:
        val = band_schema['interpretation']
        if isinstance(val, str):
            ret['interpretation'] = [conv.gci_of_str(val)] * band_count
        else:
            _test_length(val, 'nodata')
            ret['interpretation'] = [conv.gci_of_str(elt) for elt in val]
        ret['interpretation'] = [conv.str_of_gci(v) for v in ret['interpretation']]

    if 'offset' in band_schema:
        ret['offset'] = list(_normalize_multi_layer(
            'offset',
            band_schema['offset'],
            numbers.Number,
            lambda val: float(val),
            0.,
        ))

    if 'scale' in band_schema:
        ret['scale'] = list(_normalize_multi_layer(
            'scale',
            band_schema['scale'],
            numbers.Number,
            lambda val: float(val),
            1.,
        ))

    if 'mask' in band_schema:
        val = band_schema['mask']
        if isinstance(val, str):
            ret['mask'] = [conv.gmf_of_str(val)] * band_count
        else:
            _test_length(val, 'mask')
            ret['mask'] = [conv.gmf_of_str(elt) for elt in val]
            shared_bit = conv.gmf_of_str('per_dataset')
            shared = [elt for elt in ret['mask'] if elt & shared_bit]
            if len(set(shared)) > 1: # pragma: no cover
                raise ValueError('per_dataset mask must be shared with same flags')
        ret['mask'] = [conv.str_of_gmf(v) for v in ret['mask']]


    ret = {
        k: tuple(v)
        for k, v in ret.items()
    }
    return ret


class _DeprecationPool(Singleton):
    """Singleton class designed to handle function parameter renaming"""

    def __init__(self):
        self._seen = set()

    class _MethodWrapper(object):
        """Descriptor object to manage deprecation"""
        def __init__(self, method, deprecation_version, seen):
            self._method = method
            self._deprecation_version = deprecation_version
            self._seen = seen
            self._old_name = None
            self._key = None

        def __get__(self, instance, owner):
            assert self._key is not None
            if self._key not in self._seen:
                self._seen.add(self._key)
                logging.warning('`{}` is deprecated since v{}, use `{}` instead'.format(
                    self._old_name, self._deprecation_version, self._method.__name__,
                ))
            return self._method.__get__(instance, owner)

        def __set_name__(self, owner, name):
            self._old_name = name
            self._key = (self._method, name)

    class _PropertyWrapper(object):
        """Descriptor object to manage deprecation"""
        def __init__(self, new_property_name, deprecation_version, seen):
            self._new_property_name = new_property_name
            self._deprecation_version = deprecation_version
            self._seen = seen
            self._old_name = None
            self._key = None

        def __get__(self, instance, owner):
            assert self._key is not None
            if self._key not in self._seen:
                self._seen.add(self._key)
                logging.warning('`{}` is deprecated since v{}, use `{}` instead'.format(
                    self._old_name, self._deprecation_version, self._new_property_name,
                ))
            return getattr(instance, self._new_property_name)

        def __set_name__(self, owner, name):
            self._old_name = name
            self._key = (owner, self._new_property_name, name)


    def wrap_method(self, method, deprecation_version):
        return self._MethodWrapper(method, deprecation_version, self._seen)

    def wrap_property(self, new_property, deprecation_version):
        return self._PropertyWrapper(new_property, deprecation_version, self._seen)

    def streamline_with_kwargs(self, new_name, old_names, context,
                               new_name_value, new_name_is_provided, user_kwargs):
        """Look for errors with a particular parameter in an invocation

        Exemple
        -------
        >>> def fn(newname='default', **kwargs):
        ...     newname, kwargs = deprecation_pool.streamline_with_kwargs(
        ...         new_name='newname', old_names={'oldname': '0.2.3'},
        ...         new_name_value=newname, context='the fn function',
        ...         new_name_is_provided=newname != 'default',
        ...         user_kwargs=kwargs,
        ...     )
        ...     return newname

        >>> fn() # Nothing happens
        'default'

        >>> fn(newname='default') # Nothing happens
        'default'

        >>> fn(oldname='aha') # A warning is issued the first time
        WARNING:root:`oldname` is deprecated since v0.2.3, use `newname`
        'aha'

        >>> fn(newname='default', oldname='default') # A warning is issued the first time
        WARNING:root:`oldname` is deprecated since v0.2.3, use `newname`
        'default'

        >>> fn(newname='default', oldname='aha') # A warning is issued the first time
        WARNING:root:`oldname` is deprecated since v0.2.3, use `newname`
        'aha'

        >>> fn(newname='aha', oldname='default') # doctest: +IGNORE_EXCEPTION_DETAIL
        Traceback (most recent call last):
        NameError: Using both `newname` and `oldname`, `oldname` is deprecated

        >>> fn(newname='aha', oldname='aha') # doctest: +IGNORE_EXCEPTION_DETAIL
        Traceback (most recent call last):
        NameError: Using both `newname` and `oldname`, `oldname` is deprecated

        """
        deprecated_names_used = six.viewkeys(old_names) & six.viewkeys(user_kwargs)
        if len(deprecated_names_used) == 0:
            return new_name_value, user_kwargs
        n = deprecated_names_used.pop()
        if new_name_is_provided:
            raise NameError('Using both `{}` and `{}` in `{}`, `{}` is deprecated'.format(
                new_name, n, context, n,
            ))

        key = (context, new_name, n)
        if key not in self._seen:
            self._seen.add(key)
            logging.warning('`{}` is deprecated since v{}, use `{}` instead'.format(
                n, old_names[n], new_name,
            ))
        v = user_kwargs[n]
        del user_kwargs[n]
        return v, user_kwargs

deprecation_pool = _DeprecationPool()

def normalize_fields_defn(fields):
    """Used on file creation"""
    if not isinstance(fields, collections.Iterable): # pragma: no cover
        raise TypeError('Bad fields definition type')

    def _sanitize_dict(dic):
        dic = dict(dic)
        name = dic.pop('name')
        type_ = dic.pop('type')
        precision = dic.pop('precision', None)
        width = dic.pop('width', None)
        nullable = dic.pop('nullable', None)
        default = dic.pop('default', None)
        oft = conv.oft_of_any(type_)
        if default is not None:
            default = str(conv.type_of_oftstr(conv.str_of_oft(oft))(default))
        if len(dic) != 0: # pragma: no cover
            raise ValueError('unexpected keys in {} dict: {}'.format(name, dic))
        return dict(
            name=name,
            type=oft,
            precision=precision,
            width=width,
            nullable=nullable,
            default=default,
        )
    return [_sanitize_dict(dic) for dic in fields]

# Async rasters ***************************************************************************** **
def parse_queue_data_parameters(raster, band=1, dst_nodata=None, interpolation='cv_area',
                                max_queue_size=5):
    """Check and transform the last parameters of a `queue_data` method.
    Default values are duplicated in the CachedRasterRecipe.queue_data method
    """

    # Normalize and check band parameter
    band_ids, is_flat = normalize_band_parameter(band, len(raster), raster.shared_band_id)
    del band

    # Normalize and check dst_nodata parameter
    if dst_nodata is not None:
        dst_nodata = raster.dtype.type(dst_nodata)
    elif raster.nodata is not None:
        dst_nodata = raster.nodata
    else:
        dst_nodata = raster.dtype.type(0)

    # Check interpolation parameter here
    if not (interpolation is None or interpolation in raster._back.REMAP_INTERPOLATIONS): # pragma: no cover
        raise ValueError('`interpolation` should be None or one of {}'.format(
            set(raster._back.REMAP_INTERPOLATIONS.keys())
        ))

    # Check max_queue_size
    max_queue_size = int(max_queue_size)
    if max_queue_size <= 0:
        raise ValueError('`max_queue_size` should be >0')

    return dict(
        band_ids=band_ids,
        dst_nodata=dst_nodata,
        interpolation=interpolation,
        max_queue_size=max_queue_size,
        is_flat=is_flat,
    )

def shatter_queue_data_method(met, name):
    """Check and transform a `met = queue_data_per_primitive[name]` given by user

    Parameters
    ----------
    met: object
        user's parameter
    name: hashable
        name given to primitive by user

    Returns
    -------
    (ABackAsyncRaster, dict of str->object)
    """
    global AAsyncRaster
    if AAsyncRaster is None:
        from buzzard._a_async_raster import AAsyncRaster


    # Unwrap function.partial instances ****************************************
    kwargs = {}
    while isinstance(met, functools.partial):
        if met.args:
            raise ValueError("Can't handle positional arguments in functools.partial " +
                             "of `queue_data_per_primitive` element")
        kwargs.update(met.keywords)
        met = met.func

    # Check method *************************************************************
    if not callable(met):
        raise TypeError('`queue_data_per_primitive[{}]` should be callable'.format(
            name
        ))
    if not hasattr(met, '__self__') or not isinstance(met.__self__, AAsyncRaster):
        fmt = '`queue_data_per_primitive[{}]` should be the `.queue_data` method ' +\
              'of a scheduler raster'
        raise TypeError(fmt.format(name))

    kwargs = parse_queue_data_parameters(met.__self__, **kwargs)
    return met.__self__._back, kwargs

# Tiling checks ********************************************************************************* **
def is_tiling_covering_fp(tiling, fp, allow_outer_pixels, allow_overlapping_pixels):
    """Is the `tiling` object, an output of `fp.tile` or `fp.tile.count`?

    if `allow_outer_pixels`
        Some pixels of `tiling` may be outside of `fp`
    else
        All pixels of `tiling` should be inside `fp`

    if `allow_overlapping_pixels`
        All pixels of `fp` should be covered at least once.
    else
        All pixels of `fp` should be covered exactly once.

    Parameters
    ----------
    tiling: object
        An object provided by user
    fp: Footprint
    allow_outer_pixels: bool
    allow_overlapping_pixels: bool

    Returns
    -------
    bool
    """
    global Footprint
    if Footprint is None:
        from buzzard._footprint import Footprint

    # Type checking ****************************************
    if not isinstance(tiling, np.ndarray):
        return False
    if tiling.ndim != 2:
        return False
    for tile in tiling.flat:
        if not isinstance(tile, Footprint):
            return False
        if not fp.same_grid(tile):
            return False

    # Pixel indices extraction *****************************
    rtls = np.asarray([
        fp.spatial_to_raster(tile.tl)
        for tile in tiling.flat
    ]).reshape(tiling.shape[0], tiling.shape[1], 2)
    rbrs = np.asarray([
        fp.spatial_to_raster(tile.br)
        for tile in tiling.flat
    ]).reshape(tiling.shape[0], tiling.shape[1], 2)

    # is tiling ********************************************
    # All line's tly equal
    if not np.all(rtls[:, :1, 1] == rtls[:, :, 1]):
        return False
    # All line's bry equal
    if not np.all(rbrs[:, :1, 1] == rbrs[:, :, 1]):
        return False

    # All column's tlx equal
    if not np.all(rtls[:1, :, 0] == rtls[:, :, 0]):
        return False
    # All column's brx equal
    if not np.all(rbrs[:1, :, 0] == rbrs[:, :, 0]):
        return False

    # bounds ***********************************************
    if not allow_outer_pixels:
        if not np.all(rtls[0, 0] <= 0):
            return False
        if not np.all(rbrs[-1, -1] >= fp.rsize):
            return False
    else:
        if not np.all(rtls[0, 0] == [0, 0]):
            return False
        if not np.all(rbrs[-1, -1] == fp.rsize):
            return False

    # overlap **********************************************
    if allow_overlapping_pixels:
        # All line's consecutive tlx vs brx
        if not np.all(rbrs[:, :-1, 0] >= rtls[:, 1:, 0]):
            return False
        # All columns's consecutive tly vs bry
        if not np.all(rbrs[:-1, :, 1] >= rtls[1:, :, 1]):
            return False
    else:
        # All line's consecutive tlx vs brx
        if not np.all(rbrs[:, :-1, 0] == rtls[:, 1:, 0]):
            return False
        # All columns's consecutive tly vs bry
        if not np.all(rbrs[:-1, :, 1] == rtls[1:, :, 1]):
            return False

    return True
