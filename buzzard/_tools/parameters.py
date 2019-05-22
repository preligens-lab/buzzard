"""Private tools to normalize functions parameters"""

import collections
import logging
import functools
import numbers

import six
import numpy as np

from .helper_classes import Singleton
from . import conv

Footprint, AAsyncRaster = None, None # Lazy import

CHANNELS_SCHEMA_PARAMS = frozenset({
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
    if isinstance(val, str) or not isinstance(val, collections.Iterable): # pragma: no cover
        raise TypeError(
            'Expecting a `{}` or an `sequence of `{}`, found a `{}`'.format(name, name, type(val))
        )
    yield False
    for elt in val:
        attempt = clean_fn(elt)
        if attempt is None: # pragma: no cover
            fmt = 'Expecting a `{}` or an `sequence of `{}`, found an `sequence of {}`'
            raise TypeError(fmt.format(name, name, type(elt)))
        yield attempt

def normalize_fields_parameter(fields, index_of_field_name):
    count = len(index_of_field_name)

    def _normalize_value(val):
        if np.all(np.isreal(val)) and np.shape(val) == ():
            val = int(val)
            if not (val == -1 or 0 <= val < count): # pragma: no cover
                raise ValueError('field index should be -1 or between 0 and {}'.format(count - 1))
            return val
        elif isinstance(val, str): # pragma: no cover
            if val not in index_of_field_name:
                raise ValueError('Unknown field name')
            return val
        return None

    def _index_generator(clean_indices_generator):
        for index in clean_indices_generator:
            if isinstance(index, str):
                yield index_of_field_name[index]
            elif isinstance(index, int):
                if index == -1:
                    for i in range(len(index_of_field_name)):
                        yield i
                else:
                    yield index
            else: # pragma: no cover
                assert False, index

    if fields is None:
        return [], True
    if isinstance(fields, str):
        fields = [
            f
            for f in fields.replace(' ', ',').split(',')
            if f
        ]
    gen = _coro_parameter_0or1dim(fields, _normalize_value, 'field index')
    is_flat = next(gen)
    indices = list(_index_generator(gen))
    if len(indices) > 0:
        is_flat = False
    return indices, is_flat

def normalize_channels_parameter(channels, channel_count):
    if channels is None:
        if channel_count == 1:
            return [0], True
        else:
            return list(range(channel_count)), False

    indices = np.arange(channel_count)
    indices = indices[channels]
    indices = np.atleast_1d(indices)

    if isinstance(channels, slice):
        return indices.tolist(), False

    channels = np.asarray(channels)
    if not np.issubdtype(channels.dtype, np.number):
        raise TypeError('`channels` should be None or int or slice or list of int')
    if channels.ndim == 0:
        assert len(indices) == 1
        return indices.tolist(), True
    return indices.tolist(), False

def sanitize_channels_schema(channels_schema, channel_count):
    """Used on file/recipe creation"""
    ret = {}

    def _test_length(val, name):
        count = len(val)
        if count > channel_count: # pragma: no cover
            raise ValueError('Too many values provided for %s (%d instead of %d)' % (
                name, count, channel_count
            ))
        elif count < channel_count: # pragma: no cover
            raise ValueError('Not enough values provided for %s (%d instead of %d)' % (
                name, count, channel_count
            ))

    if channels_schema is None:
        return {}
    diff = set(channels_schema.keys()) - CHANNELS_SCHEMA_PARAMS
    if diff: # pragma: no cover
        raise ValueError('Unknown channels_schema keys `%s`' % diff)

    def _normalize_multi_layer(name, val, is_type, cleaner, default):
        if val is None:
            for _ in range(channel_count):
                yield default
        elif is_type(val):
            val = cleaner(val)
            for _ in range(channel_count):
                yield val
        else:
            _test_length(val, name)
            for elt in val:
                if elt is None:
                    yield default
                elif is_type(elt):
                    yield cleaner(elt)
                else: # pragma: no cover
                    raise ValueError('`{}` cannot use value `{}`'.format(name, elt))

    if 'nodata' in channels_schema:
        ret['nodata'] = list(_normalize_multi_layer(
            'nodata',
            channels_schema['nodata'],
            lambda x: np.all(np.isreal(x)) and np.shape(x) == (),
            lambda x: float(np.asscalar(np.asarray(x))),
            None,
        ))

    if 'interpretation' in channels_schema:
        val = channels_schema['interpretation']
        if isinstance(val, str):
            ret['interpretation'] = [conv.gci_of_str(val)] * channel_count
        else:
            _test_length(val, 'nodata')
            ret['interpretation'] = [conv.gci_of_str(elt) for elt in val]
        ret['interpretation'] = [conv.str_of_gci(v) for v in ret['interpretation']]

    if 'offset' in channels_schema:
        ret['offset'] = list(_normalize_multi_layer(
            'offset',
            channels_schema['offset'],
            lambda x: np.all(np.isreal(x)) and np.shape(x) == (),
            lambda x: float(np.asscalar(np.asarray(x))),
            0.,
        ))

    if 'scale' in channels_schema:
        ret['scale'] = list(_normalize_multi_layer(
            'scale',
            channels_schema['scale'],
            lambda x: np.all(np.isreal(x)) and np.shape(x) == (),
            lambda x: float(np.asscalar(np.asarray(x))),
            1.,
        ))

    if 'mask' in channels_schema:
        val = channels_schema['mask']
        if isinstance(val, str):
            ret['mask'] = [conv.gmf_of_str(val)] * channel_count
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

    def wrap_class(self, cls, old_name, deprecation_version):
        key = (cls, old_name)
        @functools.wraps(cls.__init__)
        def _f(*args, **kwargs):
            if key not in self._seen:
                self._seen.add(key)
                logging.warning('`{}` is deprecated since v{}, use `{}` instead'.format(
                    old_name, deprecation_version, cls.__name__,
                ))
            return cls.__init__(*args, **kwargs)
        return type(old_name, (cls,), {'__init__': _f})

    def handle_param_renaming_with_kwargs(self, new_name, old_names, context,
                                          new_name_value, new_name_is_provided, user_kwargs,
                                          transform_old=lambda x:x):
        """Look for errors with a particular parameter in an invocation

        Exemple
        -------
        >>> def fn(newname='default', **kwargs):
        ...     newname, kwargs = deprecation_pool.handle_param_renaming_with_kwargs(
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
            logging.warning(
                '`{}` parameter in `{}` is deprecated since v{}, use `{}` instead'.format(
                    n, context, old_names[n], new_name,
                )
            )
        v = transform_old(user_kwargs[n])
        del user_kwargs[n]
        return v, user_kwargs

    def handle_param_removal_with_kwargs(self, old_names, context, user_kwargs):
        deprecated_names_used = six.viewkeys(old_names) & six.viewkeys(user_kwargs)
        if len(deprecated_names_used) == 0:
            return user_kwargs
        n = deprecated_names_used.pop()
        key = (context, n)
        if key not in self._seen:
            self._seen.add(key)
            logging.warning('`{}` parameter in `{}` was removed in v{}'.format(
                n, context, old_names[n],
            ))
        del user_kwargs[n]
        return user_kwargs

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
def parse_queue_data_parameters(context, raster, channels=None, dst_nodata=None,
                                interpolation='cv_area', max_queue_size=5, **kwargs):
    """Check and transform the last parameters of a `queue_data` method.
    Default values are duplicated in the .queue_data and .iter_data methods
    """

    def _band_to_channels(val):
        val = np.asarray(val)
        if np.array_equal(val, -1):
            return None
        if val.ndim == 0:
            return val - 1
        if val.ndim != 1:
            raise ValueError('Error in deprecated `band` parameter')
        val = [
            v
            for v in val
            for v in (range(len(self)) if v == -1 else [v - 1])
        ]
        return val
    channels, kwargs = deprecation_pool.handle_param_renaming_with_kwargs(
        new_name='channels', old_names={'band': '0.6.0'}, context='Raster.{}'.format(context),
        new_name_value=channels,
        new_name_is_provided=channels is not None,
        user_kwargs=kwargs,
        transform_old=_band_to_channels,
    )
    if kwargs: # pragma: no cover
        raise TypeError("{}() got an unexpected keyword argument '{}'".format(
            context, list(kwargs.keys())[0]
        ))

    # Normalize and check channels parameter
    channel_ids, is_flat = normalize_channels_parameter(channels, len(raster))
    del channels

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
        channel_ids=channel_ids,
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

    kwargs = parse_queue_data_parameters('create_raster_recipe', met.__self__, **kwargs)
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
