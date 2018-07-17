"""Private tools to normalize functions parameters"""

import numbers
import collections
import logging
import functools
import six

import numpy as np

from .helper_classes import Singleton

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

class _DeprecationPool(Singleton):
    """Singleton class designed to handle function parameter renaming"""

    def __init__(self):
        self._seen = set()

    def add_deprecated_method(self, class_obj, new_name, old_name, deprecation_version):
        key = (class_obj.__name__, new_name, old_name)

        @functools.wraps(getattr(class_obj, new_name))
        def _f(this, *args, **kwargs):
            if key not in self._seen:
                self._seen.add(key)
                logging.warning('`{}` is deprecated since v{}, use `{}` instead'.format(
                    old_name, deprecation_version, new_name,
                ))
            return getattr(this, new_name)(*args, **kwargs)

        setattr(class_obj, old_name, _f)

    def add_deprecated_property(self, class_obj, new_name, old_name, deprecation_version):

        key = (class_obj.__name__, new_name, old_name)

        @functools.wraps(getattr(class_obj, new_name).fget)
        def _f(this):
            if key not in self._seen:
                self._seen.add(key)
                logging.warning('`{}` is deprecated since v{}, use `{}` instead'.format(
                    old_name, deprecation_version, new_name,
                ))
            return getattr(this, new_name)

        setattr(class_obj, old_name, property(_f))

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
