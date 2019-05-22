import sys

import numpy as np

from buzzard._a_source import ASource, ABackSource
from buzzard._a_source_raster_remap import ABackSourceRasterRemapMixin
from buzzard._footprint import Footprint
from buzzard import _tools

class ASourceRaster(ASource):
    """Base abstract class defining the common behavior of all rasters.

    Features Defined
    ----------------
    - Has a `stored` Footprint that defines the location of the raster
    - Has a Footprint that is influenced by the Dataset's opening mode
    - Has a length that defines how many channels are available
    - Has a `channels_schema` that defines per channel attributes (e.g. nodata)
    - Has a `dtype` (like np.float32)
    - Has a `get_data` method that allows to read pixels in their current state to numpy arrays
    """

    @property
    def fp_stored(self):
        return self._back.fp_stored

    @property
    def fp(self):
        return self._back.fp

    @property
    def channels_schema(self):
        return dict(self._back.channels_schema)

    @property
    def dtype(self):
        return self._back.dtype

    @property
    def nodata(self):
        """Accessor for first channel's nodata value"""
        return self._back.nodata

    def get_nodata(self, channel=0):
        """Accessor for nodata value"""
        return self._back.get_nodata(channel)

    def __len__(self):
        """Return the number of channels"""
        return len(self._back)

    def get_data(self, fp=None, channels=None, dst_nodata=None, interpolation='cv_area', **kwargs):
        """Read a rectangle of data on several channels from the source raster.

        If `fp` is not fully within the source raster, the external pixels are set to nodata. If
        nodata is missing, 0 is used.
        If `fp` is not on the same grid as the source raster, remapping is performed using
        `interpolation` algorithm. (It fails if the `allow_interpolation` parameter is set to
        False in `Dataset` (default)). When remapping, the nodata values are not interpolated,
        they are correctly spread to the output.

        If `dst_nodata` is provided, nodata pixels are set to `dst_nodata`.

        The alpha channels are currently resampled like any other channels, this behavior may
        change in the future. To normalize an `rgba` array after a resampling operation, use this
        piece of code:
        >>> arr = np.where(arr[..., -1] == 255, arr, 0)

        /!\ Bands in GDAL are indexed from 1. Channels in buzzard are indexed from 0.

        Parameters
        ----------
        fp: Footprint of shape (Y, X) or None
            If None: return the full source raster
            If Footprint: return this window from the raster
        channels: None or int or slice or sequence of int (see `Channels Parameter` below)
            The channels to be read
        dst_nodata: nbr or None
            nodata value in output array
            If None and raster.nodata is not None: raster.nodata is used
            If None and raster.nodata is None: 0 is used
        interpolation: one of {'cv_area', 'cv_nearest', 'cv_linear', 'cv_cubic', 'cv_lanczos4'} or None
            OpenCV method used if intepolation is necessary

        Returns
        -------
        array: numpy.ndarray of shape (Y, X) or (Y, X, C)
            If the `channels` parameter is `None`, the returned array is of shape (Y, X) when `C=1`,
               (Y, X, C) otherwise.
            If the `channels` parameter is an integer `>=0`, the returned array is of shape (Y, X).
            If the `channels` parameter is a sequence or a slice, the returned array is always of
               shape (Y, X, C), no matter the size of `C`.
            (see `Channels Parameter` below)

        Channels Parameter
        ------------------
        | type       | value                                               | meaning        | output shape        |
        |------------|-----------------------------------------------------|----------------|---------------------|
        | NoneType   | None (default)                                      | All channels   | (Y, X) or (Y, X, C) |
        | slice      | slice(None), slice(1), slice(0, 2), slice(2, 0, -1) | Those channels | (Y, X, C)           |
        | int        | 0, 1, 2, -1, -2, -3                                 | Channel `idx`  | (Y, X)              |
        | (int, ...) | [0], [1], [2], [-1], [-2], [-3], [0, 1], [-1, 2, 1] | Those channels | (Y, X, C)           |

        """
        dst_nodata, kwargs = _tools.deprecation_pool.handle_param_renaming_with_kwargs(
            new_name='dst_nodata', old_names={'nodata': '0.5.0'}, context='ASourceRaster.get_data',
            new_name_value=dst_nodata,
            new_name_is_provided=dst_nodata != None,
            user_kwargs=kwargs,
        )

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
        channels, kwargs = _tools.deprecation_pool.handle_param_renaming_with_kwargs(
            new_name='channels', old_names={'band': '0.6.0'}, context='ASourceRaster.get_data',
            new_name_value=channels,
            new_name_is_provided=channels is not None,
            user_kwargs=kwargs,
            transform_old=_band_to_channels,
        )
        if kwargs: # pragma: no cover
            raise TypeError("get_data() got an unexpected keyword argument '{}'".format(
                list(kwargs.keys())[0]
            ))

        # Normalize and check fp parameter
        if fp is None:
            fp = self.fp
        elif not isinstance(fp, Footprint): # pragma: no cover
            raise ValueError('`fp` parameter should be a Footprint (not {})'.format(fp))

        # Normalize and check channels parameter
        channel_ids, is_flat = _tools.normalize_channels_parameter(
            channels, len(self)
        )
        if is_flat:
            outshape = tuple(fp.shape)
        else:
            outshape = tuple(fp.shape) + (len(channel_ids),)
        del channels

        # Normalize and check dst_nodata parameter
        if dst_nodata is not None:
            dst_nodata = self.dtype.type(dst_nodata)
        elif self.nodata is not None:
            dst_nodata = self.nodata
        else:
            dst_nodata = self.dtype.type(0)

        # Check interpolation parameter
        if not (interpolation is None or
                interpolation in self._back.REMAP_INTERPOLATIONS): # pragma: no cover
            raise ValueError('`interpolation` should be None or one of {}'.format(
                set(self._back.REMAP_INTERPOLATIONS.keys())
            ))

        return self._back.get_data(
            fp=fp,
            channel_ids=channel_ids,
            dst_nodata=dst_nodata,
            interpolation=interpolation,
        ).reshape(outshape)

    # Deprecation
    fp_origin = _tools.deprecation_pool.wrap_property(
        'fp_stored',
        '0.4.4'
    )

    band_schema = _tools.deprecation_pool.wrap_property(
        'channels_schema',
        '0.6.0'
    )

class ABackSourceRaster(ABackSource, ABackSourceRasterRemapMixin):
    """Implementation of ASourceRaster's specifications"""

    def __init__(self, channels_schema, dtype, fp_stored, **kwargs):
        super(ABackSourceRaster, self).__init__(rect=fp_stored, **kwargs)

        if self.to_work is not None:
            fp = fp_stored.move(*self.to_work([
                fp_stored.tl, fp_stored.tr, fp_stored.br
            ]))
        else:
            fp = fp_stored

        self.channels_schema = channels_schema
        self.dtype = dtype
        self.fp_stored = fp_stored

        self.fp = fp

    @property
    def nodata(self):
        return self.get_nodata(0)

    def get_nodata(self, channel=0):
        return self.channels_schema['nodata'][channel]

    def __len__(self):
        return len(self.channels_schema['nodata'])

    def get_data(self, fp, channels, dst_nodata, interpolation): # pragma: no cover
        raise NotImplementedError('ABackSourceRaster.get_data is virtual pure')

if sys.version_info < (3, 6):
    # https://www.python.org/dev/peps/pep-0487/
    for k, v in ASourceRaster.__dict__.items():
        if hasattr(v, '__set_name__'):
            v.__set_name__(ASourceRaster, k)
