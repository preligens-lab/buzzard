import numpy as np

from buzzard import _tools
from buzzard._footprint import Footprint
from buzzard._a_stored import AStored, ABackStored
from buzzard._a_source_raster import ASourceRaster, ABackSourceRaster

class AStoredRaster(AStored, ASourceRaster):
    """Base abstract class defining the common behavior of all rasters that are stored somewhere
    (like RAM or disk).

    Features Defined
    ----------------
    - Has a `set_data` method that allows to write pixels to storage
    """

    def set_data(self, array, fp=None, channels=None, interpolation='cv_area', mask=None, **kwargs):
        """Write a rectangle of data to the destination raster. Each channel in `array` is written to
        one channel in `raster` in the same order as described by the `channels` parameter. An
        optional `mask` may be provided to only write certain pixels of `array`.

        If `fp` is not fully within the destination raster, only the overlapping pixels are
        written.
        If `fp` is not on the same grid as the destination raster, remapping is automatically
        performed using the `interpolation` algorithm. (It fails if the `allow_interpolation`
        parameter is set to `False` in `Dataset` (default)). When interpolating:
        - The nodata values are not interpolated, they are correctly spread to the output.
        - At most one pixel may be lost at edges due to interpolation. Provide more context in
          `array` to compensate this loss.
        - The mask parameter is also interpolated.

        The alpha bands are currently resampled like any other band, this behavior may change in
        the future.

        This method is not thread-safe.

        Parameters
        ----------
        array: numpy.ndarray of shape (Y, X) or (Y, X, C)
            The values to be written
        fp: Footprint of shape (Y, X) or None
            If None: write the full source raster
            If Footprint: write this window to the raster
        channels: None or int or slice or sequence of int (see `Channels Parameter` below)
            The channels to be written.
        interpolation: one of {'cv_area', 'cv_nearest', 'cv_linear', 'cv_cubic', 'cv_lanczos4'} or None
            OpenCV method used if intepolation is necessary
        mask: numpy array of shape (Y, X) and dtype `bool` OR inputs accepted by `Footprint.burn_polygons`

        Channels Parameter
        ------------------
        | type       | value                                               | meaning        |
        |------------|-----------------------------------------------------|----------------|
        | NoneType   | None (default)                                      | All channels   |
        | slice      | slice(None), slice(1), slice(0, 2), slice(2, 0, -1) | Those channels |
        | int        | 0, 1, 2, -1, -2, -3                                 | Channel `idx`  |
        | (int, ...) | [0], [1], [2], [-1], [-2], [-3], [0, 1], [-1, 2, 1] | Those channels |

        Caveat
        ------
        When using a Raster backed by a driver (like a GDAL driver), the data might be flushed to
        disk only after the garbage collection of the driver object. To be absolutely sure that the
        driver cache is flushed to disk, call `.close` or `.deactivate` on this Raster.

        """

        if self.mode != 'w': # pragma: no cover
            raise RuntimeError('Cannot write a read-only raster file')

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
            new_name='channels', old_names={'band': '0.6.0'}, context='ASourceRaster.set_data',
            new_name_value=channels,
            new_name_is_provided=channels is not None,
            user_kwargs=kwargs,
            transform_old=_band_to_channels,
        )
        if kwargs: # pragma: no cover
            raise TypeError("set_data() got an unexpected keyword argument '{}'".format(
                list(kwargs.keys())[0]
            ))

        # Normalize and check fp parameter
        if fp is None:
            fp = self.fp
        elif not isinstance(fp, Footprint):
            raise ValueError('`fp` parameter should be a Footprint (not {})'.format(fp)) # pragma: no cover

        # Normalize and check channels parameter
        channel_ids, _ = _tools.normalize_channels_parameter(channels, len(self))
        if len(channel_ids) != len(set(channel_ids)): # pragma: no cover
            raise ValueError("The `channels` parameter should not reference twice the same channel")
        del channels

        # Normalize and check array parameter
        array = np.atleast_3d(array)
        if array.ndim != 3: # pragma: no cover
            raise ValueError('Input array should have 2 or 3 dimensions, not {}'.format(array.ndim))
        if array.shape[:2] != tuple(fp.shape): # pragma: no cover
            msg = 'Incompatible shape between input `array` ({}) and `fp` ({})'.format(
                array.shape[:2], tuple(fp.shape)
            )
            raise ValueError(msg)
        if len(channel_ids) != array.shape[-1]: # pragma: no cover
            msg = 'Incompatible number of channels between `array` ({}) and `channels` ({})'.format(
                len(channel_ids), array.shape[-1]
            )
            raise ValueError(msg)

        # Normalize and check mask parameter
        if mask is not None:
            if isinstance(mask, np.ndarray):
                mask = mask.astype(bool, copy=False)
                if mask.ndim != 2: # pragma: no cover
                    raise ValueError('Input `mask` should 2 dimensions')
                if mask.shape[:2] != tuple(fp.shape): # pragma: no cover
                    raise ValueError('Incompatible shape between input `mask` and `fp`')
            else:
                mask = fp.burn_polygons(mask)

        # Check interpolation parameter here
        if not (interpolation is None or interpolation in self._back.REMAP_INTERPOLATIONS): # pragma: no cover
            raise ValueError('`interpolation` should be None or one of {}'.format(
                set(self._back.REMAP_INTERPOLATIONS.keys())
            ))

        return self._back.set_data(
            array=array,
            fp=fp,
            channel_ids=channel_ids,
            interpolation=interpolation,
            mask=mask,
        )

    def fill(self, value, channels=None, **kwargs):
        """Fill raster with value.

        This method is not thread-safe.

        Parameters
        ----------
        value: nbr
        channels: int or sequence of int (see `Channels Parameter` below)
            The channels to be written

        Channels Parameter
        ------------------
        | type       | value                                               | meaning        |
        |------------|-----------------------------------------------------|----------------|
        | NoneType   | None (default)                                      | All channels   |
        | slice      | slice(None), slice(1), slice(0, 2), slice(2, 0, -1) | Those channels |
        | int        | 0, 1, 2, -1, -2, -3                                 | Channel `idx`  |
        | (int, ...) | [0], [1], [2], [-1], [-2], [-3], [0, 1], [-1, 2, 1] | Those channels |

        Caveat
        ------
        When using a Raster backed by a driver (like a GDAL driver), the data might be flushed to
        disk only after the garbage collection of the driver object. To be absolutely sure that the
        driver cache is flushed to disk, call `.close` or `.deactivate` on this Raster.

        """
        if self.mode != 'w': # pragma: no cover
            raise RuntimeError('Cannot write a read-only raster file')

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
            new_name='channels', old_names={'band': '0.6.0'}, context='ASourceRaster.fill',
            new_name_value=channels,
            new_name_is_provided=channels is not None,
            user_kwargs=kwargs,
            transform_old=_band_to_channels,
        )
        if kwargs: # pragma: no cover
            raise TypeError("fill() got an unexpected keyword argument '{}'".format(
                list(kwargs.keys())[0]
            ))

        channel_ids, _ = _tools.normalize_channels_parameter(channels, len(self))
        del channels
        channel_ids = set(channel_ids)

        value = self.dtype.type(value).tolist()

        self._back.fill(
            value=value,
            channel_ids=channel_ids,
        )

class ABackStoredRaster(ABackStored, ABackSourceRaster):
    """Implementation of AStoredRaster's specifications"""

    def set_data(self, array, fp, channels, interpolation, mask, **kwargs): # pragma: no cover
        raise NotImplementedError('ABackStoredRaster.set_data is virtual pure')

    def fill(self, value, channels, **kwargs): # pragma: no cover
        raise NotImplementedError('ABackStoredRaster.fill is virtual pure')
