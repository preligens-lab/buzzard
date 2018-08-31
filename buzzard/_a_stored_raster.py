import numpy as np

from buzzard import _tools
from buzzard._footprint import Footprint
from buzzard._a_stored import AStored, ABackStored
from buzzard._a_proxy_raster import AProxyRaster, ABackProxyRaster

class AStoredRaster(AStored, AProxyRaster):
    """Proxy that has both Stored and Raster specifications"""

    def set_data(self, array, fp=None, band=1, interpolation='cv_area', mask=None):
        """Write a rectangle of data on several channels to the destination raster. An optional
        `mask` may be provided to only write certain pixels of `array`.
        If `fp` is not fully within the destination raster, only the overlapping pixels are
        written.
        If `fp` is not on the same grid as the destination raster, remapping is performed using
        `interpolation` algorithm. (It fails if the `allow_interpolation` parameter is set to
        False in `DataSource` (default)). When remapping:
        - The nodata values are not interpolated, they are correctly spread to the output.
        - At most one pixel may be lost at edges due to interpolation. Provide more context in
          `array` to compensate this loss.
        - The mask parameter is also interpolated.

        The alpha bands are currently resampled like any other band, this behavior may change in
        the future.

        This method is not thread-safe.

        Parameters
        ----------
        array: numpy.ndarray of shape (Y, X) or (Y, X, B)
            Input data
        fp: Footprint of shape (Y, X) or None
            If None: write the full source raster
            If Footprint: write this window to the raster
        band: band ids or sequence of band ids (see `Band Identifiers` below)
        interpolation: one of {'cv_area', 'cv_nearest', 'cv_linear', 'cv_cubic', 'cv_lanczos4'} or None
            Resampling method
        mask: numpy array of shape (Y, X) and dtype `bool` OR inputs accepted by Footprint.burn_polygons

        Band Identifiers
        ------------
        | id type    | id value        | meaning          |
        |------------|-----------------|------------------|
        | int        | -1              | All bands        |
        | int        | 1, 2, 3, ...    | Band `i`         |
        | complex    | -1j             | All bands mask   |
        | complex    | 0j              | Shared mask band |
        | complex    | 1j, 2j, 3j, ... | Mask of band `i` |

        Caveat
        ------
        When using a Raster backed by a driver (like a GDAL driver), the data might be flushed to
        disk only after the garbage collection of the driver object. To be absolutely sure that the
        driver cache is flushed to disk, call `.close` or `.deactivate` on this Raster.

        """
        if self.mode != 'w': # pragma: no cover
            raise RuntimeError('Cannot write a read-only raster file')

        # Normalize and check fp parameter
        if fp is None:
            fp = self.fp
        elif not isinstance(fp, Footprint):
            raise ValueError('`fp` parameter should be a Footprint (not {})'.format(fp)) # pragma: no cover

        # Normalize and check band parameter
        band_ids, _ = _tools.normalize_band_parameter(band, len(self), self.shared_band_id)

        # Normalize and check array parameter
        array = np.atleast_3d(array)
        if array.ndim != 3: # pragma: no cover
            raise ValueError('Input array should have 2 or 3 dimensions')
        if array.shape[:2] != tuple(fp.shape): # pragma: no cover
            raise ValueError('Incompatible shape between input `array` and `fp`')
        if len(band_ids) != array.shape[-1]: # pragma: no cover
            raise ValueError('Incompatible number of channels between input `array` and `band`')

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
            band_ids=band_ids,
            interpolation=interpolation,
            mask=mask,
        )

    def fill(self, value, band=1):
        """Fill bands with value.

        This method is not thread-safe.

        Parameters
        ----------
        value: nbr
        band: band ids or sequence of band ids (see `Band Identifiers` below)

        Band Identifiers
        ------------
        | id type    | id value        | meaning          |
        |------------|-----------------|------------------|
        | int        | -1              | All bands        |
        | int        | 1, 2, 3, ...    | Band `i`         |
        | complex    | -1j             | All bands mask   |
        | complex    | 0j              | Shared mask band |
        | complex    | 1j, 2j, 3j, ... | Mask of band `i` |

        Caveat
        ------
        When using a Raster backed by a driver (like a GDAL driver), the data might be flushed to
        disk only after the garbage collection of the driver object. To be absolutely sure that the
        driver cache is flushed to disk, call `.close` or `.deactivate` on this Raster.

        """
        if self.mode != 'w': # pragma: no cover
            raise RuntimeError('Cannot write a read-only raster file')

        band_ids, _ = _tools.normalize_band_parameter(band, len(self), self.shared_band_id)

        self._back.fill(
            value=value,
            band_ids=band_ids,
        )

class ABackStoredRaster(ABackStored, ABackProxyRaster):
    """Implementation of AStoredRaster's specifications"""

    def set_data(self, array, fp, band_ids, interpolation, mask): # pragma: no cover
        raise NotImplementedError('ABackStoredRaster.set_data is virtual pure')

    def fill(self, value, band_ids): # pragma: no cover
        raise NotImplementedError('ABackStoredRaster.fill is virtual pure')
