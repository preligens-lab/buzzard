""">>> help(RasterPhysical)"""

from __future__ import division, print_function
import numbers
import os

import numpy as np
from osgeo import gdal, osr

from buzzard._footprint import Footprint
from buzzard._tools import conv
from buzzard import _tools
from buzzard._raster import Raster

class RasterPhysical(Raster):
    """Concrete class of raster sources containing physical data"""

    @classmethod
    def _create_file(cls, path, fp, dtype, band_count, band_schema, driver, options, sr):
        """Create a raster datasource"""
        dr = gdal.GetDriverByName(driver)
        if os.path.isfile(path):
            err = dr.Delete(path)
            if err:
                raise Exception('Could not delete %s' % path)

        options = [str(arg) for arg in options] if len(options) else []
        gdal_ds = dr.Create(
            path, fp.rsizex, fp.rsizey, band_count, conv.gdt_of_any_equiv(dtype), options
        )
        if gdal_ds is None:
            raise Exception('Could not create gdal dataset (%s)' % gdal.GetLastErrorMsg())
        if sr is not None:
            gdal_ds.SetProjection(osr.GetUserInputAsWKT(sr))
        gdal_ds.SetGeoTransform(fp.gt)

        band_schema = cls._sanitize_band_schema(band_schema, band_count)
        cls._apply_band_schema(gdal_ds, band_schema)

        gdal_ds.FlushCache()
        return gdal_ds

    @classmethod
    def _open_file(cls, path, driver, options, mode):
        """Open a raster datasource"""
        options = [str(arg) for arg in options] if len(options) else []
        gdal_ds = gdal.OpenEx(
            path,
            conv.of_of_mode(mode) | conv.of_of_str('raster'),
            [driver],
            options,
        )
        if gdal_ds is None:
            raise ValueError('Could not open `{}` with `{}` (gdal error: `{}`)'.format(
                path, driver, gdal.GetLastErrorMsg()
            ))
        return gdal_ds

    def __init__(self, ds, gdal_ds, mode):
        """Instanciated by DataSource class, instanciation by user is undefined"""
        Raster.__init__(self, ds, gdal_ds)
        self._mode = mode

    @property
    def delete(self):
        """Delete a raster file with a call or a context management.

        Example
        -------
        >>> ds.dem.delete()
        >>> with ds.dem.delete:
                # code...
        >>> with ds.create_araster('/tmp/tmp.tif', fp, float, 1).delete as tmp:
                # code...
        """
        if self._mode != 'w':
            raise RuntimeError('Cannot remove a read-only file')

        def _delete():
            path = self.path
            dr = self._gdal_ds.GetDriver()
            self._ds._unregister(self)
            del self._gdal_ds
            del self._ds
            err = dr.Delete(path)
            if err:
                raise RuntimeError('Could not delete `{}` (gdal error: `{}`)'.format(
                    path, gdal.GetLastErrorMsg()
                ))

        return _RasterDeleteRoutine(self, _delete)

    @property
    def mode(self):
        """Get raster open mode"""
        return str(self._mode)

    @property
    def path(self):
        """Get raster file path"""
        return self._gdal_ds.GetDescription()

    def set_data(self, array, fp=None, band=1, interpolation='cv_area', mask=None, op=np.rint):
        """Set `data` located at `fp` in raster file. An optional `mask` may be provided.

        fp can be partially or fully outside of target

        If `allow_interpolation` is enabled in the DataSource constructor, it is then possible to
        use a `fp` that is not aligned  with the source raster, interpolation in then used to remap
        `array` from `fp` to raster. `nodata` values are also handled and spreaded to the raster
        through remapping.

        When remapping, if the input is not aligned with the raster file, at most one pixel pixel
        may be lost at edges due to interpolation. Provide more context in `array` to counter this effect.

        Parameters
        ----------
        array: numpy.ndarray of shape (Y, X) or (Y, X, B)
            Input data
        fp: Footprint
            Of shape (Y, X)
            Within in raster file
        band: band index or sequence of band index (see `Band Indices` below)
        interpolation: one of ('cv_area', 'cv_nearest', 'cv_linear', 'cv_cubic', 'cv_lanczos4')
            Resampling method
        mask: numpy array of shape (Y, X) OR inputs accepted by Footprint.burn_polygons
        op: None or vector function
            Rounding function following an interpolation when file type is integer

        Band Indices
        ------------
        | index type | index value     | meaning          |
        |------------|-----------------|------------------|
        | int        | -1              | All bands        |
        | int        | 1, 2, 3, ...    | Band `i`         |
        | complex    | -1j             | All bands mask   |
        | complex    | 0j              | Shared mask band |
        | complex    | 1j, 2j, 3j, ... | Mask of band `i` |

        """

        if self._mode != 'w':
            raise RuntimeError('Cannot write a read-only file')

        # Normalize fp parameter
        if fp is None:
            fp = self.fp
        elif not isinstance(fp, Footprint):
            raise ValueError('Bad fp type `%s`' % type(fp)) # pragma: no cover

        # Check array shape
        array = np.asarray(array)
        if array.shape[:2] != tuple(fp.shape):
            raise ValueError('Incompatible shape between array:%s and fp:%s' % (
                array.shape, fp.shape
            )) # pragma: no cover

        # Normalize and check band parameter
        bands, _ = _tools.normalize_band_parameter(band, len(self), self._shared_band_index)

        # Normalize and check mask parameter
        if mask is None:
            mask = np.ones(fp.shape, dtype=bool)
        elif isinstance(mask, np.ndarray):
            if mask.ndim != 2 or mask.shape != tuple(fp.shape):
                raise ValueError('Mask of shape %s instead of %s' % (mask.shape, fp.shape))
            mask = mask.astype(bool)
        else:
            mask = fp.burn_polygons(mask)

        # Normalize and check array shape
        if array.ndim == 2:
            array = array[:, :, np.newaxis]
        elif array.ndim != 3:
            raise ValueError('Array has shape %d' % array.shape) # pragma: no cover
        if array.shape[-1] != len(bands):
            raise ValueError('Incompatible band count between array:%d and band:%d' % (
                array.shape[-1], len(bands)
            )) # pragma: no cover

        # Check op parameter
        if not isinstance(np.zeros(1, dtype=self.dtype)[0], numbers.Integral):
            op = None

        # Normalize interpolation parameter
        if not self._ds._allow_interpolation:
            interpolation = None

        # Normalize array dtype
        array = array.astype(self.dtype)

        self._set_data_unsafe(array, fp, bands, interpolation, mask, op)

    def fill(self, value, band=1):
        """Fill bands with value.

        Parameters
        ----------
        value: nbr
        band: band index or sequence of band index (see `Band Indices` below)

        Band Indices
        ------------
        | index type | index value     | meaning          |
        |------------|-----------------|------------------|
        | int        | -1              | All bands        |
        | int        | 1, 2, 3, ...    | Band `i`         |
        | complex    | -1j             | All bands mask   |
        | complex    | 0j              | Shared mask band |
        | complex    | 1j, 2j, 3j, ... | Mask of band `i` |

        """
        if self._mode != 'w':
            raise RuntimeError('Cannot write a read-only file')

        bands, _ = _tools.normalize_band_parameter(band, len(self), self._shared_band_index)
        del band

        for gdalband in [self._gdalband_of_index(i) for i in bands]:
            gdalband.Fill(value)

_RasterDeleteRoutine = type('_RasterDeleteRoutine', (_tools.CallOrContext,), {
    '__doc__': RasterPhysical.delete.__doc__,
})
