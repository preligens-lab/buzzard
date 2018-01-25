""">>> help(Raster)"""

from __future__ import division, print_function
import numbers

import numpy as np

from buzzard._footprint import Footprint
from buzzard._proxy import Proxy
from buzzard._raster_utils import RasterUtilsMixin
from buzzard._raster_remap import RemapMixin
from buzzard._raster_getset_data import RasterGetSetMixin
from buzzard._tools import conv
from buzzard import _tools

class Raster(Proxy, RasterGetSetMixin, RasterUtilsMixin, RemapMixin):
    """Abstract class to all raster sources"""

    def __init__(self, ds, gdal_ds):
        fp_origin = Footprint(
            gt=gdal_ds.GetGeoTransform(),
            rsize=(gdal_ds.RasterXSize, gdal_ds.RasterYSize),
        )
        Proxy.__init__(self, ds, gdal_ds.GetProjection(), fp_origin)

        if self._to_work is not None:
            fp = fp_origin.move(*self._to_work([
                fp_origin.tl, fp_origin.tr, fp_origin.br
            ]))
        else:
            fp = fp_origin

        self._gdal_ds = gdal_ds
        self._fp = fp
        self._fp_origin = fp_origin
        self._band_schema = self._band_schema_of_gdal_ds(gdal_ds)

        self._shared_band_index = None
        for i, type in enumerate(self._band_schema['mask'], 1):
            if type == 'per_dataset':
                self._shared_band_index = i
                break

    @property
    def close(self):
        """Close a raster with a call or a context management.

        Examples
        --------
        >>> ds.dem.close()
        >>> with ds.dem.close:
                # code...
        >>> with ds.create_araster('result.tif', fp, float, 1).close as result:
                # code...
        """
        def _close():
            self._ds._unregister(self)
            del self._gdal_ds
            del self._ds

        return _RasterCloseRoutine(self, _close)

    # PROPERTY GETTERS ************************************************************************** **
    @property
    def band_schema(self):
        """Band schema"""
        return dict(self._band_schema)

    @property
    def fp(self):
        """Accessor for inner Footprint instance"""
        return self._fp

    @property
    def fp_origin(self):
        """Accessor for inner Footprint instance"""
        return self._fp_origin

    @property
    def dtype(self):
        """Accessor for dtype"""
        return conv.dtype_of_gdt_downcast(self._gdal_ds.GetRasterBand(1).DataType)

    @property
    def nodata(self):
        """Accessor for first band's nodata value"""
        return self.get_nodata(1)

    def get_nodata(self, band=1):
        """Accessor for nodata value"""
        return self._gdal_ds.GetRasterBand(band).GetNoDataValue()

    def __len__(self):
        return self._gdal_ds.RasterCount

    def get_data(self, band=1, fp=None, mask=None, nodata=None, interpolation='cv_area',
                 dtype=None, op=np.rint):
        """Get `data` located at `fp` in raster file. An optional `mask` may be provided.

        fp can be partially or fully outside of file


        Parameters
        ----------
        band: band index or sequence of band index (see `Band Indices` below)
        fp: Footprint
            Clip output to `fp`
            If none provided, return the full raster
        nodata: Number
            Override self.get_nodata()
        interpolation: one of ['cv_area', ]
            Resampling method
        dtype: type
            Override gdal output type
        op: None or vector function
            Rounding function following an interpolation when output type is integer


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

        # Normalize and check fp parameter
        if fp is None:
            fp = self.fp
        elif not isinstance(fp, Footprint):
            raise ValueError('Bad fp type `%s`' % type(fp)) # pragma: no cover

        # Normalize and check dtype parameter
        if dtype is None:
            dtype = self.dtype
        else:
            dtype = conv.dtype_of_any_downcast(dtype)

        # Normalize and check band parameter
        bands, is_flat = _tools.normalize_band_parameter(band, len(self), self._shared_band_index)
        if is_flat:
            outshape = fp.shape
        else:
            outshape = tuple(fp.shape) + (len(bands),)
        del band

        # Normalize and check nodata
        if nodata is None:
            nodataconv = False
            onodata = self.nodata # May be None
        else:
            if not isinstance(nodata, numbers.Number):
                raise ValueError('Bad nodata type') # pragma: no cover
            onodata = nodata
            nodataconv = self.nodata is not None

        # Check op parameter
        if not isinstance(np.zeros(1, dtype=dtype)[0], numbers.Integral):
            op = None

        # Check mask parameter
        if mask is not None:
            mask = np.asarray(mask).astype(bool)
            if mask.shape != tuple(fp.shape):
                raise ValueError('mask should have the same shape as `fp`') # pragma: no cover

        # Normalize interpolation parameter
        if not self._ds._allow_interpolation:
            interpolation = None

        # Work
        dilate_size = 4 * self.fp.pxsizex / fp.pxsizex # hyperparameter
        dilate_size = max(2, np.ceil(dilate_size))
        samplefp = fp.dilate(dilate_size)
        if not samplefp.share_area(self.fp):
            if onodata is None:
                raise Exception(
                    "Querying data fully outside of file's Footprint, but `nodata` is not known. "
                    "Provide a `nodata` parameter or create a new file with a `nodata` value set."
                )
            return np.full(outshape, onodata, dtype)
        else:
            samplefp = self.fp & samplefp

        samplebands = self._sample_bands(fp, samplefp, bands, mask, interpolation)

        if nodataconv:
            samplebands[samplebands == self.nodata] = onodata

        array = self._remap(
            samplefp,
            fp,
            interpolation=interpolation,
            array=samplebands,
            mask=None,
            nodata=onodata,
            mask_mode='erode',
        )
        del samplebands

        if op is not None:
            array = op(array)
        return array.astype(dtype).reshape(outshape)

_RasterCloseRoutine = type('_RasterCloseRoutine', (_tools.CallOrContext,), {
    '__doc__': Raster.close.__doc__,
})
