import os
import numpy as np

from osgeo import gdal

from buzzard._a_emissary_raster import *
from buzzard._a_gdal_raster import *
from buzzard._tools import conv
from buzzard import _tools

class GDALMemRaster(AEmissaryRaster):

    def __init__(self, ds, fp, dtype, band_count, band_schema, open_options, sr):
        back = BackGDALMemRaster(
            ds._back, fp, dtype, band_count, band_schema, open_options, sr,
        )
        super(GDALMemRaster, self).__init__(ds=ds, back=back)

class BackGDALMemRaster(ABackEmissaryRaster, ABackGDALRaster):

    def __init__(self, back_ds, fp, dtype, band_count, band_schema, open_options, sr):

        gdal_ds = self._create_file(
            '', fp, dtype, band_count, band_schema, 'MEM', open_options, sr
        )
        self._gdal_ds = gdal_ds

        path = gdal_ds.GetDescription()
        driver = gdal_ds.GetDriver().ShortName
        fp_stored = Footprint(
            gt=gdal_ds.GetGeoTransform(),
            rsize=(gdal_ds.RasterXSize, gdal_ds.RasterYSize),
        )
        band_schema = self._band_schema_of_gdal_ds(gdal_ds)
        dtype = conv.dtype_of_gdt_downcast(gdal_ds.GetRasterBand(1).DataType)
        wkt_stored = gdal_ds.GetProjection()

        super(BackGDALMemRaster, self).__init__(
            back_ds=back_ds,
            wkt_stored=wkt_stored,
            band_schema=band_schema,
            dtype=dtype,
            fp_stored=fp_stored,
            mode='w',
            driver=driver,
            open_options=open_options,
            path=path,
        )

    def _sample(self, fp, band_ids):
        assert fp.same_grid(self.fp), (
            str(fp),
            str(self.fp),
        )
        return self.get_data_driver(fp, band_ids, self._gdal_ds)


    def set_data(self, array, fp, band_ids, interpolation, mask):
        if not fp.share_area(self.fp):
            return
        if not fp.same_grid(self.fp) and mask is None:
            mask = np.ones(fp.shape, bool)

        dstfp = self.fp.intersection(fp)
        # if array.dtype == np.int8:
        #     array = array.astype('uint8')

        # Remap ****************************************************************
        ret = self.remap(
            fp,
            dstfp,
            array=array,
            mask=mask,
            src_nodata=self.nodata,
            dst_nodata=self.nodata or 0,
            mask_mode='erode',
            interpolation=interpolation,
        )
        if mask is not None:
            array, mask = ret
        else:
            array = ret
        del ret
        array = array.astype(self.dtype, copy=False)
        fp = dstfp
        del dstfp

        # Write ****************************************************************
        gdal_ds = self._gdal_ds
        if True:
            for i, band_id in enumerate(band_ids):
                leftx, topy = self.fp.spatial_to_raster(fp.tl)
                gdalband = self._gdalband_of_band_id(gdal_ds, band_id)

                for sl in _tools.slices_of_matrix(mask):
                    a = array[:, :, i][sl]
                    assert a.ndim == 2
                    x = int(sl[1].start + leftx)
                    y = int(sl[0].start + topy)
                    assert x >= 0
                    assert y >= 0
                    assert x + a.shape[1] <= self.fp.rsizex
                    assert y + a.shape[0] <= self.fp.rsizey
                    gdalband.WriteArray(a, x, y)

            # gdal_ds.FlushCache()

    def fill(self, value, band_ids):
        for gdalband in [self._gdalband_of_band_id(self._gdal_ds, band_id) for band_id in band_ids]:
            gdalband.Fill(value)

    def delete(self):
        raise NotImplementedError('GDAL MEM driver does no allow deletion, use `close`')

    def close(self):
        super(BackGDALMemRaster, self).close()
        del self._gdal_ds
