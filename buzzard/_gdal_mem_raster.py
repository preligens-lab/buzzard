import contextlib

from buzzard._a_emissary_raster import AEmissaryRaster, ABackEmissaryRaster
from buzzard._a_gdal_raster import ABackGDALRaster
from buzzard._tools import conv
from buzzard._footprint import Footprint

class GDALMemRaster(AEmissaryRaster):
    """Proxy for 'MEM' driver raster GDAL datasets"""

    def __init__(self, ds, fp, dtype, band_count, band_schema, open_options, sr):
        back = BackGDALMemRaster(
            ds._back, fp, dtype, band_count, band_schema, open_options, sr,
        )
        super(GDALMemRaster, self).__init__(ds=ds, back=back)

class BackGDALMemRaster(ABackEmissaryRaster, ABackGDALRaster):
    """Implementation of GDALMemRaster"""

    def __init__(self, back_ds, fp, dtype, band_count, band_schema, open_options, sr):

        gdal_ds = self.create_file(
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
        sr = gdal_ds.GetProjection()
        if sr == '':
            wkt_stored = None
        else:
            wkt_stored = sr

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

    @contextlib.contextmanager
    def acquire_driver_object(self):
        yield self._gdal_ds

    def delete(self): # pragma: no cover
        raise NotImplementedError('GDAL MEM driver does no allow deletion, use `close`')

    def close(self):
        super(BackGDALMemRaster, self).close()
        del self._gdal_ds
