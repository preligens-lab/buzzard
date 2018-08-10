import uuid
import contextlib

from osgeo import gdal

from buzzard._a_pooled_emissary_raster import APooledEmissaryRaster, ABackPooledEmissaryRaster
from buzzard._a_gdal_raster import ABackGDALRaster
from buzzard._tools import conv
from buzzard._footprint import Footprint

class GDALFileRaster(APooledEmissaryRaster):
    """Proxy for raster files using GDAL driver except MEM"""

    def __init__(self, ds, allocator, open_options, mode):
        back = BackGDALFileRaster(
            ds._back, allocator, open_options, mode,
        )
        super(GDALFileRaster, self).__init__(ds=ds, back=back)

class BackGDALFileRaster(ABackPooledEmissaryRaster, ABackGDALRaster):
    """Implementation of GDALFileRaster"""

    def __init__(self, back_ds, allocator, open_options, mode):
        uid = uuid.uuid4()

        with back_ds.acquire_driver_object(uid, allocator) as gdal_ds:
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

        super(BackGDALFileRaster, self).__init__(
            back_ds=back_ds,
            wkt_stored=wkt_stored,
            band_schema=band_schema,
            dtype=dtype,
            fp_stored=fp_stored,
            mode=mode,
            driver=driver,
            open_options=open_options,
            path=path,
            uid=uid,
        )

    @contextlib.contextmanager
    def acquire_driver_object(self):
        with self.back_ds.acquire_driver_object(
            self.uid,
            self.allocator
        ) as gdal_ds:
            yield gdal_ds

    def delete(self):
        super(BackGDALFileRaster, self).delete()

        dr = gdal.GetDriverByName(self.driver)
        err = dr.Delete(self.path)
        if err: # pragma: no cover
            raise RuntimeError('Could not delete `{}` (gdal error: `{}`)'.format(
                self.path, str(gdal.GetLastErrorMsg()).strip('\n')
            ))

    def allocator(self):
        return self.open_file(self.path, self.driver, self.open_options, self.mode)

    @staticmethod
    def open_file(path, driver, options, mode):
        """Open a raster dataset"""
        gdal_ds = gdal.OpenEx(
            path,
            conv.of_of_mode(mode) | conv.of_of_str('raster'),
            [driver],
            options,
        )
        if gdal_ds is None: # pragma: no cover
            raise ValueError('Could not open `{}` with `{}` (gdal error: `{}`)'.format(
                path, driver, str(gdal.GetLastErrorMsg()).strip('\n')
            ))
        return gdal_ds
