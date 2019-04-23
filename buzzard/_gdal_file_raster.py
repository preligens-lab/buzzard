import uuid
import contextlib

from osgeo import gdal

from buzzard._a_pooled_emissary_raster import APooledEmissaryRaster, ABackPooledEmissaryRaster
from buzzard._a_gdal_raster import ABackGDALRaster
from buzzard._tools import conv, GDALErrorCatcher
from buzzard._footprint import Footprint

class GDALFileRaster(APooledEmissaryRaster):
    """Concrete class defining the behavior of a GDAL raster using a file.

    >>> help(Dataset.open_raster)
    >>> help(Dataset.create_raster)

    Features Defined
    ----------------
    None
    """

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
            channels_schema = self._channels_schema_of_gdal_ds(gdal_ds)
            dtype = conv.dtype_of_gdt_downcast(gdal_ds.GetRasterBand(1).DataType)
            sr = gdal_ds.GetProjection()
            if sr == '':
                wkt_stored = None
            else:
                wkt_stored = sr

        super(BackGDALFileRaster, self).__init__(
            back_ds=back_ds,
            wkt_stored=wkt_stored,
            channels_schema=channels_schema,
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

        success, payload = GDALErrorCatcher(gdal.GetDriverByName, none_is_error=True)(self.driver)
        if not success: # pragma: no cover
            raise ValueError('Could not find a driver named `{}` (gdal error: `{}`)'.format(
                self.driver, payload[1]
            ))
        dr = payload

        success, payload = GDALErrorCatcher(dr.Delete, nonzero_int_is_error=True)(self.path)
        if not success: # pragma: no cover
            raise RuntimeError('Could not delete `{}` using driver `{}` (gdal error: `{}`)'.format(
                self.path, dr.ShortName, payload[1]
            ))

    def allocator(self):
        return self.open_file(self.path, self.driver, self.open_options, self.mode)

    @staticmethod
    def open_file(path, driver, options, mode):
        """Open a raster dataset"""

        success, payload = GDALErrorCatcher(gdal.OpenEx, none_is_error=True)(
            path,
            conv.of_of_mode(mode) | conv.of_of_str('raster'),
            [driver],
            options,
        )
        if not success:
            raise RuntimeError('Could not open `{}` using driver `{}` (gdal error: `{}`)'.format(
                path, driver, payload[1]
            ))
        gdal_ds = payload

        return gdal_ds
