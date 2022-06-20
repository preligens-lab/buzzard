import contextlib

from buzzard._a_emissary_raster import AEmissaryRaster, ABackEmissaryRaster
from buzzard._a_gdal_raster import ABackGDALRaster
from buzzard._tools import conv
from buzzard._footprint import Footprint

class GDALMemRaster(AEmissaryRaster):
    """Concrete class defining the behavior of a GDAL raster using the "MEM" driver.

    >>> help(Dataset.create_raster)

    Features Defined
    ----------------
    None
    """

    def __init__(self, ds, fp, dtype, channel_count, channels_schema, open_options, sr):
        back = BackGDALMemRaster(
            ds._back, fp, dtype, channel_count, channels_schema, open_options, sr,
        )
        super().__init__(ds=ds, back=back)

class BackGDALMemRaster(ABackEmissaryRaster, ABackGDALRaster):
    """Implementation of GDALMemRaster"""

    def __init__(self, back_ds, fp, dtype, channel_count, channels_schema, open_options, sr):

        gdal_ds = self.create_file(
            '', fp, dtype, channel_count, channels_schema, 'MEM', open_options, sr, False
        )
        self._gdal_ds = gdal_ds

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

        super().__init__(
            back_ds=back_ds,
            wkt_stored=wkt_stored,
            channels_schema=channels_schema,
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
        super().close()
        del self._gdal_ds
