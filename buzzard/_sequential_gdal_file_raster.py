import uuid

from _a_pooled_emissary_raster import *

class SequentialGDALFileRaster(APooledEmissaryRaster):

    def __init__(self, ds, path, driver, open_options, mode):
        back_ds = ds._back

        # uuid = uuid.uuid4()
        # with back_ds.acquire(uuid, lambda: TODO._open_file(path, driver, open_options, mode)) as gdal_ds:
        #     path = gdal_ds.GetDescription()
        #     driver = gdal_ds.GetDriver().ShortName
        #     fp_stored = Footprint(
        #         gt=gdal_ds.GetGeoTransform(),
        #         rsize=(gdal_ds.RasterXSize, gdal_ds.RasterYSize),
        #     )
        #     band_schema = Raster._band_schema_of_gdal_ds(gdal_ds)
        #     dtype = conv.dtype_of_gdt_downcast(gdal_ds.GetRasterBand(1).DataType)
        #     wkt_stored = gdal_ds.GetProjection()

        back = BackSequentialGDALFileRaster(
            back_ds,
            ...
        )

        super(SequentialGDALFileRaster, self).__init__(ds=ds, back=back)

class BackSequentialGDALFileRaster(ABackPooledEmissaryRaster):

    def __init__(self, back_ds, ...,
                 # gdal_ds,
                 # path, driver, open_options, mode
    ):

        super(..., self).__init__(
            back_ds=back_ds,
            wkt_stored=wkt_stored,
            band_schema=band_schema,
            dtype=dtype,
            fp_stored=fp_stored,
            mode=mode,
            driver=driver,
            open_options=open_options,
            path=path,
            uuid=uuid,
        )

    def get_data(self): pass
    def set_data(self): pass

    def fill(self, value, bands):
        for gdalband in [self._gdalband_of_index(i) for i in bands]:
            gdalband.Fill(value)

    @property
    def delete(self):
        pass

    def allocator(self):
        return TODO._open_file(self.path, self.driver, self.open_options, self.mode))
