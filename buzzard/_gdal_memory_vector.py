import numpy as np
import uuid
from buzzard._a_pooled_emissary_vector import *
from buzzard._a_gdal_vector import *
from buzzard._tools import conv

class GDALMemoryVector(AEmissaryVector):
    """Proxy for 'Memory' driver vector GDAL datasets"""

    def __init__(self, ds, geometry, fields, open_options, mode, layer, sr):
        back = BackGDALMemoryVector(
            ds._back, geometry, fields, open_options, mode, layer, sr,
        )
        super(GDALMemoryVector, self).__init__(ds=ds, back=back)

class BackGDALMemoryVector(ABackEmissaryVector, ABackGDALVector):
    """Implementation of GDALMemoryVector"""

    def __init__(self, back_ds, geometry, fields, open_options, mode, layer, sr):
        gdal_ds, lyr = self.create_file('', geometry, fields, layer, 'Memory', open_options, sr)

        self._gdal_ds = gdal_ds
        self._lyr = lyr

        rect = None
        if lyr is not None:
            rect = lyr.GetExtent()

        path = gdal_ds.GetDescription()
        driver = gdal_ds.GetDriver().ShortName
        wkt_stored = gdal_ds.GetProjection()
        fields = BackGDALMemoryVector._fields_of_lyr(lyr)
        type = conv.str_of_wkbgeom(lyr.GetGeomType())

        super(BackGDALMemoryVector, self).__init__(
            back_ds=back_ds,
            wkt_stored=wkt_stored,
            mode=mode,
            driver=driver,
            open_options=open_options,
            path=path,
            layer=layer,
            fields=fields,
            rect=rect,
            type=type
        )

        self._type_of_field_index = [
            conv.type_of_oftstr(field['type'])
            for field in self.fields
        ]

    @contextlib.contextmanager
    def acquire_driver_object(self):
        yield self._gdal_ds, self._lyr

    def delete(self):
        raise NotImplementedError('GDAL Memory driver does no allow deletion, use `close`')

    def close(self):
        super(BackGDALMemoryVector, self).close()
        del self._lyr
        del self._gdal_ds
