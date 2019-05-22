import uuid
import contextlib

from osgeo import gdal
import numpy as np

from buzzard._a_pooled_emissary_vector import APooledEmissaryVector, ABackPooledEmissaryVector
from buzzard._a_gdal_vector import ABackGDALVector
from buzzard._tools import conv, GDALErrorCatcher

class GDALFileVector(APooledEmissaryVector):
    """Concrete class defining the behavior of a GDAL vector using a file

    >>> help(Dataset.open_vector)
    >>> help(Dataset.create_vector)

    Features Defined
    ----------------
    None
    """

    def __init__(self, ds, allocator, open_options, mode):
        back = BackGDALFileVector(
            ds._back, allocator, open_options, mode,
        )
        super(GDALFileVector, self).__init__(ds=ds, back=back)

class BackGDALFileVector(ABackPooledEmissaryVector, ABackGDALVector):
    """Implementation of GDALFileVector"""

    def __init__(self, back_ds, allocator, open_options, mode):
        uid = uuid.uuid4()

        with back_ds.acquire_driver_object(uid, allocator) as (gdal_ds, lyr):
            rect = None
            if lyr is not None:
                rect = lyr.GetExtent()
            path = gdal_ds.GetDescription()
            driver = gdal_ds.GetDriver().ShortName
            sr = lyr.GetSpatialRef()
            if sr is None:
                wkt_stored = None
            else:
                wkt_stored = sr.ExportToWkt()
            fields = BackGDALFileVector._fields_of_lyr(lyr)
            type = conv.str_of_wkbgeom(lyr.GetGeomType())
            layer = lyr.GetName()

        super(BackGDALFileVector, self).__init__(
            back_ds=back_ds,
            wkt_stored=wkt_stored,
            mode=mode,
            driver=driver,
            open_options=open_options,
            path=path,
            uid=uid,
            layer=layer,
            fields=fields,
            rect=rect,
            type=type
        )

        self._type_of_field_index = [
            conv.type_of_oftstr(field['type'])
            for field in self.fields
        ]

    def allocator(self):
        return self.open_file(self.path, self.layer, self.driver, self.open_options, self.mode)

    @staticmethod
    def open_file(path, layer, driver, options, mode):
        """Open a vector dataset"""
        options = [str(arg) for arg in options] if len(options) else []
        success, payload = GDALErrorCatcher(gdal.OpenEx, none_is_error=True)(
            path,
            conv.of_of_mode(mode) | conv.of_of_str('vector'),
            [driver],
            options,
        )
        if not success:
            raise RuntimeError('Could not open `{}` using driver `{}` (gdal error: `{}`)'.format(
                path, driver, payload[1]
            ))
        gdal_ds = payload

        if layer is None:
            layer = 0
        if np.all(np.isreal(layer)):
            success, payload = GDALErrorCatcher(gdal_ds.GetLayer)(int(layer))
        else:
            success, payload = GDALErrorCatcher(gdal_ds.GetLayerByName)(layer)

        if not success: # pragma: no cover
            count = gdal_ds.GetLayerCount()
            raise Exception('Could not open layer `{}` of `{}` ({} layers available: {}) (gdal error: `{}`)'.format(
                layer,
                path,
                count,
                {i: gdal_ds.GetLayerByIndex(i).GetName() for i in range(count)},
                payload[1],
            ))
        lyr = payload

        return gdal_ds, lyr

    @contextlib.contextmanager
    def acquire_driver_object(self):
        with self.back_ds.acquire_driver_object(
            self.uid,
            self.allocator,
        ) as gdal_objs:
            yield gdal_objs

    def delete(self):
        super(BackGDALFileVector, self).delete()

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
