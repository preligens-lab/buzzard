import numpy as np
import uuid
import os
import numbers
import ntpath
import collections
import contextlib

from osgeo import gdal, ogr
import shapely
import shapely.geometry as sg

from buzzard._a_pooled_emissary_vector import *
from buzzard._a_gdal_vector import *
from buzzard._tools import conv
from buzzard import _tools
from buzzard._env import Env

class GDALFileVector(APooledEmissaryVector):

    def __init__(self, ds, allocator, open_options, mode, layer):
        back = BackGDALFileVector(
            ds._back, allocator, open_options, mode, layer,
        )
        super(GDALFileVector, self).__init__(ds=ds, back=back)

class BackGDALFileVector(ABackPooledEmissaryVector, ABackGDALVector):

    def __init__(self, back_ds, allocator, open_options, mode, layer):
        uid = uuid.uuid4()

        with back_ds.acquire_driver_object(uid, allocator) as gdal_objs:
            gdal_ds, lyr = gdal_objs
            rect = None
            if lyr is not None:
                rect = lyr.GetExtent()
            path = gdal_ds.GetDescription()
            driver = gdal_ds.GetDriver().ShortName
            wkt_stored = gdal_ds.GetProjection()
            fields = BackGDALFileVector._fields_of_lyr(lyr)
            type = conv.str_of_wkbgeom(lyr.GetGeomType())

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

    @staticmethod
    def _open_file(path, layer, driver, options, mode):
        """Open a vector datasource"""
        options = [str(arg) for arg in options] if len(options) else []
        gdal_ds = gdal.OpenEx(
            path,
            conv.of_of_mode(mode) | conv.of_of_str('vector'),
            [driver],
            options,
        )
        if gdal_ds is None:
            raise ValueError('Could not open `{}` with `{}` (gdal error: `{}`)'.format(
                path, driver, str(gdal.GetLastErrorMsg()).strip('\n')
            ))
        if layer is None:
            layer = 0
        if isinstance(layer, numbers.Integral):
            lyr = gdal_ds.GetLayer(layer)
        else:
            lyr = gdal_ds.GetLayerByName(layer)
        if lyr is None:
            raise Exception('Could not open layer (gdal error: %s)' % str(gdal.GetLastErrorMsg()).strip('\n'))
        return gdal_ds, lyr

    # Read operations *************************************************************************** **
    @property
    def extent(self):
        """Get the vector's extent in work spatial reference. (`x` then `y`)

        Example
        -------
        >>> minx, maxx, miny, maxy = ds.roofs.extent
        """
        with self.back_ds.acquire_driver_object(self.uid, self._allocator) as gdal_objs:
            _, lyr = gdal_objs
            extent = lyr.GetExtent()

        if extent is None:
            raise ValueError('Could not compute extent')
        if self.to_work:
            xa, xb, ya, yb = extent
            extent = self.to_work([[xa, ya], [xb, yb]])
            extent = np.asarray(extent)[:, :2]
            extent = extent[0, 0], extent[1, 0], extent[0, 1], extent[1, 1]
        return np.asarray(extent)

    @property
    def extent_stored(self):
        """Get the vector's extent in stored spatial reference. (minx, miny, maxx, maxy)"""
        with self.back_ds.acquire_driver_object(self.uid, self._allocator) as gdal_objs:
            _, lyr = gdal_objs
            extent = lyr.GetExtent()
        if extent is None:
            raise ValueError('Could not compute extent')
        return extent

    def __len__(self):
        """Return the number of features in vector layer"""
        with self.back_ds.acquire_driver_object(self.uid, self._allocator) as gdal_objs:
            _, lyr = gdal_objs
            return len(lyr)

    def iter_features(self, slicing, mask_poly, mask_rect):
        with self.back_ds.acquire_driver_object(self.uid, self._allocator) as gdal_objs:
            _, lyr = gdal_objs
            return self.iter_features_driver(slicing, mask_poly, mask_rect, lyr)

    # Write operations ************************************************************************** **
    def insert_data(self, geom, fields, index):
        with self.back_ds.acquire_driver_object(self.uid, self._allocator) as gdal_objs:
            _, lyr = gdal_objs
            self.insert_data_driver(geom, fields, index, lyr)

    def delete(self):
        super(BackGDALFileVector, self).delete()

        dr = gdal.GetDriverByName(self.driver)
        err = dr.Delete(self.path)
        if err:
            raise RuntimeError('Could not delete `{}` (gdal error: `{}`)'.format(
                self.path, str(gdal.GetLastErrorMsg()).strip('\n')
            ))

    # Misc ************************************************************************************** **
    def _allocator(self):
        return self._open_file(self.path, self.layer, self.driver, self.open_options, self.mode)
