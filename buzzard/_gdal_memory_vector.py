import numpy as np
import uuid
import os
import numbers
import ntpath
import collections

from osgeo import gdal, ogr
import shapely
import shapely.geometry as sg

from buzzard._a_pooled_emissary_vector import *
from buzzard._a_gdal_vector import *
from buzzard._tools import conv
from buzzard import _tools
from buzzard._env import Env

class GDALMemoryVector(AEmissaryVector):

    def __init__(self, ds, geometry, fields, open_options, mode, layer, sr):
        back = BackGDALMemoryVector(
            ds._back, geometry, fields, open_options, mode, layer, sr,
        )
        super(GDALMemoryVector, self).__init__(ds=ds, back=back)

class BackGDALMemoryVector(ABackEmissaryVector, ABackGDALVector):

    def __init__(self, back_ds, geometry, fields, open_options, mode, layer, sr):
        gdal_ds, lyr = self._create_file('', geometry, fields, layer, 'Memory', open_options, sr)

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

    # Read operations *************************************************************************** **
    @property
    def extent(self):
        """Get the vector's extent in work spatial reference. (`x` then `y`)

        Example
        -------
        >>> minx, maxx, miny, maxy = ds.roofs.extent
        """
        extent = self._lyr.GetExtent()

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
        extent = self._lyr.GetExtent()

        if extent is None:
            raise ValueError('Could not compute extent')
        return extent

    def __len__(self):
        """Return the number of features in vector layer"""
        return len(self._lyr)

    def iter_features(self, slicing, mask_poly, mask_rect):
        return self.iter_features_driver(slicing, mask_poly, mask_rect, self._lyr)

    # Write operations ************************************************************************** **
    def insert_data(self, geom, fields, index):
        self.insert_data_driver(geom, fields, index, self._lyr)

    def delete(self):
        raise NotImplementedError('GDAL Memory driver does no allow deletion, use `close`')

    # Misc ************************************************************************************** **
    def close(self):
        super(BackGDALMemoryVector, self).close()
        del self._gdal_ds
