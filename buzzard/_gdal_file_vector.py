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
from buzzard._gdal_vector_mixin import *
from buzzard._tools import conv
from buzzard import _tools
from buzzard._env import Env

class GDALFileVector(APooledEmissaryVector):

    def __init__(self, ds, allocator, open_options, mode, layer):
        back = BackGDALFileVector(
            ds._back, allocator, open_options, mode, layer,
        )
        super(GDALFileVector, self).__init__(ds=ds, back=back)

class BackGDALFileVector(ABackPooledEmissaryVector, BackGDALVectorMixin):

    def __init__(self, back_ds, allocator, open_options, mode, layer):
        uid = uuid.uuid4()

        with back_ds.acquire_driver_object(uid, allocator) as gdal_objds:
            gdal_ds, lyr = gdal_objds
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



    @property
    def extent(self):
        """Get the vector's extent in work spatial reference. (`x` then `y`)

        Example
        -------
        >>> minx, maxx, miny, maxy = ds.roofs.extent
        """
        with self.back_ds.acquire_driver_object(self.uid, self.allocator) as gdal_objds:
            _, lyr = gdal_objds
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
        with self.back_ds.acquire_driver_object(self.uid, self.allocator) as gdal_objds:
            _, lyr = gdal_objds
            extent = lyr.GetExtent()

        if extent is None:
            raise ValueError('Could not compute extent')
        return extent


    def insert_data(self, geom_type, geom, fields, index):
        if geom is None:
            pass
        elif self._to_virtual:
            if geom_type == 'coordinates':
                geom = sg.asShape({
                    'type': self.type,
                    'coordinates': geom,
                })
            geom = shapely.ops.transform(self.to_virtual, geom)
            geom = conv.ogr_of_shapely(geom)
            # TODO: Use json and unit test
            # mapping = sg.mapping(geom)
            # geom = conv.ogr_of_coordinates(
            #     mapping['coordinates'],
            #     mapping['type'],
            # )
            if geom is None:
                raise ValueError('Could not convert `{}` of type `{}` to `ogr.Geometry`'.format(
                    geom_type, self.type
                ))
        elif geom_type == 'coordinates':
            geom = conv.ogr_of_coordinates(geom, self.type)
            if geom is None:
                raise ValueError('Could not convert `{}` of type `{}` to `ogr.Geometry`'.format(
                    geom_type, self.type
                ))
        elif geom_type == 'shapely':
            geom = conv.ogr_of_shapely(geom)
            # TODO: Use json and unit test
            # mapping = sg.mapping(geom)
            # geom = conv.ogr_of_coordinates(
            #     mapping['coordinates'],
            #     mapping['type'],
            # )
            if geom is None:
                raise ValueError('Could not convert `{}` of type `{}` to `ogr.Geometry`'.format(
                    geom_type, self.type
                ))
        else:
            assert False # pragma: no cover

        with self.back_ds.acquire_driver_object(self.uid, self.allocator) as gdal_objds:
            _, lyr = gdal_objds
            ftr = ogr.Feature(lyr.GetLayerDefn())

            if geom is not None:
                err = ftr.SetGeometry(geom)
                if err:
                    raise ValueError('Could not set geometry (%s)' % str(gdal.GetLastErrorMsg()).strip('\n'))

                if not self._ds._allow_none_geometry and ftr.GetGeometryRef() is None:
                    raise ValueError(
                        'Invalid geometry inserted '
                        '(allow None geometry in DataSource constructor to silence)'
                    )

            if index >= 0:
                err = ftr.SetFID(index)
                if err:
                    raise ValueError('Could not set field id (%s)' % str(gdal.GetLastErrorMsg()).strip('\n'))
            for i, field in enumerate(fields):
                if field is not None:
                    err = ftr.SetField2(i, self._type_of_field_index[i](field))
                    if err:
                        raise ValueError('Could not set field #{} ({}) ({})'.format(
                            i, field, str(gdal.GetLastErrorMsg()).strip('\n')
                        ))
            passed = ftr.Validate(ogr.F_VAL_ALL, True)
            if not passed:
                raise ValueError('Invalid feature {} ({})'.format(
                    err, str(gdal.GetLastErrorMsg()).strip('\n')
                ))

            err = lyr.CreateFeature(ftr)
            if err:
                raise ValueError('Could not create feature {} ({})'.format(
                    err, str(gdal.GetLastErrorMsg()).strip('\n')
                ))


    def delete(self):
        super(BackGDALFileVector, self).delete()

        dr = gdal.GetDriverByName(self.driver)
        err = dr.Delete(self.path)
        if err:
            raise RuntimeError('Could not delete `{}` (gdal error: `{}`)'.format(
                self.path, str(gdal.GetLastErrorMsg()).strip('\n')
            ))

    def _allocator(self):
        return self._open_file(self.path, self.layer, self.driver, self.open_options, self.mode)

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

    def _iter_feature(self, slicing, mask_poly, mask_rect):
        with self.back_ds.acquire_driver_object(self.uid, self.allocator) as gdal_objds:
            _, lyr = gdal_objds
            if mask_poly is not None:
                lyr.SetSpatialFilter(mask_poly)
            elif mask_rect is not None:
                lyr.SetSpatialFilterRect(*mask_rect)

            start, stop, step = slicing.indices(len(lyr))
            indices = range(start, stop, step)
            ftr = None # Necessary to prevent the old swig bug
            if step == 1:
                lyr.SetNextByIndex(start)
                for i in indices:
                    ftr = lyr.GetNextFeature()
                    if ftr is None:
                        raise IndexError('Feature #{} not found'.format(i))
                    yield ftr
            else:
                for i in indices:
                    lyr.SetNextByIndex(i)
                    ftr = lyr.GetNextFeature()
                    if ftr is None:
                        raise IndexError('Feature #{} not found'.format(i))
                    yield ftr

        # Necessary to prevent the old swig bug
        # https://trac.osgeo.org/gdal/ticket/6749
        del slicing, mask_poly, mask_rect, ftr


