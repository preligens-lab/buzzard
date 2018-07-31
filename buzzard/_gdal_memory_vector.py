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

        with self.__class__._LayerIteration(self._lyr, self._lock,
                                            self._ds._ogr_layer_lock == 'wait'):
            lyr = self._lyr
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
        raise NotImplementedError('GDAL Memory driver does no allow deletion, use `close`')

    def close(self):
        super(BackGDALMemoryVector, self).close()
        del self._gdal_ds


    def _iter_feature(self, slicing, mask_poly, mask_rect):
        with self.__class__._LayerIteration(self._lyr, self._lock,
                                            self._ds._ogr_layer_lock == 'wait'):
            lyr = self._lyr
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

    class _LayerIteration(object):
        """Context manager to control layer iteration"""

        def __init__(self, lyr, lock, wait):
            self._lock = lock
            self._wait = wait
            self._lyr = lyr

        def __enter__(self):
            if self._lock is not None:
                got_lock = self._lock.acquire(self._wait)
                if not got_lock:
                    raise Exception('ogr layer is already locked')

        def __exit__(self, exc_type=None, exc_val=None, exc_tb=None):
            self._lyr.ResetReading()
            self._lyr.SetSpatialFilter(None)
            if self._lock is not None:
                self._lock.release()
