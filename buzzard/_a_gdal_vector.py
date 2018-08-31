import contextlib

import numpy as np
from osgeo import gdal, ogr, osr
import shapely
import shapely.geometry as sg

from buzzard._a_stored_vector import ABackStoredVector
from buzzard._tools import conv
from buzzard._env import Env

class ABackGDALVector(ABackStoredVector):
    """Abstract class defining the common implementation of all vector file formats
    defined by the OGR
    """

    # extent/len implementation ***************************************************************** **
    @property
    def extent(self):
        with self.acquire_driver_object() as gdal_objs:
            _, lyr = gdal_objs
            extent = lyr.GetExtent()

        if extent is None: # pragma: no cover
            raise ValueError('Could not compute extent')
        if self.to_work:
            xa, xb, ya, yb = extent
            extent = self.to_work([[xa, ya], [xb, yb]])
            extent = np.asarray(extent)[:, :2]
            extent = extent[0, 0], extent[1, 0], extent[0, 1], extent[1, 1]
        return np.asarray(extent)

    @property
    def extent_stored(self):
        with self.acquire_driver_object() as gdal_objs:
            _, lyr = gdal_objs
            extent = lyr.GetExtent()
        if extent is None: # pragma: no cover
            raise ValueError('Could not compute extent')
        return np.asarray(extent)

    def __len__(self):
        """Return the number of features in vector layer"""
        with self.acquire_driver_object() as gdal_objs:
            _, lyr = gdal_objs
            return len(lyr)

    # iter_data implementation ****************************************************************** **
    def iter_data(self, geom_type, field_indices, slicing, mask_poly, mask_rect, clip):
        clip_poly = None
        if mask_poly is not None:
            mask_poly = conv.ogr_of_shapely(mask_poly)
            if clip:
                clip_poly = mask_poly
        elif mask_rect is not None:
            if clip:
                clip_poly = conv.ogr_of_shapely(sg.box(*mask_rect))

        ftr = None # Necessary to prevent the old swig bug
        geom = None # Necessary to prevent the old swig bug
        with self.acquire_driver_object() as gdal_objs:
            _, lyr = gdal_objs
            for ftr in self.iter_features_driver(slicing, mask_poly, mask_rect, lyr):
                geom = ftr.geometry()
                if geom is None or geom.IsEmpty():
                    # `geom is None` and `geom.IsEmpty()` is not exactly the same case, but whatever
                    geom = None
                    if not self.back_ds.allow_none_geometry: # pragma: no cover
                        raise Exception(
                            'None geometry in feature '
                            '(allow None geometry in DataSource constructor to silence)'
                        )
                else:
                    if clip:
                        geom = geom.Intersection(clip_poly)
                        assert not geom.IsEmpty()
                    geom = conv.shapely_of_ogr(geom)
                    if self.to_work:
                        geom = shapely.ops.transform(self.to_work, geom)
                    if geom_type == 'coordinates':
                        geom = sg.mapping(geom)['coordinates']
                    elif geom_type == 'geojson':
                        geom = sg.mapping(geom)

                yield (geom,) + tuple(
                    self._type_of_field_index[index](ftr.GetField(index))
                    if ftr.GetField(index) is not None
                    else None
                    for index in field_indices
                )

        # Necessary to prevent the old swig bug
        # https://trac.osgeo.org/gdal/ticket/6749
        del geom
        del ftr
        del clip_poly
        del mask_rect, mask_poly

    @staticmethod
    def iter_features_driver(slicing, mask_poly, mask_rect, lyr):
        with contextlib.ExitStack() as stack:
            stack.push(lambda *args, **kwargs: lyr.ResetReading())
            if mask_poly is not None:
                lyr.SetSpatialFilter(mask_poly)
                stack.push(lambda *args, **kwargs: lyr.SetSpatialFilter(None))
            elif mask_rect is not None:
                lyr.SetSpatialFilterRect(*mask_rect)
                stack.push(lambda *args, **kwargs: lyr.SetSpatialFilter(None))

            start, stop, step = slicing.indices(len(lyr))
            indices = range(start, stop, step)
            ftr = None # Necessary to prevent the old swig bug
            if step == 1:
                lyr.SetNextByIndex(start)
                for i in indices:
                    ftr = lyr.GetNextFeature()
                    if ftr is None: # pragma: no cover
                        raise IndexError('Feature #{} not found'.format(i))
                    yield ftr
            else:
                for i in indices:
                    lyr.SetNextByIndex(i)
                    ftr = lyr.GetNextFeature()
                    if ftr is None: # pragma: no cover
                        raise IndexError('Feature #{} not found'.format(i))
                    yield ftr

        # Necessary to prevent the old swig bug
        # https://trac.osgeo.org/gdal/ticket/6749
        del slicing, mask_poly, mask_rect, ftr

    # insert_data implementation **************************************************************** **
    def insert_data(self, geom, geom_type, fields, index):
        geom = self._ogr_of_geom(geom, geom_type)
        with self.acquire_driver_object() as gdal_objs:
            _, lyr = gdal_objs
            ftr = ogr.Feature(lyr.GetLayerDefn())

            if geom is not None:
                err = ftr.SetGeometry(geom)
                if err: # pragma: no cover
                    raise ValueError('Could not set geometry (%s)' % str(gdal.GetLastErrorMsg()).strip('\n'))

                if not self.back_ds.allow_none_geometry and ftr.GetGeometryRef() is None: # pragma: no cover
                    raise ValueError(
                        'Invalid geometry inserted '
                        '(allow None geometry in DataSource constructor to silence)'
                    )

            if index >= 0:
                err = ftr.SetFID(index)
                if err: # pragma: no cover
                    raise ValueError('Could not set field id (%s)' % str(gdal.GetLastErrorMsg()).strip('\n'))
            for i, field in enumerate(fields):
                if field is not None:
                    err = ftr.SetField2(i, self._type_of_field_index[i](field))
                    if err: # pragma: no cover
                        raise ValueError('Could not set field #{} ({}) ({})'.format(
                            i, field, str(gdal.GetLastErrorMsg()).strip('\n')
                        ))
            passed = ftr.Validate(ogr.F_VAL_ALL, True)
            if not passed: # pragma: no cover
                raise ValueError('Invalid feature {} ({})'.format(
                    err, str(gdal.GetLastErrorMsg()).strip('\n')
                ))

            err = lyr.CreateFeature(ftr)
            if err: # pragma: no cover
                raise ValueError('Could not create feature {} ({})'.format(
                    err, str(gdal.GetLastErrorMsg()).strip('\n')
                ))

    def _ogr_of_geom(self, geom, geom_type):
        if geom_type is None:
            geom = geom
        elif self.to_virtual:
            if geom_type == 'coordinates':
                geom = sg.asShape({
                    'type': self.type,
                    'coordinates': geom,
                })
            geom = shapely.ops.transform(self.to_virtual, geom)
            # geom = conv.ogr_of_shapely(geom)
            # TODO: Use json and unit test
            mapping = sg.mapping(geom)
            geom = conv.ogr_of_coordinates(
                mapping['coordinates'],
                mapping['type'],
            )
            if geom is None: # pragma: no cover
                raise ValueError('Could not convert `{}` of type `{}` to `ogr.Geometry`'.format(
                    geom_type, self.type
                ))
        elif geom_type == 'coordinates':
            geom = conv.ogr_of_coordinates(geom, self.type)
            if geom is None: # pragma: no cover
                raise ValueError('Could not convert `{}` of type `{}` to `ogr.Geometry`'.format(
                    geom_type, self.type
                ))
        elif geom_type == 'shapely':
            # geom = conv.ogr_of_shapely(geom)
            # TODO: Use json and unit test
            mapping = sg.mapping(geom)
            geom = conv.ogr_of_coordinates(
                mapping['coordinates'],
                mapping['type'],
            )
            if geom is None: # pragma: no cover
                raise ValueError('Could not convert `{}` of type `{}` to `ogr.Geometry`'.format(
                    geom_type, self.type
                ))
        else:
            assert False # pragma: no cover
        return geom

    # Misc ************************************************************************************** **
    def acquire_driver_object(self): # pragma: no cover
        raise NotImplementedError('ABackGDALRaster.acquire_driver_object is virtual pure')

    @classmethod
    def create_file(cls, path, geometry, fields, layer, driver, options, sr):
        """Create a vector datasource"""

        with Env(_osgeo_use_exceptions=False):
            dr = gdal.GetDriverByName(driver)
            gdal_ds = gdal.OpenEx(
                path,
                conv.of_of_mode('w') | conv.of_of_str('vector'),
                [driver],
                options,
            )
            if gdal_ds is None:
                gdal_ds = dr.Create(path, 0, 0, 0, 0, options)
            else:
                if gdal_ds.GetLayerByName(layer) is not None: # pragma: no cover
                    err = gdal_ds.DeleteLayer(layer)
                    if err:
                        raise Exception('Could not delete %s' % path)

            # See TODO on deletion of existing file
            # if gdal_ds.GetLayerCount() == 0:
            #     del gdal_ds
            #     err = dr.DeleteDataSource(path)
            #     if err:
            #         raise Exception('Could not delete %s' % path)
            #     gdal_ds = dr.CreateDataSource(path, options)

            if gdal_ds is None: # pragma: no cover
                raise Exception('Could not create gdal dataset (%s)' % str(gdal.GetLastErrorMsg()).strip('\n'))

        if sr is not None:
            sr = osr.SpatialReference(osr.GetUserInputAsWKT(sr))

        geometry = conv.wkbgeom_of_str(geometry)
        lyr = gdal_ds.CreateLayer(layer, sr, geometry, options)

        if lyr is None: # pragma: no cover
            raise Exception('Could not create layer (%s)' % str(gdal.GetLastErrorMsg()).strip('\n'))

        for field in fields:
            flddef = ogr.FieldDefn(field['name'], field['type'])
            if field['precision'] is not None:
                flddef.SetPrecision(field['precision'])
            if field['width'] is not None:
                flddef.SetWidth(field['width'])
            if field['nullable'] is not None:
                flddef.SetNullable(field['nullable'])
            if field['default'] is not None:
                flddef.SetDefault(field['default'])
            lyr.CreateField(flddef)
        lyr.SyncToDisk()
        gdal_ds.FlushCache()
        return gdal_ds, lyr

    @classmethod
    def _fields_of_lyr(cls, lyr):
        """Used on file opening / creation"""
        featdef = lyr.GetLayerDefn()
        field_count = featdef.GetFieldCount()
        return [cls._field_of_def(featdef.GetFieldDefn(i)) for i in range(field_count)]

    @staticmethod
    def _field_of_def(fielddef):
        """Used on file opening / creation"""
        oft = fielddef.type
        oftstr = conv.str_of_oft(oft)
        type_ = conv.type_of_oftstr(oftstr)
        default = fielddef.GetDefault()
        return {
            'name': fielddef.name,
            'precision': fielddef.precision,
            'width': fielddef.width,
            'nullable': bool(fielddef.IsNullable()),
            'default': None if default is None else type_(default),
            'type': oftstr,
        }
