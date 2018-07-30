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
from buzzard._tools import conv
from buzzard import _tools
from buzzard._env import Env

class GDALFileVector(APooledEmissaryVector):

    def __init__(self, ds, allocator, open_options, mode, layer):
        back = BackGDALFileVector(
            ds._back, allocator, open_options, mode, layer,
        )
        super(GDALFileVector, self).__init__(ds=ds, back=back)

class BackGDALFileVector(ABackPooledEmissaryVector):

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
            extent = self._to_work([[xa, ya], [xb, yb]])
            extent = np.asarray(extent)[:, :2]
            extent = extent[0, 0], extent[1, 0], extent[0, 1], extent[1, 1]
        return np.asarray(extent)

    @property
    def extent_stored(self):
        """Get the vector's extent in stored spatial reference. (minx, miny, maxx, maxy)"""
        with self.back_ds.acquire_driver_object(self.uid, self.allocator) as gdal_objds:
            gdal_ds, lyr = gdal_objds
            extent = lyr.GetExtent()

        if extent is None:
            raise ValueError('Could not compute extent')
        return extent


    def get_bounds(self):
        extent = self.extent
        return np.asarray([extent[0], extent[2], extent[1], extent[3]])

    def get_bounds_stored(self,):
        extent = self.extent_stored
        return np.asarray([extent[0], extent[2], extent[1], extent[3]])

    def iter_data(self, geom_type, field_indices, slicing,
                          mask_poly, mask_rect, clip):
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
        for ftr in self._iter_feature(slicing, mask_poly, mask_rect):
            geom = ftr.geometry()
            if geom is None or geom.IsEmpty():
                # `geom is None` and `geom.IsEmpty()` is not exactly the same case, but whatever?
                geom = None
                if not self.back_ds.allow_none_geometry:
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
            yield (geom,) + tuple([
                self._type_of_field_index[index](ftr.GetField(index))
                if ftr.GetField(index) is not None
                else None
                for index in field_indices
            ])

        # Necessary to prevent the old swig bug
        # https://trac.osgeo.org/gdal/ticket/6749
        del geom
        del ftr
        del clip_poly
        del mask_rect, mask_poly


    def get_data(self, index, fields=-1, geom_type='shapely', mask=None, clip=False):
        index = int(index)
        for val in self.iter_data(fields, geom_type, mask, clip, slice(index, index + 1, 1)):
            return val
        else:
            raise IndexError('Feature `{}` not found'.format(index))


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


    @classmethod
    def _create_file(cls, path, geometry, fields, layer, driver, options, sr):
        """Create a vector datasource"""

        if layer is None:
            layer = '.'.join(ntpath.basename(path).split('.')[:-1])
        elif not isinstance(layer, str):
            raise TypeError('layer should be None or str')

        options = [str(arg) for arg in options] if len(options) else []

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
                if gdal_ds.GetLayerByName(layer) is not None:
                    err = gdal_ds.DeleteLayer(layer)
                    if err:
                        raise Exception('Could not delete %s' % path)

            # See todo on deletion of existing file
            # if gdal_ds.GetLayerCount() == 0:
            #     del gdal_ds
            #     err = dr.DeleteDataSource(path)
            #     if err:
            #         raise Exception('Could not delete %s' % path)
            #     gdal_ds = dr.CreateDataSource(path, options)

            if gdal_ds is None:
                raise Exception('Could not create gdal dataset (%s)' % str(gdal.GetLastErrorMsg()).strip('\n'))

        if sr is not None:
            sr = osr.SpatialReference(osr.GetUserInputAsWKT(sr))

        geometry = conv.wkbgeom_of_str(geometry)
        lyr = gdal_ds.CreateLayer(layer, sr, geometry, options)

        if lyr is None:
            raise Exception('Could not create layer (%s)' % str(gdal.GetLastErrorMsg()).strip('\n'))

        fields = cls._normalize_fields_defn(fields)
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

    @staticmethod
    def _normalize_fields_defn(fields):
        """Used on file creation"""
        if not isinstance(fields, collections.Iterable):
            raise TypeError('Bad fields definition type')

        def _sanitize_dict(dic):
            dic = dict(dic)
            name = dic.pop('name')
            type_ = dic.pop('type')
            precision = dic.pop('precision', None)
            width = dic.pop('width', None)
            nullable = dic.pop('nullable', None)
            default = dic.pop('default', None)
            oft = conv.oft_of_any(type_)
            if default is not None:
                default = str(conv.type_of_oftstr(conv.str_of_oft(oft))(default))
            if len(dic) != 0:
                raise ValueError('unexpected keys in {} dict: {}'.format(name, dic))
            return dict(
                name=name,
                type=oft,
                precision=precision,
                width=width,
                nullable=nullable,
                default=default,
            )
        return [_sanitize_dict(dic) for dic in fields]

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

    @classmethod
    def _fields_of_lyr(cls, lyr):
        """Used on file opening / creation"""
        featdef = lyr.GetLayerDefn()
        field_count = featdef.GetFieldCount()
        return [cls._field_of_def(featdef.GetFieldDefn(i)) for i in range(field_count)]
