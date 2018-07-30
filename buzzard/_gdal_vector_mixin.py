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

class BackGDALVectorMixin(object):

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
