""">>> help(VectorGetSetMixin)"""

import collections

from osgeo import ogr, gdal
import shapely.geometry as sg
import shapely.ops

from buzzard._tools import conv
from ._footprint import Footprint

class VectorGetSetMixin(object):
    """Private mixin for the Vector class containing subroutines for iteration and insertions"""

    @staticmethod
    def _normalize_mask_parameter(mask):
        if isinstance(mask, sg.base.BaseGeometry):
            return mask, None
        elif isinstance(mask, Footprint):
            return mask.poly, None
        elif isinstance(mask, collections.Container):
            mask = [float(v) for v in mask]
            minx, maxx, miny, maxy = mask
            mask = minx, miny, maxx, maxy
            return None, mask
        elif mask is None:
            return None, None
        else:
            raise TypeError('`mask` should be a Footprint, an extent or a shapely object')

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

    def _iter_feature(self, slicing, mask_poly, mask_rect):
        with self.__class__._LayerIteration(self._lyr, self._lock,
                                            self._ds._ogr_layer_lock == 'wait'):
            if mask_poly is not None:
                self._lyr.SetSpatialFilter(mask_poly)
            elif mask_rect is not None:
                self._lyr.SetSpatialFilterRect(*mask_rect)

            start, stop, step = slicing.indices(len(self._lyr))
            indices = range(start, stop, step)
            if step == 1:
                self._lyr.SetNextByIndex(start)
                for i in indices:
                    ftr = self._lyr.GetNextFeature()
                    if ftr is None:
                        raise IndexError('Feature #{} not found'.format(i))
                    yield ftr
            else:
                for i in indices:
                    self._lyr.SetNextByIndex(i)
                    ftr = self._lyr.GetNextFeature()
                    if ftr is None:
                        raise IndexError('Feature #{} not found'.format(i))
                    yield ftr

    def _iter_data_unsafe(self, geom_type, field_indices, slicing,
                          mask_poly, mask_rect, clip):
        clip_poly = None
        if mask_poly is not None:
            mask_poly = conv.ogr_of_shapely(mask_poly)
            if clip:
                clip_poly = mask_poly
        elif mask_rect is not None:
            if clip:
                clip_poly = conv.ogr_of_shapely(sg.box(*mask_rect))

        for ftr in self._iter_feature(slicing, mask_poly, mask_rect):
            geom = ftr.geometry()
            if geom is None or geom.IsEmpty():
                # `geom is None` and `geom.IsEmpty()` is not exactly the same case, but whatever?
                geom = None
                if not self._ds._allow_none_geometry:
                    raise Exception(
                        'None geometry in feature '
                        '(allow None geometry in DataSource constructor to silence)'
                    )
            else:
                if clip:
                    geom = geom.Intersection(clip_poly)
                    assert not geom.IsEmpty()
                geom = conv.shapely_of_ogr(geom)
                if self._to_work:
                    geom = shapely.ops.transform(self._to_work, geom)
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

    def _insert_data_unsafe(self, geom_type, geom, fields, index):
        if geom is None:
            pass
        elif self._to_file:
            if geom_type == 'coordinates':
                geom = sg.asShape({
                    'type': self.type,
                    'coordinates': geom,
                })
            geom = shapely.ops.transform(self._to_file, geom)
            geom = conv.ogr_of_shapely(geom)
            if geom is None:
                raise ValueError('Could not convert `{}` of type `{}` to `ogr.Geometry`'.format(
                    geom_type, self._type
                ))
        elif geom_type == 'coordinates':
            geom = conv.ogr_of_coordinates(geom, self.type)
            if geom is None:
                raise ValueError('Could not convert `{}` of type `{}` to `ogr.Geometry`'.format(
                    geom_type, self._type
                ))
        elif geom_type == 'shapely':
            geom = conv.ogr_of_shapely(geom)
            if geom is None:
                raise ValueError('Could not convert `{}` of type `{}` to `ogr.Geometry`'.format(
                    geom_type, self._type
                ))
        else:
            assert False # pragma: no cover

        with self.__class__._LayerIteration(self._lyr, self._lock,
                                            self._ds._ogr_layer_lock == 'wait'):
            ftr = ogr.Feature(self._lyr.GetLayerDefn())

            if geom is not None:
                err = ftr.SetGeometry(geom)
                if err:
                    raise ValueError('Could not set geometry (%s)' % gdal.GetLastErrorMsg())

                if not self._ds._allow_none_geometry and ftr.GetGeometryRef() is None:
                    raise ValueError(
                        'Invalid geometry inserted '
                        '(allow None geometry in DataSource constructor to silence)'
                    )

            if index >= 0:
                err = ftr.SetFID(index)
                if err:
                    raise ValueError('Could not set field id (%s)' % gdal.GetLastErrorMsg())
            for i, field in enumerate(fields):
                if field is not None:
                    err = ftr.SetField2(i, self._type_of_field_index[i](field))
                    if err:
                        raise ValueError('Could not set field #{} ({}) ({})'.format(
                            i, field, gdal.GetLastErrorMsg()
                        ))
            passed = ftr.Validate(ogr.F_VAL_ALL, True)
            if not passed:
                raise ValueError('Invalid feature {} ({})'.format(
                    err, gdal.GetLastErrorMsg()
                ))

            err = self._lyr.CreateFeature(ftr)
            if err:
                raise ValueError('Could not create feature {} ({})'.format(
                    err, gdal.GetLastErrorMsg()
                ))
