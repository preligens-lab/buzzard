import numpy as np
import shapely.geometry as sg

from buzzard import _tools
from buzzard._tools import conv
from buzzard._a_stored import *
from buzzard._a_proxy_vector import *

class AStoredVector(AStored, AProxyVector):

    def insert_data(self, geom, fields=(), index=-1):
        """Insert a feature in vector

        Parameters
        ----------
        geom: shapely.base.BaseGeometry or nested sequence of coordinates
        fields: sequence or dict
            Feature's fields, missing or None fields are defaulted.

            if empty sequence: Keep all fields defaulted
            if sequence of length len(self.fields): Fields to be set, same order as self.fields
            if dict: Mapping of fields to be set
        index: int
            if -1: append feature
            else: insert feature at index (if applicable)

        Example
        -------
        >>> poly = shapely.geometry.box(10, 10, 42, 43)
        >>> fields = {'volume': 42.24}
        >>> ds.stocks.insert_data(poly, fields)

        """
        geom = self._normalize_geom(geom)
        fields = self._normalize_field_values(fields)
        self._back.insert_data(geom, fields, index)

    def _normalize_geom(self, geom):
        # Classify geom type ***************************************************
        if geom is None:
            geom_type = None
        elif isinstance(geom, sg.base.BaseGeometry):
            geom_type = 'shapely'
        elif isinstance(geom, collections.Iterable):
            geom_type = 'coordinates'
        else:
            raise TypeError('input `geom` should be a shapely geometry or nest coordinates')

        # Convert geom *********************************************************
        if geom_type is None:
            if not self._ds._back.allow_none_geometry:
                raise TypeError(
                    'Inserting None geometry not allowed '
                    '(allow None geometry in DataSource constructor to proceed)'
                )
        elif self._back.to_virtual:
            if geom_type == 'coordinates':
                geom = sg.asShape({
                    'type': self.type,
                    'coordinates': geom,
                })
            geom = shapely.ops.transform(self._back.to_virtual, geom)
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
        return geom

    def _normalize_field_values(self, fields):
        """Used on feature insertion"""
        if isinstance(fields, collections.Mapping):
            lst = [None] * len(self._back.fields)
            for k, v in fields.items():
                if v is None:
                    pass
                else:
                    i = self._back.index_of_field_name[k]
                    lst[i] = self._back.type_of_field_index[i](v)
            for defn, val in zip(self._back.fields, lst):
                if val is None and defn['nullable'] is False:
                    raise ValueError('{} not nullable'.format(defn))
            return lst
        elif isinstance(fields, collections.Iterable):
            if len(fields) == 0 and self._back.all_nullable:
                return [None] * len(self._back.fields)
            elif len(fields) != len(self._back.fields):
                raise ValueError('{} fields provided instead of {}'.format(
                    len(fields), len(self._back.fields),
                ))
            else:
                return [
                    norm(val) if val is not None else None
                    for (norm, val) in zip(self._back.type_of_field_index, fields)
                ]
        else:
            raise TypeError('Bad fields type')

class ABackStoredVector(ABackStored, ABackProxyVector):

    def __init__(self, **kwargs):
        super(ABackStoredVector, self).__init__(**kwargs)

    def insert_data(self, geom_type, geom, fields, index):
        raise NotImplementedError('ABackStoredVector.insert_data is virtual pure')
