import numpy as np

from buzzard import _tools
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
        if geom is None:
            if not self._ds._back.allow_none_geometry:
                raise TypeError(
                    'Inserting None geometry not allowed '
                    '(allow None geometry in DataSource constructor to proceed)'
                )
            geom_type = None
        elif isinstance(geom, sg.base.BaseGeometry):
            geom_type = 'shapely'
        elif isinstance(geom, collections.Iterable):
            geom_type = 'coordinates'
        else:
            raise TypeError('input `geom` should be a shapely geometry or nest coordinates')
        fields = self._normalize_field_values(fields)
        self._back.insert_data(geom_type, geom, fields, index)

class ABackStoredVector(ABackStored, ABackProxyVector):

    def insert_data(self, geom_type, geom, fields, index):
        raise NotImplementedError('ABackStoredVector.insert_data is virtual pure')
