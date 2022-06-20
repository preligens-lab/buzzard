from collections.abc import Iterable, Mapping

import shapely.geometry as sg

from buzzard._a_stored import AStored, ABackStored
from buzzard._a_source_vector import ASourceVector, ABackSourceVector

class AStoredVector(AStored, ASourceVector):
    """Base abstract class defining the common behavior of all vectors that are stored somewhere
    (like RAM or disk).

    Features Defined
    ----------------
    - Has an `insert_data` method that allows to write geometries to storage
    """

    def insert_data(self, geom, fields=(), index=-1):
        """.. _vector file insert_data:

        Insert a feature in vector.

        This method is not thread-safe.

        Parameters
        ----------
        geom: shapely.base.BaseGeometry or nested sequence of coordinates
            ..
        fields: sequence or dict
            Feature's fields, missing or None fields are defaulted.

            - if empty sequence: Keep all fields defaulted
            - if sequence of length len(self.fields): Fields to be set, same order as self.fields
            - if dict: Mapping of fields to be set

        index: int
            - if -1: append feature
            - else: insert feature at index (if applicable)

        Example
        -------
        >>> poly = shapely.geometry.box(10, 10, 42, 43)
        >>> fields = {'volume': 42.24}
        >>> ds.stocks.insert_data(poly, fields)

        Caveat
        ------
        When using a Vector backed by a driver (like an OGR driver), the data might be flushed to
        disk only after the garbage collection of the driver object. To be absolutely sure that the
        driver cache is flushed to disk, call `.close` or `.deactivate` on this Vector.

        """
        if geom is None: # pragma: no cover
            if not self._back.back_ds.allow_none_geometry:
                raise TypeError(
                    'Inserting None geometry not allowed '
                    '(Set `allow_none_geometry=True` in Dataset constructor to proceed)'
                )
        elif isinstance(geom, sg.base.BaseGeometry):
            geom_type = 'shapely'
        elif isinstance(geom, Iterable):
            geom_type = 'coordinates'
        else:
            raise TypeError('input `geom` should be a shapely geometry or nest coordinates')
        fields = self._normalize_field_values(fields)
        self._back.insert_data(geom, geom_type, fields, index)

    def _normalize_field_values(self, fields):
        """Used on feature insertion"""
        if isinstance(fields, Mapping):
            lst = [None] * len(self._back.fields)
            for k, v in fields.items():
                if v is None:
                    pass
                else:
                    i = self._back.index_of_field_name[k]
                    lst[i] = self._back.type_of_field_index[i](v)
            for defn, val in zip(self._back.fields, lst):
                if val is None and defn['nullable'] is False: # pragma: no cover
                    raise ValueError('{} not nullable'.format(defn))
            return lst
        elif isinstance(fields, Iterable):
            if len(fields) == 0 and self._back.all_nullable:
                return [None] * len(self._back.fields)
            elif len(fields) != len(self._back.fields): # pragma: no cover
                raise ValueError('{} fields provided instead of {}'.format(
                    len(fields), len(self._back.fields),
                ))
            else:
                return [
                    norm(val) if val is not None else None
                    for (norm, val) in zip(self._back.type_of_field_index, fields)
                ]
        else: # pragma: no cover
            raise TypeError('Bad fields type')

class ABackStoredVector(ABackStored, ABackSourceVector):
    """Implementation of AStoredRaster's specifications"""

    def __init__(self, **kwargs):
        super(ABackStoredVector, self).__init__(**kwargs)

    def insert_data(self, geom, geom_type, fields, index): # pragma: no cover
        raise NotImplementedError('ABackStoredVector.insert_data is virtual pure')
