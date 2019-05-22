import collections
import sys

import shapely.geometry as sg
import numpy as np

from buzzard._a_source import ASource, ABackSource
from buzzard import _tools
from buzzard._footprint import Footprint
from buzzard._tools import conv

class ASourceVector(ASource):
    """Base abstract class defining the common behavior of all vectors.

    Features Defined
    ----------------
    - Has a `type` that defines the type of geometry (like "Polygon")
    - Has `fields` that define the type of informations that is paired with each geometries
    - Has a `stored` extent that allows to retrieve the current extent of all the geometries
    - Has a length that indicates how many geometries this source contains.
    - Has several read functions (like `iter_data`) to retrieve geometries in their current state to
        shapely objects
    """

    @property
    def type(self):
        """Geometry type"""
        return self._back.type

    @property
    def fields(self):
        """Fields definition"""
        return [dict(d) for d in self._back.fields]

    @property
    def extent(self):
        """Get the vector's extent in work spatial reference. (`x` then `y`)

        Example
        -------
        >>> minx, maxx, miny, maxy = ds.roofs.extent
        """
        return self._back.extent

    @property
    def extent_stored(self):
        """Get the vector's extent in stored spatial reference. (minx, miny, maxx, maxy)"""
        return self._back.extent_stored

    @property
    def bounds(self):
        """Get the vector's bounds in work spatial reference. (`min` then `max`)

        Example
        -------
        >>> minx, miny, maxx, maxy = ds.roofs.extent
        """
        return self._back.bounds

    @property
    def bounds_stored(self):
        """Get the vector's bounds in stored spatial reference. (`min` then `max`)"""
        return self._back.bounds_stored

    def __len__(self):
        """Return the number of features in vector"""
        return len(self._back)

    def iter_data(self, fields=None, geom_type='shapely',
                  mask=None, clip=False, slicing=slice(0, None, 1)):
        """Create an iterator over vector's features

        Parameters
        ----------
        fields: None or string or -1 or sequence of string/int
            Which fields to include in iteration

            if None, empty sequence or empty string: No fields included
            if -1: All fields included
            if string: Name of fields to include (separated by comma or space)
            if sequence: List of indices / names to include
        geom_type: {'shapely', 'coordinates'}
            Returned geometry type
        mask: None or Footprint or shapely geometry or (nbr, nbr, nbr, nbr)
            Add a spatial filter to iteration, only geometries not disjoint with mask will be
            included.

            if None: No spatial filter
            if Footprint or shapely polygon: Polygon
            if (nbr, nbr, nbr, nbr): Extent (minx, maxx, miny, maxy)
        clip: bool
            Returns intersection of geometries and mask.
            Caveat: A clipped geometry might not be of the same type as the original geometry.
            e.g: polygon might be clipped to might be converted to one of those:
            - polygon
            - line
            - point
            - multipolygon
            - multiline
            - multipoint
            - geometrycollection
        slicing: slice
            Slice of the iteration to return. It is applied after spatial filtering

        Yields
        ------
        feature: geometry or (geometry,) or (geometry, *fields)
            If `geom_type` is 'shapely', geometry is a `shapely geometry`.
            If `geom_type` is `coordinates`, geometry is a `nested lists of numpy arrays`.

            If `fields` is not a sequence, `feature` is `geometry` or `(geometry, *fields)`,
                 depending on the number of fields to yield.
            If `fields` is a sequence or a string, `feature` is `(geometry,)` or
                `(geometry, *fields)`. Use `fields=[-1]` to get a monad containing all fields.

        Examples
        --------
        >>> for polygon, volume, stock_type in ds.stocks.iter_data('volume,type'):
                print('area:{}m**2, volume:{}m**3'.format(polygon.area, volume))

        >>> for polygon, in ds.stocks.iter_data([]):
                print('area:{}m**2'.format(polygon.area))

        >>> for polygon in ds.stocks.iter_data():
                print('area:{}m**2'.format(polygon.area))

        """
        # Normalize and check fields parameter
        field_indices, is_flat = _tools.normalize_fields_parameter(
            fields, self._back.index_of_field_name
        )
        del fields

        # Normalize and check geom_type parameter
        if geom_type not in ['shapely', 'coordinates']: # pragma: no cover
            raise ValueError('Bad parameter `geom_type`')

        # Normalize and check clip parameter
        clip = bool(clip)
        if mask is None and clip is True: # pragma: no cover
            raise ValueError('`clip` is True but `mask` is None')

        # Normalize and check mask parameter
        # TODO: Convert to_work
        mask_poly, mask_rect = self._normalize_mask_parameter(mask)
        del mask

        # Normalize and check slicing parameter
        if not isinstance(slicing, slice): # pragma: no cover
            raise TypeError('`slicing` of type `{}` instead of `slice'.format(
                type(slicing),
            ))

        for data in self._back.iter_data(geom_type, field_indices, slicing,
                                         mask_poly, mask_rect, clip):
            if is_flat:
                assert len(data) == 1, len(data)
                yield data[0]
            else:
                yield data

    def get_data(self, index, fields=-1, geom_type='shapely', mask=None, clip=False):
        """Fetch a single feature in vector. See ASourceVector.iter_data"""
        index = int(index)
        for val in self.iter_data(fields, geom_type, mask, clip, slice(index, index + 1, 1)):
            return val
        else: # pragma: no cover
            raise IndexError('Feature `{}` not found'.format(index))

    def iter_geojson(self, mask=None, clip=False, slicing=slice(0, None, 1)):
        """Create an iterator over vector's features

        Parameters
        ----------
        mask: None or Footprint or shapely geometry or (nbr, nbr, nbr, nbr)
            Add a spatial filter to iteration, only geometries not disjoint with mask will be
            included.

            if None: No spatial filter
            if Footprint or shapely polygon: Polygon
            if (nbr, nbr, nbr, nbr): Extent (minx, maxx, miny, maxy)
        clip: bool
            Returns intersection of geometries and mask.
            Caveat: A clipped geometry might not be of the same type as the original geometry.
            e.g: polygon might be clipped to might be converted to one of those:
            - polygon
            - line
            - point
            - multipolygon
            - multiline
            - multipoint
            - geometrycollection
        slicing: slice
            Slice of the iteration to return. It is applied after spatial filtering

        Returns
        -------
        iterable of geojson feature (dict)


        Example
        -------
        >>> for geojson in ds.stocks.iter_geojson():
                print('exterior-point-count:{}, volume:{}m**3'.format(
                    len(geojson['geometry']['coordinates'][0]),
                    geojson['properties']['volume']
                ))
        """
        # Normalize and check clip parameter
        clip = bool(clip)
        if mask is None and clip is True: # pragma: no cover
            raise ValueError('`clip` is True but `mask` is None')

        # Normalize and check mask parameter
        mask_poly, mask_rect = self._normalize_mask_parameter(mask)
        del mask

        # Normalize and check slicing parameter
        if not isinstance(slicing, slice): # pragma: no cover
            raise TypeError('`slicing` of type `{}` instead of `slice'.format(
                type(slicing),
            ))

        gen = self._back.iter_data(
            'geojson',
            list(range(len(self.fields))),
            slicing,
            mask_poly,
            mask_rect,
            clip,
        )
        for data in gen:
            yield {
                'type': 'Feature',
                'properties': collections.OrderedDict(
                    (field['name'], value)
                    for field, value in zip(self.fields, data[1:])
                ),
                'geometry':  data[0],
            }

    def get_geojson(self, index, mask=None, clip=False):
        """Fetch a single feature in vector. See ASourceVector.iter_geojson"""
        index = int(index)
        for val in self.iter_geojson(mask, clip, slice(index, index + 1, 1)):
            return val
        else: # pragma: no cover
            raise IndexError('Feature `{}` not found'.format(index))

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
        else: # pragma: no cover
            raise TypeError('`mask` should be a Footprint, an extent or a shapely object')

    # Deprecation
    extent_origin = _tools.deprecation_pool.wrap_property(
        'extent_stored',
        '0.4.4'
    )

class ABackSourceVector(ABackSource):
    """Implementation of ASourceVector's specifications"""

    def __init__(self, type, fields, **kwargs):
        super(ABackSourceVector, self).__init__(**kwargs)
        self.type = type
        self.fields = fields
        self.index_of_field_name = {
            field['name']: i
            for i, field in enumerate(self.fields)
        }
        self.type_of_field_index = [
            conv.type_of_oftstr(field['type'])
            for field in self.fields
        ]
        self.all_nullable = all(field['nullable'] for field in self.fields)

    @property
    def extent(self): # pragma: no cover
        raise NotImplementedError('ABackSourceVector.extent is virtual pure')

    @property # pragma: no cover
    def extent_stored(self):
        raise NotImplementedError('ABackSourceVector.extent_stored is virtual pure')

    @property
    def bounds(self):
        extent = self.extent
        return np.asarray([extent[0], extent[2], extent[1], extent[3]])

    @property
    def bounds_stored(self):
        extent = self.extent_stored
        return np.asarray([extent[0], extent[2], extent[1], extent[3]])

    def __len__(self): # pragma: no cover
        raise NotImplementedError('ABackSourceVector.__len__ is virtual pure')

    def iter_data(self, geom_type, field_indices, slicing, mask_poly, mask_rect, clip): # pragma: no cover
        raise NotImplementedError('ABackSourceVector.iter_data is virtual pure')

if sys.version_info < (3, 6):
    # https://www.python.org/dev/peps/pep-0487/
    for k, v in ASourceVector.__dict__.items():
        if hasattr(v, '__set_name__'):
            v.__set_name__(ASourceVector, k)
