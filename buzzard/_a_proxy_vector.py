import collections
import numbers
import sys

import shapely.geometry as sg
import numpy as np

from buzzard._a_proxy import AProxy, ABackProxy
from buzzard import _tools
from buzzard._footprint import Footprint
from buzzard._tools import conv

class AProxyVector(AProxy):
    """Base abstract class defining the common behavior of all vectors"""

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

        This property is thread-safe (Unless you are using the GDAL::Memory driver).

        Example
        -------
        >>> minx, maxx, miny, maxy = ds.roofs.extent
        """
        return self._back.extent

    @property
    def extent_stored(self):
        """Get the vector's extent in stored spatial reference. (minx, miny, maxx, maxy)

        This property is thread-safe (Unless you are using the GDAL::Memory driver).
        """
        return self._back.extent_stored

    @property
    def bounds(self):
        """Get the vector's bounds in work spatial reference. (`min` then `max`)

        This property is thread-safe (Unless you are using the GDAL::Memory driver).

        Example
        -------
        >>> minx, miny, maxx, maxy = ds.roofs.extent
        """
        return self._back.bounds

    @property
    def bounds_stored(self):
        """Get the vector's bounds in stored spatial reference. (`min` then `max`)

        This property is thread-safe (Unless you are using the GDAL::Memory driver).

        """
        return self._back.bounds_stored

    def __len__(self):
        """Return the number of features in vector

        This property is thread-safe (Unless you are using the GDAL::Memory driver).

        """
        return len(self._back)

    def iter_data(self, fields=-1, geom_type='shapely',
                  mask=None, clip=False, slicing=slice(0, None, 1)):
        """Create an iterator over vector's features

        This method is thread-safe (Unless you are using the GDAL::Memory driver). Iteration is
        thread-safe too.

        Parameters
        ----------
        fields: None or string or -1 or sequence of string/int
            Which fields to include in iteration

            if None or empty sequence: No fields included
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

        Returns
        -------
        iterable of value:

        | geom_type     | fields | value type                            |
        |---------------|--------|---------------------------------------|
        | 'shapely'     | None   | shapely object                        |
        | 'coordinates' | None   | nested list / numpy arrays            |
        | 'shapely'     | Some   | (shapely object, *fields)             |
        | 'coordinates' | Some   | (nested list / numpy arrays, *fields) |

        Example
        -------
        >>> for polygon, volume, stock_type in ds.stocks.iter_data('volume,type'):
                print('area:{}m**2, volume:{}m**3'.format(polygon.area, volume))

        """
        # Normalize and check fields parameter
        field_indices = list(self._iter_user_intput_field_keys(fields))
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
            if len(field_indices) == 0:
                yield data[0]
            else:
                yield data

    def get_data(self, index, fields=-1, geom_type='shapely', mask=None, clip=False):
        """Fetch a single feature in vector. See AProxyVector.iter_data

        This method is thread-safe (Unless you are using the GDAL::Memory driver).

        """
        index = int(index)
        for val in self.iter_data(fields, geom_type, mask, clip, slice(index, index + 1, 1)):
            return val
        else: # pragma: no cover
            raise IndexError('Feature `{}` not found'.format(index))

    def iter_geojson(self, mask=None, clip=False, slicing=slice(0, None, 1)):
        """Create an iterator over vector's features

        This method is thread-safe (Unless you are using the GDAL::Memory driver). Iteration is
        thread-safe too.

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
        """Fetch a single feature in vector. See AProxyVector.iter_geojson

        This method is thread-safe (Unless you are using the GDAL::Memory driver).

        """
        index = int(index)
        for val in self.iter_geojson(mask, clip, slice(index, index + 1, 1)):
            return val
        else: # pragma: no cover
            raise IndexError('Feature `{}` not found'.format(index))

    def _iter_user_intput_field_keys(self, keys):
        """Used on features reading"""
        if keys == -1:
            for i in range(len(self._back.fields)):
                yield i
        elif isinstance(keys, str):
            for str_ in keys.replace(' ', ',').split(','):
                if str_ != '':
                    yield self._back.index_of_field_name[str_]
        elif keys is None:
            return
        elif isinstance(keys, collections.Iterable):
            for val in keys:
                if isinstance(val, numbers.Number):
                    val = int(val)
                    if val >= len(self._back.fields): # pragma: no cover
                        raise ValueError('Out of bound %d' % val)
                    yield val
                elif isinstance(val, str):
                    yield self._back.index_of_field_name[val]
                else: # pragma: no cover
                    raise TypeError('bad type in `fields`')
        else: # pragma: no cover
            raise TypeError('bad `fields` type')

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

class ABackProxyVector(ABackProxy):
    """Implementation of AProxyVector's specifications"""

    def __init__(self, type, fields, **kwargs):
        super(ABackProxyVector, self).__init__(**kwargs)
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
        raise NotImplementedError('ABackProxyVector.extent is virtual pure')

    @property # pragma: no cover
    def extent_stored(self):
        raise NotImplementedError('ABackProxyVector.extent_stored is virtual pure')

    @property
    def bounds(self):
        extent = self.extent
        return np.asarray([extent[0], extent[2], extent[1], extent[3]])

    @property
    def bounds_stored(self):
        extent = self.extent_stored
        return np.asarray([extent[0], extent[2], extent[1], extent[3]])

    def __len__(self): # pragma: no cover
        raise NotImplementedError('ABackProxyVector.__len__ is virtual pure')

    def iter_data(self, geom_type, field_indices, slicing, mask_poly, mask_rect, clip): # pragma: no cover
        raise NotImplementedError('ABackProxyVector.iter_data is virtual pure')

if sys.version_info < (3, 6):
    # https://www.python.org/dev/peps/pep-0487/
    for k, v in AProxyVector.__dict__.items():
        if hasattr(v, '__set_name__'):
            v.__set_name__(AProxyVector, k)
