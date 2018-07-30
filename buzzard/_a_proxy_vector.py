from buzzard._a_proxy import *
from buzzard import _tools

class AProxyVector(AProxy):

    @property
    def type(self):
        """Geometry type"""
        return self._back.type

    @property
    def fields(self):
        """Fields definition"""
        return [dict(d) for d in self._back.fields]

    def __len__(self):
        """Return the number of features in vector"""
        return len(self._back)

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
        >>> minx, miny, maxx, maxy = df.roofs.extent
        """
        return self._back.bounds

    @property
    def bounds_stored(self):
        """Get the vector's bounds in stored spatial reference. (`min` then `max`)"""
        return self._back.bounds_stored

    def iter_data(self, fields=-1, geom_type='shapely',
                  mask=None, clip=False, slicing=slice(0, None, 1)):
        """Create an iterator over vector's features

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
        if geom_type not in ['shapely', 'coordinates']:
            raise ValueError('Bad parameter `geom_type`')

        # Normalize and check mask parameter
        mask_poly, mask_rect = self._normalize_mask_parameter(mask)
        del mask

        # Normalize and check clip parameter
        clip = bool(clip)
        if mask is None and clip is True:
            raise ValueError('`clip` is True but `mask` is None')

        # Normalize and check slicing parameter
        if not isinstance(slicing, slice):
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
        """Fetch a single feature in vector. See AProxyVector.iter_data"""
        index = int(index)
        for val in self.iter_data(fields, geom_type, mask, clip, slice(index, index + 1, 1)):
            return val
        else:
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
        # Normalize and check mask parameter
        mask_poly, mask_rect = self._normalize_mask_parameter(mask)
        del mask

        # Normalize and check clip parameter
        clip = bool(clip)
        if mask is None and clip is True:
            raise ValueError('`clip` is True but `mask` is None')

        # Normalize and check slicing parameter
        if not isinstance(slicing, slice):
            raise TypeError('`slicing` of type `{}` instead of `slice'.format(
                type(slicing),
            ))

        for data in self._back.iter_data('geojson', range(len(self.fields)), slicing,
                                         mask_poly, mask_rect, clip):
            yield {
                'type': 'Feature',
                'properties': collections.OrderedDict(
                    (field['name'], value)
                    for field, value in zip(self.fields, data[1:])
                ),
                'geometry':  data[0],
            }

    def get_geojson(self, index, mask=None, clip=False):
        """Fetch a single feature in vector. See AProxyVector.iter_geojson"""
        index = int(index)
        for val in self.iter_geojson(fields, geom_type, mask, clip, slice(index, index + 1, 1)):
            return val
        else:
            raise IndexError('Feature `{}` not found'.format(index))

class ABackProxyVector(ABackProxy):

    def __init__(self, type, fields, **kwargs):
        super(ABackProxyVector, self).__init__(**kwargs)
        self.type = type
        self.fields = fields

    def __len__(self):
        raise NotImplementedError('ABackProxyVector.__len__ is virtual pure')

    @property
    def extent(self):
        raise NotImplementedError('ABackProxyVector.extent is virtual pure')

    @property
    def extent_stored(self):
        raise NotImplementedError('ABackProxyVector.extent_stored is virtual pure')

    @property
    def bounds(self):
        raise NotImplementedError('ABackProxyVector.bounds is virtual pure')

    @property
    def bounds_stored(self):
        raise NotImplementedError('ABackProxyVector.bounds_stored is virtual pure')

    def iter_data(self, geom_type, field_indices, slicing, mask_poly, mask_rect, clip):
        raise NotImplementedError('ABackProxyVector.iter_data is virtual pure')
