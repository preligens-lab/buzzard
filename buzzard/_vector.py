""">>> help(Vector)"""

# pylint: disable=too-many-lines

from __future__ import division, print_function
import numbers
import collections
import threading
import ntpath
import functools

import numpy as np
from osgeo import gdal, osr, ogr
import shapely.geometry as sg

from buzzard._proxy import Proxy
from buzzard._tools import conv
from buzzard._vector_utils import VectorUtilsMixin
from buzzard._vector_getset_data import VectorGetSetMixin
from buzzard._env import Env
from buzzard import _tools

class Vector(Proxy, VectorUtilsMixin, VectorGetSetMixin):
    """Proxy to a vector file registered in a DataSource"""

    class _Constants(Proxy._Constants):
        """See Proxy._Constants"""

        def __init__(self, ds, **kwargs):
            # Opening informations
            self.mode = kwargs.pop('mode')
            self.open_options = kwargs.pop('open_options')
            self.layer = kwargs.pop('layer')

            # GDAL informations
            if 'gdal_ds' in kwargs:
                gdal_ds = kwargs.pop('gdal_ds')
                lyr = kwargs.pop('lyr')
                kwargs['path'] = gdal_ds.GetDescription()
                kwargs['driver'] = gdal_ds.GetDriver().ShortName
                kwargs['type'] = conv.str_of_wkbgeom(lyr.GetGeomType())
                kwargs['fields'] = Vector._fields_of_lyr(lyr)
                wkt = lyr.GetSpatialRef()
                if wkt is not None:
                    wkt = wkt.ExportToWkt()
                kwargs['wkt'] = wkt
            self.path = kwargs.pop('path')
            self.driver = kwargs.pop('driver')
            self.type = kwargs.pop('type')
            self.fields = kwargs.pop('fields')

            super(Vector._Constants, self).__init__(ds, **kwargs)

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
                raise Exception('Could not create gdal dataset (%s)' % gdal.GetLastErrorMsg())

        if sr is not None:
            sr = osr.SpatialReference(osr.GetUserInputAsWKT(sr))

        geometry = conv.wkbgeom_of_str(geometry)
        lyr = gdal_ds.CreateLayer(layer, sr, geometry, options)

        if lyr is None:
            raise Exception('Could not create layer (%s)' % gdal.GetLastErrorMsg())

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

    @classmethod
    def _open_file(cls, path, layer, driver, options, mode):
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
                path, driver, gdal.GetLastErrorMsg()
            ))
        if layer is None:
            layer = 0
        if isinstance(layer, numbers.Integral):
            lyr = gdal_ds.GetLayer(layer)
        else:
            lyr = gdal_ds.GetLayerByName(layer)
        if lyr is None:
            raise Exception('Could not open layer (gdal error: %s)' % gdal.GetLastErrorMsg())
        return gdal_ds, lyr

    def __init__(self, ds, consts, gdal_ds=None, lyr=None):
        """Instanciated by DataSource class, instanciation by user is undefined"""
        rect = None
        if lyr is not None:
            rect = lyr.GetExtent()
        Proxy.__init__(self, ds, consts, rect)

        self._gdal_ds = gdal_ds
        self._lyr = lyr

        self._index_of_field_name = {
            field['name']: i
            for i, field in enumerate(self._c.fields)
        }
        self._type_of_field_index = [
            conv.type_of_oftstr(field['type'])
            for field in self._c.fields
        ]
        self._all_nullable = all(field['nullable'] for field in self._c.fields)
        if ds._ogr_layer_lock == 'none':
            self._lock = None
        else:
            self._lock = threading.Lock()

    # Life control ****************************************************************************** **
    @property
    def close(self):
        """Close a vector source with a call or a context management.

        Example
        -------
        >>> ds.roofs.close()
        >>> with ds.roofs.close:
                # code...
        >>> with ds.create_avector('./results.shp', 'linestring').close as roofs:
                # code...
        """
        def _close():
            if self._ds._is_locked_activate(self):
                raise RuntimeError('Attempting to close a `buzz.Vector` while an read operation is in progress')

            self._ds._unregister(self)
            self.deactivate()
            del self._lyr
            del self._gdal_ds
            del self._ds

        return _VectorCloseRoutine(self, _close)

    @property
    def delete(self):
        """Delete vector file with a call or a context management.

        Example
        -------
        >>> ds.polygons.delete()
        >>> with ds.polygons.delete:
                # code...
        >>> with ds.create_avector('/tmp/tmp.shp', 'polygon').delete as tmp:
                # code...
        """
        if self._c.mode != 'w':
            raise RuntimeError('Cannot remove a read-only file')

        def _delete():
            if self._ds._is_locked_activate(self):
                raise RuntimeError('Attempting to delete a `buzz.Vector` while an read operation is in progress')

            path = self.path
            dr = gdal.GetDriverByName(self._c.driver)

            self._ds._unregister(self)
            self.deactivate()
            del self._lyr
            del self._gdal_ds
            del self._ds

            err = dr.Delete(path)
            if err:
                raise RuntimeError('Could not delete `{}` (gdal error: `{}`)'.format(
                    path, gdal.GetLastErrorMsg()
                ))

        return _VectorDeleteRoutine(self, _delete)

    @property
    def delete_layer(self):
        """Delete vector layer with a call or a context management."""
        if self._c.mode != 'w':
            raise RuntimeError('Cannot remove a read-only layer')

        def _delete_layer():
            if self._ds._is_locked_activate(self):
                raise RuntimeError('Attempting to delete a `buzz.Vector` layer while an read operation is in progress')

            self.activate()
            lyr_name = self._lyr.GetDescription()
            # Lose the reference to the gdal layer but keep the variable `_lyr` alive until deletion
            # below.
            self._lyr = 42

            err = self._gdal_ds.DeleteLayer(lyr_name)
            if err:
                raise RuntimeError('Could not delete layer `{}` (gdal error: `{}`)'.format(
                    lyr_name, gdal.GetLastErrorMsg()
                ))

            self._ds._unregister(self)
            self.deactivate()
            del self._lyr
            del self._gdal_ds
            del self._ds

        return _VectorDeleteLayerRoutine(self, _delete_layer)

    # Activation mechanisms ********************************************************************* **
    @property
    @functools.wraps(Proxy.deactivable.fget)
    def deactivable(self):
        """See buzz.Proxy.deactivable"""
        v = self.driver != 'Memory'
        v &= super(Vector, self).deactivable
        return v

    @property
    @functools.wraps(Proxy.picklable.fget)
    def picklable(self):
        """See buzz.Proxy.picklable"""
        return self.deactivable

    @property
    @functools.wraps(Proxy.activated.fget)
    def activated(self):
        """See buzz.Proxy.activated"""
        assert (self._lyr is None) == (self._gdal_ds is None)
        return self._gdal_ds is not None

    def _activate(self):
        """See buzz.Proxy._activate"""
        assert self.deactivable
        assert self._gdal_ds is None
        gdal_ds, lyr = self._open_file(
            self.path, self._c.layer, self.driver, self.open_options, self.mode
        )

        # Check that self._c hasn't changed
        consts_check = Vector._Constants(
            self._ds, gdal_ds=gdal_ds, lyr=lyr, open_options=self.open_options, mode=self.mode, layer=self._c.layer,
        )
        new = consts_check.__dict__
        old = self._c.__dict__
        for k in new.keys():
            oldv = old[k]
            newv = new[k]
            if oldv != newv:
                raise RuntimeError("Vector's `{}` changed between deactivation and activation!\nold: `{}`\nnew: `{}` ".format(
                    k, oldv, newv
                ))
        self._gdal_ds = gdal_ds
        self._lyr = lyr

    def _deactivate(self):
        """See buzz.Proxy._deactivate"""
        assert self.deactivable
        assert self._gdal_ds is not None
        self._gdal_ds, self._lyr = None, None

    # Properties ******************************************************************************** **
    @property
    def mode(self):
        """Get raster open mode"""
        return self._c.mode

    @_tools.ensure_activated
    def __len__(self):
        """Return the number of features in vector layer"""
        return len(self._lyr)

    @property
    def fields(self):
        """Fields definition"""
        return [dict(d) for d in self._c.fields]

    @property
    def type(self):
        """Geometry type"""
        return self._c.type

    @property
    @_tools.ensure_activated
    def extent(self):
        """Get file's extent. (`x` then `y`)

        Example
        -------
        >>> minx, maxx, miny, maxy = ds.roofs.extent
        """
        extent = self._lyr.GetExtent()
        if extent is None:
            raise ValueError('Could not compute extent')
        if self._to_work:
            xa, xb, ya, yb = extent
            extent = self._to_work([[xa, ya], [xb, yb]])
            extent = np.asarray(extent)[:, :2]
            extent = extent[0, 0], extent[1, 0], extent[0, 1], extent[1, 1]
        return np.asarray(extent)

    @property
    @_tools.ensure_activated
    def bounds(self):
        """Get the file's bounds (`min` then `max`)

        Example
        -------
        >>> minx, miny, maxx, maxy = df.roofs.extent
        """
        extent = self.extent
        return np.asarray([extent[0], extent[2], extent[1], extent[3]])

    @property
    @_tools.ensure_activated
    def extent_origin(self):
        """Get file's extent in origin spatial reference. (minx, miny, maxx, maxy)"""
        extent = self._lyr.GetExtent()
        if extent is None:
            raise ValueError('Could not compute extent')
        return extent

    @property
    def path(self):
        """Get vector file path"""
        return self._c.path

    @property
    def open_options(self):
        """Get vector open options"""
        return self._c.open_options

    @property
    def driver(self):
        """Get the GDAL driver name"""
        return self._c.driver

    # Vector read operations ******************************************************************** **
    @_tools.ensure_activated_iteration
    def iter_data(self, fields=-1, geom_type='shapely',
                  mask=None, clip=False, slicing=slice(0, None, 1)):
        """Create an iterator over file's features

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
        mask: None or Footprint or shapely polygon or (nbr, nbr, nbr, nbr)
            Add a spatial filter to iteration, only geometries intersecting (not disjoint) with mask
            will be included.

            if None: No spatial filter
            if Footprint or shapely polygon: Polygon
            if (nbr, nbr, nbr, nbr): Extent (minx, maxx, miny, maxy)
        clip: bool
            Returns intersection of geometries and mask.
            Caveat: A polygon in a file might be converted to one of point, line, multiline,
                  multipoint, geometrycollection.
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
        # Parameter - fields
        field_indices = list(self._iter_user_intput_field_keys(fields))
        # Parameter - geom_type
        if geom_type not in ['shapely', 'coordinates']:
            raise ValueError('Bad parameter `geom_type`')
        # Parameter - slicing
        if not isinstance(slicing, slice):
            raise TypeError('`slicing` of type `{}` instead of `slice'.format(
                type(slicing),
            ))
        # Parameter - clip
        clip = bool(clip)
        if mask is None and clip is True:
            raise ValueError('`clip` is True but `mask` is None')
        # Parameter - mask
        mask_poly, mask_rect = self._normalize_mask_parameter(mask)
        del mask

        for data in self._iter_data_unsafe(geom_type, field_indices, slicing,
                                           mask_poly, mask_rect, clip):
            if len(field_indices) == 0:
                yield data[0]
            else:
                yield data

    def get_data(self, index, fields=-1, geom_type='shapely', mask=None, clip=False):
        """Fetch a single feature in file

        Parameters
        ----------
        index: int
        fields: None or string or -1 or sequence of string/int
            which fields to include in iteration

            if None or empty sequence: No fields included
            if -1: All fields included
            if string: Name of fields to include (separated by comma or space)
            if sequence: List of indices / names to include
        geom_type: {'shapely', 'coordinates'}
            returned geometry type
        mask: None or Footprint or shapely polygon or (nbr, nbr, nbr, nbr)
            Add a spatial filter to iteration, only geometries intersecting (not disjoint) with mask
            will be included.

            if None: No spatial filter
            if Footprint or shapely polygon: Polygon
            if (nbr, nbr, nbr, nbr): Extent (minx, maxx, miny, maxy)
        clip: bool
            Returns intersection of geometries and mask.
            Caveat: A polygon in a file might be converted to one of point, line, multiline,
                  multipoint, geometrycollection.
        slicing: slice
            Slice of the iteration to return. It is applied after spatial filtering

        Returns
        -------
        value:
        | geom_type     | fields | value type                            |
        |---------------|--------|---------------------------------------|
        | 'shapely'     | None   | shapely object                        |
        | 'coordinates' | None   | nested list / numpy arrays            |
        | 'shapely'     | Some   | (shapely object, *fields)             |
        | 'coordinates' | Some   | (nested list / numpy arrays, *fields) |

        """
        index = int(index)
        for val in self.iter_data(fields, geom_type, mask, clip, slice(index, index + 1, 1)):
            return val
        else:
            raise IndexError('Feature `{}` not found'.format(index))

    @_tools.ensure_activated_iteration
    def iter_geojson(self, mask=None, clip=False, slicing=slice(0, None, 1)):
        """Create an iterator over file's data

        Parameters
        ----------
        mask: None or Footprint or shapely polygon or (nbr, nbr, nbr, nbr)
            Add a spatial filter to iteration, only geometries intersecting (not disjoint) with mask
            will be included.

            if None: No spatial filter
            if Footprint or shapely polygon: Polygon
            if (nbr, nbr, nbr, nbr): Extent (minx, maxx, miny, maxy)
        clip: bool
            Returns intersection of geometries and mask.
            Caveat: A polygon in a file might be converted to one of point, line, multiline,
                  multipoint, geometrycollection.
        slicing: slice
            Slice of the iteration to return. It is applyied after spatial filtering

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
        # Parameter - slicing
        if not isinstance(slicing, slice):
            raise TypeError('`slicing` of type `{}` instead of `slice'.format(
                type(slicing),
            ))
        # Parameter - clip
        clip = bool(clip)
        if mask is None and clip is True:
            raise ValueError('`clip` is True but `mask` is None')
        # Parameter - mask
        mask_poly, mask_rect = self._normalize_mask_parameter(mask)
        del mask
        for data in self._iter_data_unsafe('geojson', range(len(self._c.fields)), slicing,
                                           mask_poly, mask_rect, clip):
            yield {
                'type': 'Feature',
                'properties': collections.OrderedDict(
                    (field['name'], value)
                    for field, value in zip(self._c.fields, data[1:])
                ),
                'geometry':  data[0],
            }

    def get_geojson(self, index, mask=None, clip=False):
        """Fetch a single feature in file

        Parameters
        ----------
        index: int
        mask: None or Footprint or shapely polygon or (nbr, nbr, nbr, nbr)
            Add a spatial filter to iteration, only geometries intersecting (not disjoint) with mask
            will be included.

            if None: No spatial filter
            if Footprint or shapely polygon: Polygon
            if (nbr, nbr, nbr, nbr): Extent (minx, maxx, miny, maxy)
        clip: bool
            Returns intersection of geometries and mask.
            Caveat: A polygon in a file might be converted to one of point, line, multiline,
                  multipoint, geometrycollection.

        Returns
        -------
        geojson feature (dict)

        """
        index = int(index)
        for val in self.iter_geojson(mask, clip, slice(index, index + 1, 1)):
            return val
        else:
            raise IndexError('Feature `{}` not found'.format(index))

    # Vector write operations ******************************************************************* **
    @_tools.ensure_activated
    def insert_data(self, geom, fields=(), index=-1):
        """Insert a feature in file

        Parameters
        ----------
        geom: shapely.base.BaseGeometry or nested sequence of coordinates
        fields: sequence or dict
            Feature's fields
            Missing or None fields are defaulted

            if empty sequence: Keep all fields defaulted
            if sequence of length len(self.fields): Fields to be set, same order as self.fields
            if dict: Mapping of fields to be set
        index: int
            if -1: append feature
            else: insert feature at index (if supported by driver)

        Example
        -------
        >>> poly = shapely.geometry.box(10, 10, 42, 43)
        >>> fields = {'volume': 42.24}
        >>> ds.stocks.insert_data(poly, fields)

        """
        if geom is None:
            if not self._ds._allow_none_geometry:
                raise TypeError(
                    'Inserting None geometry not allowed '
                    '(allow None geometry in DataSource constructor to silence)'
                )
            geom_type = None
        if isinstance(geom, sg.base.BaseGeometry):
            geom_type = 'shapely'
        elif isinstance(geom, collections.Iterable):
            geom_type = 'coordinates'
        else:
            raise TypeError('input `geom` should be a shapely object or coordinates, not {}'.format(
                type(geom)
            ))
        fields = self._normalize_field_values(fields)
        self._insert_data_unsafe(geom_type, geom, fields, index)

    # The end *********************************************************************************** **
    # ******************************************************************************************* **

_VectorCloseRoutine = type('_VectorCloseRoutine', (_tools.CallOrContext,), {
    '__doc__': Vector.close.__doc__,
})

_VectorDeleteRoutine = type('_VectorDeleteRoutine', (_tools.CallOrContext,), {
    '__doc__': Vector.delete.__doc__,
})

_VectorDeleteLayerRoutine = type('_VectorDeleteLayerRoutine', (_tools.CallOrContext,), {
    '__doc__': Vector.delete_layer.__doc__,
})
