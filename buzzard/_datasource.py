""">>> help(buzz.DataSource)"""

# pylint: disable=too-many-lines

import collections

from osgeo import osr

from buzzard import _datasource_tools
from buzzard._proxy import Proxy
from buzzard._raster_physical import RasterPhysical
from buzzard._raster_recipe import RasterRecipe
from buzzard._vector import Vector
from buzzard._datasource_conversions import DataSourceConversionsMixin

class DataSource(_datasource_tools.DataSourceToolsMixin, DataSourceConversionsMixin):
    """DataSource is a class that stores references to files, it allows quick manipulations
    by assigning a key to each registered files.

    For actions specific to opened files, see Raster, RasterPhysical and VectorProxy classes

    Example
    -------
    >>> import buzzard as buzz

    Opening
    >>> ds = buzz.DataSource()
    >>> ds.open_vector('roofs', 'path/to/roofs.shp')
    >>> ds.open_raster('dem', 'path/to/dem.tif')

    Opening with context management
    >>> with ds.open_raster('rgb', 'path/to/rgb.tif').close:
    ...     print(ds.rgb.fp)
    ...     arr = ds.rgb.get_data()
    >>> with ds.open_araster('path/to/rgb.tif').close as rgb:
    ...     print(rgb.fp)
    ...     arr = rgb.get_data()

    Creation
    >>> ds.create_vector('targets', 'path/to/targets.geojson', 'point', driver='GeoJSON')
    >>> with ds.create_araster('/tmp/cache.tif', ds.dem.fp, 'float32', 1).delete as cache:
    ...      cache.set_data(dem.get_data())

    """

    def __init__(self, sr_work=None, sr_implicit=None, sr_origin=None,
                 analyse_transformation=True,
                 ogr_layer_lock='raise',
                 allow_none_geometry=False,
                 allow_interpolation=False):
        """Constructor

        Parameters
        ----------
        sr_work: None or string (see `Coordinates conversions` below)
        sr_implicit: None or string (see `Coordinates conversions` below)
        sr_origin: None or string (see `Coordinates conversions` below)
        analyse_transformation: bool
            Whether or not to perform a basic analysis on two sr to check their compatibilty
        ogr_layer_lock: one of ('none', 'wait', 'raise')
            Mutex operations when reading or writing vector files
        allow_none_geometry: bool
        allow_interpolation: bool

        Coordinates conversions
        -----------------------
        A DataSource may perform coordinates conversions on the fly using osr by following a set of
        rules. Those conversions only include vector files, raster files Footprints and basic raster
        remapping. General raster warping is not included (yet).

        If `analyse_transformation` is set to `True` (default), all coordinates conversions are
        tested against `buzz.env.significant` on file opening to ensure their feasibility or
        raise an exception otherwise. This system is naive and very restrictive, a smarter and
        smarter version is planned. Use with caution.

        Terminology:
        `sr`: Spatial reference
        `sr_work`: Spatial reference of all interactions with a DataSource.
            (i.e. Footprints, polygons...)
        `sr_origin`: Spatial reference of data stored in a file
        `sr_implicit`: Fallback spatial reference of a file if it cannot be determined by reading it

        Parameters and modes:
        | mode | sr_work | sr_implicit | sr_origin | How is the `sr` of a file determined                                                     |
        |------|---------|-------------|-----------|------------------------------------------------------------------------------------------|
        | 1    | None    | None        | None      | Not determined (no coordinates conversion for the lifetime of this DataSource)           |
        | 2    | Some    | None        | None      | Read the `sr` of a file in its metadata. If missing raise an exception                   |
        | 3    | Some    | Some        | None      | Read the `sr` of a file in its metadata. If missing it is considered to be `sr_implicit` |
        | 4    | Some    | None        | Some      | Ignore sr read, consider all opened files to be encoded in `sr_origin`                   |

        For example if all opened files are known to be all written in a same `sr` use `mode 1`, or
        `mode 4` if you wish to work in a different `sr`.
        On the other hand, if not all files are written in the same `sr`, `mode 2` and
        `mode 3` may help, but make sure to use `buzz.Env(analyse_transformation=True)` to be
        informed on the quality of the transformations performed.

        A spatial reference parameter may be
        - A path to a file
        - A [textual spatial reference](http://gdal.org/java/org/gdal/osr/SpatialReference.html#SetFromUserInput-java.lang.String-)

        Example
        -------
        mode 1
        >>> ds = buzz.DataSource()

        mode 2
        >>> ds = buzz.DataSource(
                sr_work=buzz.srs.wkt_of_file('path/to.tif', center=True),
            )

        mode 4
        >>> ds = buzz.DataSource(
                sr_work=buzz.srs.wkt_of_file('path/to.tif', unit='meter'),
                sr_origin='path/to.tif',
            )

        """
        mode = (sr_work is not None, sr_implicit is not None, sr_origin is not None)
        if mode == (False, False, False):
            pass
        elif mode == (True, False, False):
            pass
        elif mode == (True, True, False):
            pass
        elif mode == (True, False, True):
            pass
        else:
            raise ValueError('Bad combination of `sr_*` parameters') # pragma: no cover

        if ogr_layer_lock not in frozenset({'none', 'wait', 'raise'}):
            raise ValueError('Unknown `ogr_layer_lock` value') # pragma: no cover

        allow_interpolation = bool(allow_interpolation)
        allow_none_geometry = bool(allow_none_geometry)

        if mode[0]:
            wkt_work = osr.GetUserInputAsWKT(sr_work)
            sr_work = osr.SpatialReference(wkt_work)
        else:
            wkt_work = None
            sr_work = None
        if mode[1]:
            wkt_implicit = osr.GetUserInputAsWKT(sr_implicit)
            sr_implicit = osr.SpatialReference(wkt_implicit)
        else:
            wkt_implicit = None
            sr_implicit = None
        if mode[2]:
            wkt_origin = osr.GetUserInputAsWKT(sr_origin)
            sr_origin = osr.SpatialReference(wkt_origin)
        else:
            wkt_origin = None
            sr_origin = None

        DataSourceConversionsMixin.__init__(
            self, sr_work, sr_implicit, sr_origin, analyse_transformation
        )
        _datasource_tools.DataSourceToolsMixin.__init__(self)

        self._wkt_work = wkt_work
        self._wkt_implicit = wkt_implicit
        self._wkt_origin = wkt_origin
        self._ogr_layer_lock = ogr_layer_lock
        self._allow_interpolation = allow_interpolation
        self._allow_none_geometry = allow_none_geometry

    # Raster entry points *********************************************************************** **
    def open_raster(self, key, path, driver='GTiff', options=(), mode='r'):
        """Open a raster file in this DataSource under `key`. Only metadata are kept in memory.

        Parameters
        ----------
        key: hashable (like a string)
            File identifier within DataSource
        path: string
        driver: string
            gdal driver to use when opening the file
            http://www.gdal.org/formats_list.html
        options: sequence of str
            options for gdal
        mode: one of ('r', 'w')

        Example
        -------
        >>> ds.open_raster('ortho', '/path/to/ortho.tif')
        >>> ortho = ds.open_araster('/path/to/ortho.tif')
        >>> ds.open_raster('dem', '/path/to/dem.tif', mode='w')

        """
        self._validate_key(key)
        gdal_ds = RasterPhysical._open_file(path, driver, options, mode)
        prox = RasterPhysical(self, gdal_ds, mode)
        self._register([key], prox)
        return prox

    def open_araster(self, path, driver='GTiff', options=(), mode='r'):
        """Open a raster file anonymously in this DataSource. Only metadata are kept in memory.

        See DataSource.open_raster
        """
        gdal_ds = RasterPhysical._open_file(path, driver, options, mode)
        prox = RasterPhysical(self, gdal_ds, mode)
        self._register([], prox)
        return prox

    def create_raster(self, key, path, fp, dtype, band_count, band_schema=None,
                      driver='GTiff', options=(), sr=None):
        """Create a raster file and register it under `key` in this DataSource. Only metadata are
        kept in memory.

        Parameters
        ----------
        key: hashable (like a string)
            File identifier within DataSource
        path: string
        fp: Footprint
            Description of the location and size of the raster to create.
        dtype: numpy type (or any alias)
        band_count: integer
            number of bands
        band_schema: dict or None
            Band(s) metadata. (see `Band fields` below)
        driver: string
            gdal driver to use when opening the file
            http://www.gdal.org/formats_list.html
        options: sequence of str
            options for gdal
            http://www.gdal.org/frmt_gtiff.html
        sr: string or None
            Spatial reference of the new file

            if None: don't set a spatial reference
            if string:
                if path: Use same projection as file at `path`
                if textual spatial reference:
                    http://gdal.org/java/org/gdal/osr/SpatialReference.html#SetFromUserInput-java.lang.String-

        Band fields
        -----------
        Fields:
            'nodata': None or number
            'interpretation': None or str
            'offset': None or number
            'scale': None or number
            'mask': None or one of ('')
        Interpretation values:
            undefined, grayindex, paletteindex, redband, greenband, blueband, alphaband, hueband,
            saturationband, lightnessband, cyanband, magentaband, yellowband, blackband
        Mask values:
            all_valid, per_dataset, alpha, nodata

        A field missing or None is kept to default value.
        A field can be passed as:
            a value: All bands are set to this value
            a sequence of length `band_count` of value: All bands will be set to respective state

        Example
        -------
        >>> ds.create_raster('out', 'output.tif', ds.dem.fp, 'float32', 1)
        >>> mask = ds.create_araster('mask.tif', ds.dem.fp, bool, 1, options=['SPARSE_OK=YES'])
        >>> fields = {
        ...     'nodata': -32767,
        ...     'interpretation': ['blackband', 'cyanband'],
        ... }
        >>> out = ds.create_araster('output.tif', ds.dem.fp, 'float32', 2, fields)

        Caveat
        ------
        When using the GTiff driver, specifying a `mask` or `interpretation` field may lead to unexpected results.

        """
        self._validate_key(key)
        if sr is not None:
            fp = self._convert_footprint(fp, sr)
        gdal_ds = RasterPhysical._create_file(
            path, fp, dtype, band_count, band_schema, driver, options, sr
        )
        prox = RasterPhysical(self, gdal_ds, 'w')
        self._register([key], prox)
        return prox

    def create_araster(self, path, fp, dtype, band_count, band_schema=None,
                       driver='GTiff', options=(), sr=None):
        """Create a raster file anonymously in this DataSource. Only metadata are kept in memory.

        See DataSource.create_raster
        """
        if sr is not None:
            fp = self._convert_footprint(fp, sr)
        gdal_ds = RasterPhysical._create_file(
            path, fp, dtype, band_count, band_schema, driver, options, sr
        )
        prox = RasterPhysical(self, gdal_ds, 'w')
        self._register([], prox)
        return prox

    def create_recipe_raster(self, key, fn, fp, dtype, band_schema=None, sr=None):
        """Create a raster recipe and register it under `key` in this DataSource.

        A recipe is a read-only raster that behaves like any other raster, pixel values are computed
        on-the-fly with calls to the provided `pixel functions`. A pixel function maps a Footprint to
        a numpy array of the same shape, it may be called several time to compute a result.

        Parameters
        ----------
        key: hashable (like a string)
            File identifier within DataSource
        fn: callable or sequence of callable
            pixel functions, one per band
            A pixel function take a Footprint and return a np.ndarray with the same shape.
        path: string
        fp: Footprint
            Location and size of the raster to create.
        dtype: numpy type (or any alias)
        band_schema: dict or None
            Band(s) metadata. (see `DataSource.create_raster` below)
        sr: string or None
            Spatial reference of the new file

            if None: don't set a spatial reference
            if string:
                if path: Use same projection as file at `path`
                if textual spatial reference:
                    http://gdal.org/java/org/gdal/osr/SpatialReference.html#SetFromUserInput-java.lang.String-

        Exemple
        -------
        Computing the Mandelbrot fractal using buzzard

        >>> import buzzard as buzz
        ... import numpy as np
        ... from numba import jit
        ... import matplotlib.pyplot as plt
        ... import shapely.geometry as sg
        ...
        ... @jit(nopython=True, nogil=True, cache=True)
        ... def mandelbrot_jit(array, tl, scale, maxit):
        ...     for j in range(array.shape[0]):
        ...         y0 = tl[1] + j * scale[1]
        ...         for i in range(array.shape[1]):
        ...             x0 = tl[0] + i * scale[0]
        ...             x = 0.0
        ...             y = 0.0
        ...             x2 = 0.0
        ...             y2 = 0.0
        ...             iteration = 0
        ...             while x2 + y2 < 4 and iteration < maxit:
        ...                 y = 2 * x * y + y0
        ...                 x = x2 - y2 + x0
        ...                 x2 = x * x
        ...                 y2 = y * y
        ...                 iteration += 1
        ...             array[j][i] = iteration * 255 / maxit
        ...
        ... with buzz.Env(allow_complex_footprint=True, warnings=False):
        ...     ds = buzz.DataSource()
        ...
        ...     def pixel_function(fp):
        ...         print('  Computing {}'.format(fp))
        ...         array = np.empty(fp.shape, 'uint8')
        ...         mandelbrot_jit(array, fp.tl, fp.scale, maxit)
        ...         return array
        ...
        ...     size = 5000
        ...     fp = buzz.Footprint(
        ...         gt=(-2, 4 / size, 0, -2, 0, 4 / size),
        ...         rsize=(size, size),
        ...     )
        ...     print('Recipe:{}'.format(fp))
        ...     r = ds.create_recipe_araster(pixel_function, fp, 'uint8', {'nodata': 0})
        ...
        ...     focus = sg.Point(-1.1172, -0.221103)
        ...     for factor in [1, 4, 16, 64]:
        ...         buffer = 2 / factor
        ...         maxit = 25 * factor
        ...         fp = fp.dilate(buffer // fp.pxsizex) & focus.buffer(buffer)
        ...         print('Zoom:{}, radius:{}, max-iteration:{}, fp:{}'.format(factor, buffer, maxit, fp))
        ...         a = r.get_data(fp=fp)
        ...         plt.imshow(a, origin='upper', extent=(fp.lx, fp.rx, fp.by, fp.ty))
        ...         plt.show()
        ...
        Recipe:     Footprint(tl=(-2.000000, -2.000000), scale=(0.000800, 0.000800), angle=0.000000, rsize=(5000, 5000))
        Zoom:1, radius:2.0, max-iteration:25,
                 fp:Footprint(tl=(-3.117600, -2.221600), scale=(0.000800, 0.000800), angle=0.000000, rsize=(5001, 5001))
          Computing Footprint(tl=(-2.000000, -2.000000), scale=(0.000800, 0.000800), angle=0.000000, rsize=(3609, 4729))
        Zoom:4, radius:0.5, max-iteration:100,
                 fp:Footprint(tl=(-1.617600, -0.721600), scale=(0.000800, 0.000800), angle=0.000000, rsize=(1251, 1251))
          Computing Footprint(tl=(-1.620800, -0.724800), scale=(0.000800, 0.000800), angle=0.000000, rsize=(1259, 1259))
        Zoom:16, radius:0.125, max-iteration:400,
                 fp:Footprint(tl=(-1.242400, -0.346400), scale=(0.000800, 0.000800), angle=0.000000, rsize=(313, 313))
          Computing Footprint(tl=(-1.246400, -0.350400), scale=(0.000800, 0.000800), angle=0.000000, rsize=(323, 323))
        Zoom:64, radius:0.03125, max-iteration:1600,
                 fp:Footprint(tl=(-1.148800, -0.252800), scale=(0.000800, 0.000800), angle=0.000000, rsize=(79, 79))
          Computing Footprint(tl=(-1.152800, -0.256800), scale=(0.000800, 0.000800), angle=0.000000, rsize=(89, 89))

        """
        self._validate_key(key)
        if sr is not None:
            fp = self._convert_footprint(fp, sr)

        if hasattr(fn, '__call__'):
            fn_lst = (fn,)
        elif not isinstance(fn, collections.Container):
            fn_lst = tuple(fn)
            for fn_elt in fn_lst:
                if not hasattr(fn_elt, '__call__'):
                    raise TypeError('fn should be a callable or a sequence of callables')
        else:
            raise TypeError('fn should be a callable or a sequence of callables')

        gdal_ds = RasterRecipe._create_vrt(fp, dtype, len(fn_lst), band_schema, sr)
        prox = RasterRecipe(self, gdal_ds, fn_lst)
        self._register([key], prox)
        return prox

    def create_recipe_araster(self, fn, fp, dtype, band_schema=None, sr=None):
        """Create a raster recipe anonymously in this DataSource.

        See DataSource.create_recipe_raster
        """
        if sr is not None:
            fp = self._convert_footprint(fp, sr)

        if hasattr(fn, '__call__'):
            fn_lst = (fn,)
        elif not isinstance(fn, collections.Container):
            fn_lst = tuple(fn)
            for fn_elt in fn_lst:
                if not hasattr(fn_elt, '__call__'):
                    raise TypeError('fn should be a callable or a sequence of callables')
        else:
            raise TypeError('fn should be a callable or a sequence of callables')

        gdal_ds = RasterRecipe._create_vrt(fp, dtype, len(fn_lst), band_schema, sr)
        prox = RasterRecipe(self, gdal_ds, fn_lst)
        self._register([], prox)
        return prox

    # Vector entry points *********************************************************************** **
    def open_vector(self, key, path, layer=None, driver='ESRI Shapefile', options=(), mode='r'):
        """Open a vector file in this DataSource under `key`. Only metadata are kept in memory.

        Parameters
        ----------
        key: hashable (like a string)
            File identifier within DataSource
        path: string
        layer: None or int or string
        driver: string
            ogr driver to use when opening the file
            http://www.gdal.org/ogr_formats.html
        options: sequence of str
            options for ogr
        mode: one of ('r', 'w')

        Example
        -------
        >>> ds.open_vector('trees', '/path/to.shp')
        >>> trees = ds.open_avector('/path/to.shp')
        >>> ds.open_vector('roofs', '/path/to.json', driver='GeoJSON', mode='w')

        """
        self._validate_key(key)
        ogr_ds, lyr = Vector._open_file(path, layer, driver, options, mode)
        prox = Vector(self, ogr_ds, lyr, mode)
        self._register([key], prox)
        return prox

    def open_avector(self, path, layer=None, driver='ESRI Shapefile', options=(), mode='r'):
        """Open a vector file anonymously in this DataSource. Only metadata are kept in memory.

        See DataSource.open_vector
        """
        ogr_ds, lyr = Vector._open_file(path, layer, driver, options, mode)
        prox = Vector(self, ogr_ds, lyr, mode)
        self._register([], prox)
        return prox

    def create_vector(self, key, path, geometry, fields=(), layer=None,
                      driver='ESRI Shapefile', options=(), sr=None):
        """Create a vector file and register it under `key` in this DataSource. Only metadata are
        kept in memory.

        Parameters
        ----------
        key: hashable (like a string)
            File identifier within DataSource
        path: string
        geometry: string
            name of a wkb geometry type
            http://www.gdal.org/ogr__core_8h.html#a800236a0d460ef66e687b7b65610f12a
            (see example below)
        fields: sequence of dict
            Attributes of fields, one dict per field. (see `Field attributes` below)
        layer: None or string
        driver: string
            ogr driver to use when opening the file
            http://www.gdal.org/ogr_formats.html
        options: sequence of str
            options for ogr
        sr: string or None
            Spatial reference of the new file

            if None: don't set a spatial reference
            if string:
                if path: Use same projection as file at `path`
                if textual spatial reference:
                    http://gdal.org/java/org/gdal/osr/SpatialReference.html#SetFromUserInput-java.lang.String-

        Field attributes
        ----------------
        Attributes:
            'name': string
            'type': string (see `Field type` below)
            'precision': int
            'width': int
            'nullable': bool
            'default': same as `type`
        An attribute missing or None is kept to default value.

        Field types
        -----------
        Binary        key: 'binary', bytes, np.bytes_, aliases of np.bytes_
        Date          key: 'date'
        DateTime      key: 'datetime', datetime.datetime, np.datetime64, aliases of np.datetime64
        Time          key: 'time'

        Integer       key: 'integer' np.int32, aliases of np.int32
        Integer64     key: 'integer64', int, np.int64, aliases of np.int64
        Real          key: 'real', float, np.float64, aliases of np.float64
        String        key: 'string', str, np.str_, aliases of np.str_

        Integer64List key: 'integer64list', 'int list'
        IntegerList   key: 'integerlist'
        RealList      key: 'reallist', 'float list'
        StringList    key: 'stringlist', 'str list'

        Example
        -------
        >>> ds.create_vector('lines', '/path/to.shp', 'linestring')
        >>> lines = ds.create_avector('/path/to.shp', 'linestring')

        >>> fields = [
            {'name': 'name', 'type': str},
            {'name': 'count', 'type': 'int32'},
            {'name': 'area', 'type': np.float64, 'width': 5, precision: 18},
            {'name': 'when', 'type': np.datetime64},
        ]
        >>> ds.create_vector('zones', '/path/to.shp', 'polygon', fields)

        """
        self._validate_key(key)
        ogr_ds, lyr = Vector._create_file(
            path, geometry, fields, layer, driver, options, sr
        )
        prox = Vector(self, ogr_ds, lyr, 'w')
        self._register([key], prox)
        return prox

    def create_avector(self, path, geometry, fields=(), layer=None,
                       driver='ESRI Shapefile', options=(), sr=None):
        """Create a vector file anonymously in this DataSource. Only metadata are kept in memory.

        See DataSource.create_vector
        """
        ogr_ds, lyr = Vector._create_file(
            path, geometry, fields, layer, driver, options, sr
        )
        prox = Vector(self, ogr_ds, lyr, 'w')
        self._register([], prox)
        return prox

    # Proxy getters ********************************************************* **
    def __getitem__(self, key):
        """Retrieve a proxy from its key"""
        return self._proxy_of_key[key]

    def __contains__(self, item):
        """Is key or proxy registered in DataSource"""
        if isinstance(item, Proxy):
            return item in self._keys_of_proxy
        return item in self._proxy_of_key

    # Spatial reference getters ********************************************* **
    @property
    def proj4(self):
        """DataSource's work spatial reference in WKT proj4.
        Returns None if none set.
        """
        if self._sr_work is None:
            return None
        return self._sr_work.ExportToProj4()

    @property
    def wkt(self):
        """DataSource's work spatial reference in WKT format.
        Returns None if none set.
        """
        if self._sr_work is None:
            return None
        return self._wkt_work
