""">>> help(buzz.DataSource)"""

# pylint: disable=too-many-lines

import collections

from osgeo import osr
import numpy as np

from buzzard import _datasource_tools
from buzzard._proxy import Proxy
from buzzard._raster_stored import RasterStored
from buzzard._raster_recipe import RasterRecipe
from buzzard._vector import Vector
from buzzard._tools import conv, deprecation_pool
from buzzard._datasource_conversions import DataSourceConversionsMixin

class DataSource(_datasource_tools.DataSourceToolsMixin, DataSourceConversionsMixin):
    """DataSource is a class that stores references to files, it allows quick manipulations
    by assigning a key to each registered file.

    For actions specific to opened files, see Raster, RasterStored and VectorProxy classes

    Parameters
    ----------
    sr_work: None or string (see `Coordinates conversions` below)
    sr_fallback: None or string (see `Coordinates conversions` below)
    sr_forced: None or string (see `Coordinates conversions` below)
    analyse_transformation: bool
        Whether or not to perform a basic analysis on two sr to check their compatibilty
    ogr_layer_lock: one of ('none', 'wait', 'raise')
        Mutex operations when reading or writing vector files
    allow_none_geometry: bool
    allow_interpolation: bool
    max_activated: nbr >= 1
        Maximum number of sources activated at the same time.
    assert_no_change_on_activation: bool
        When activating a deactivated file, check that the definition did not change
        (see `Sources activation / deactivation` below)

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
    >>> with ds.aopen_raster('path/to/rgb.tif').close as rgb:
    ...     print(rgb.fp)
    ...     arr = rgb.get_data()

    Creation
    >>> ds.create_vector('targets', 'path/to/targets.geojson', 'point', driver='GeoJSON')
    >>> with ds.acreate_raster('/tmp/cache.tif', ds.dem.fp, 'float32', 1).delete as cache:
    ...      cache.set_data(dem.get_data())

    Coordinates conversions
    -----------------------
    A DataSource may perform spatial reference conversions on the fly, like a GIS does. Several
    modes are available, a set of rules define how each mode work. Those conversions concern both
    read operations and write operations, all are performed by OSR.

    Those conversions are only perfomed on vector's data/metadata and raster's Footprints.
    This implies that classic raster warping is not included (yet) in those conversions, only raster
    shifting/scaling/rotation.

    The `z` coordinates of vectors features are also converted, on the other hand elevations are not
    converted in DEM rasters.

    If `analyse_transformation` is set to `True` (default), all coordinates conversions are
    tested against `buzz.env.significant` on file opening to ensure their feasibility or
    raise an exception otherwise. This system is naive and very restrictive, a smarter
    version is planned. Use with caution.

    Terminology:
    `sr`: Spatial reference
    `sr_work`: The sr of all interactions with a DataSource (i.e. Footprints, extents, Polygons...),
        may be missing
    `sr_stored`: The sr that can be found in the metadata of a raster/vector storage, may be None
        or ignored
    `sr_virtual`: The sr considered to be written in the metadata of a raster/vector storage, it is
        often the same as `sr_stored`. When a raster/vector is read, a conversion is performed from
        `sr_virtual` to `sr_work`. When setting vector data, a conversion is performed from
        `sr_work` to `sr_virtual`.
    `sr_forced`: A `sr_virtual` provided by user to ignore all `sr_stored`
    `sr_fallback`: A `sr_virtual` provided by user to be used when `sr_stored` is missing

    DataSource parameters and modes:
    | mode | sr_work | sr_fallback | sr_forced | How is the `sr_virtual` of a raster/vector determined                               |
    |------|---------|-------------|-----------|-------------------------------------------------------------------------------------|
    | 1    | None    | None        | None      | Use `sr_stored`, but no conversion is performed for the lifetime of this DataSource |
    | 2    | string  | None        | None      | Use `sr_stored`, if None raise an exception                                         |
    | 3    | string  | string      | None      | Use `sr_stored`, if None it is considered to be `sr_fallback`                       |
    | 4    | string  | None        | string    | Use `sr_forced`                                                                     |

    - If all opened files are known to be written in a same sr, use `mode 1`. No conversions will
        be performed, this is the safest way to work.
    - If all opened files are known to be written in the same sr but you wish to work in a different
        sr, use `mode 4`. The huge benefit of this mode is that the `driver` specific behaviors
        concerning spatial references have no impacts on the data you manipulate.
    - If you want to manipulate files in different sr, `mode 2` and `mode 3` should be used.
       - Side note: Since the GeoJSON driver cannot store a `sr`, it is impossible to open or
         create a GeoJSON file in `mode 2`.

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
            sr_forced='path/to.tif',
        )

    Sources activation / deactivation
    ---------------------------------
    A source may be temporary deactivated, releasing it's internal file descriptor while keeping
    enough informations to reactivate itself later. By setting a `max_activated` different that
    `np.inf` in DataSource constructor, the sources of data are automatically deactivated in a
    lru fashion, and automatically reactivated when necessary.

    Benefits:
    - Open an infinite number of files without worrying about the number of file descriptors allowed
      by the system.
    - Pickle/unpickle a DataSource

    Side notes:
    - A `RasterRecipe` may require the `cloudpickle` library to be pickled
    - All sources open in 'w' mode should be closed before pickling
    - If a source's definition changed between a deactivation and an activation an exception is
      raised (i.e. file changed on the file system)

    """

    def __init__(self, sr_work=None, sr_fallback=None, sr_forced=None,
                 analyse_transformation=True,
                 ogr_layer_lock='raise',
                 allow_none_geometry=False,
                 allow_interpolation=False,
                 max_activated=np.inf,
                 assert_no_change_on_activation=True,
                 **kwargs):
        sr_fallback, kwargs = deprecation_pool.streamline_with_kwargs(
            new_name='sr_fallback', old_names={'sr_implicit': '0.4.4'}, context='DataSource.__init__',
            new_name_value=sr_fallback,
            new_name_is_provided=sr_fallback is not None,
            user_kwargs=kwargs,
        )
        sr_forced, kwargs = deprecation_pool.streamline_with_kwargs(
            new_name='sr_forced', old_names={'sr_origin': '0.4.4'}, context='DataSource.__init__',
            new_name_value=sr_forced,
            new_name_is_provided=sr_forced is not None,
            user_kwargs=kwargs,
        )
        if kwargs:
            raise NameError('Unknown parameters like `{}`'.format(
                list(kwargs.keys())[0]
            ))

        mode = (sr_work is not None, sr_fallback is not None, sr_forced is not None)
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

        if max_activated < 1:
            raise ValueError('`max_activated` should be greater than 1')

        allow_interpolation = bool(allow_interpolation)
        allow_none_geometry = bool(allow_none_geometry)
        assert_no_change_on_activation = bool(assert_no_change_on_activation)

        if mode[0]:
            wkt_work = osr.GetUserInputAsWKT(sr_work)
            sr_work = osr.SpatialReference(wkt_work)
        else:
            wkt_work = None
            sr_work = None
        if mode[1]:
            wkt_fallback = osr.GetUserInputAsWKT(sr_fallback)
            sr_fallback = osr.SpatialReference(wkt_fallback)
        else:
            wkt_fallback = None
            sr_fallback = None
        if mode[2]:
            wkt_forced = osr.GetUserInputAsWKT(sr_forced)
            sr_forced = osr.SpatialReference(wkt_forced)
        else:
            wkt_forced = None
            sr_forced = None

        DataSourceConversionsMixin.__init__(
            self, sr_work, sr_fallback, sr_forced, analyse_transformation
        )
        _datasource_tools.DataSourceToolsMixin.__init__(self, max_activated)

        self._wkt_work = wkt_work
        self._wkt_fallback = wkt_fallback
        self._wkt_forced = wkt_forced
        self._ogr_layer_lock = ogr_layer_lock
        self._allow_interpolation = allow_interpolation
        self._allow_none_geometry = allow_none_geometry
        self._assert_no_change_on_activation = assert_no_change_on_activation

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
        >>> ortho = ds.aopen_raster('/path/to/ortho.tif')
        >>> ds.open_raster('dem', '/path/to/dem.tif', mode='w')

        """
        self._validate_key(key)
        gdal_ds = RasterStored._open_file(path, driver, options, mode)
        options = [str(arg) for arg in options]
        _ = conv.of_of_mode(mode)
        consts = RasterStored._Constants(
            self, gdal_ds=gdal_ds, open_options=options, mode=mode
        )
        prox = RasterStored(self, consts, gdal_ds)
        self._register([key], prox)
        self._register_new_activated(prox)
        return prox

    def aopen_raster(self, path, driver='GTiff', options=(), mode='r'):
        """Open a raster file anonymously in this DataSource. Only metadata are kept in memory.

        See DataSource.open_raster
        """
        gdal_ds = RasterStored._open_file(path, driver, options, mode)
        options = [str(arg) for arg in options]
        _ = conv.of_of_mode(mode)
        consts = RasterStored._Constants(
            self, gdal_ds=gdal_ds, open_options=list(options), mode=mode
        )
        prox = RasterStored(self, consts, gdal_ds)
        self._register([], prox)
        self._register_new_activated(prox)
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
        >>> mask = ds.acreate_raster('mask.tif', ds.dem.fp, bool, 1, options=['SPARSE_OK=YES'])
        >>> fields = {
        ...     'nodata': -32767,
        ...     'interpretation': ['blackband', 'cyanband'],
        ... }
        >>> out = ds.acreate_raster('output.tif', ds.dem.fp, 'float32', 2, fields)

        Caveat
        ------
        When using the GTiff driver, specifying a `mask` or `interpretation` field may lead to unexpected results.

        """
        self._validate_key(key)
        if sr is not None:
            fp = self._convert_footprint(fp, sr)
        gdal_ds = RasterStored._create_file(
            path, fp, dtype, band_count, band_schema, driver, options, sr
        )
        options = [str(arg) for arg in options]
        consts = RasterStored._Constants(
            self, gdal_ds=gdal_ds, open_options=options, mode='w'
        )
        prox = RasterStored(self, consts, gdal_ds)
        self._register([key], prox)
        self._register_new_activated(prox)
        return prox

    def acreate_raster(self, path, fp, dtype, band_count, band_schema=None,
                       driver='GTiff', options=(), sr=None):
        """Create a raster file anonymously in this DataSource. Only metadata are kept in memory.

        See DataSource.create_raster
        """
        if sr is not None:
            fp = self._convert_footprint(fp, sr)
        gdal_ds = RasterStored._create_file(
            path, fp, dtype, band_count, band_schema, driver, options, sr
        )
        options = [str(arg) for arg in options]
        consts = RasterStored._Constants(
            self, gdal_ds=gdal_ds, open_options=options, mode='w'
        )
        prox = RasterStored(self, consts, gdal_ds)
        self._register([], prox)
        self._register_new_activated(prox)
        return prox

    def create_recipe_raster(self, key, fn, fp, dtype, band_schema=None, sr=None):
        """Experimental feature!

        Create a raster recipe and register it under `key` in this DataSource.

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
        ...     r = ds.acreate_recipe_raster(pixel_function, fp, 'uint8', {'nodata': 0})
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
        consts = RasterRecipe._Constants(
            self, gdal_ds=gdal_ds, fn_list=fn_lst,
        )
        prox = RasterRecipe(self, consts, gdal_ds)
        self._register([key], prox)
        self._register_new_activated(prox)
        return prox

    def acreate_recipe_raster(self, fn, fp, dtype, band_schema=None, sr=None):
        """Experimental feature!

        Create a raster recipe anonymously in this DataSource.

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
        consts = RasterRecipe._Constants(
            self, gdal_ds=gdal_ds, fn_list=fn_lst,
        )
        prox = RasterRecipe(self, consts, gdal_ds)
        self._register([], prox)
        self._register_new_activated(prox)
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
        >>> trees = ds.aopen_vector('/path/to.shp')
        >>> ds.open_vector('roofs', '/path/to.json', driver='GeoJSON', mode='w')

        """
        self._validate_key(key)
        gdal_ds, lyr = Vector._open_file(path, layer, driver, options, mode)
        options = [str(arg) for arg in options]
        _ = conv.of_of_mode(mode)
        consts = Vector._Constants(
            self, gdal_ds=gdal_ds, lyr=lyr, open_options=options, mode=mode, layer=layer,
        )
        prox = Vector(self, consts, gdal_ds, lyr)
        self._register([key], prox)
        self._register_new_activated(prox)
        return prox

    def aopen_vector(self, path, layer=None, driver='ESRI Shapefile', options=(), mode='r'):
        """Open a vector file anonymously in this DataSource. Only metadata are kept in memory.

        See DataSource.open_vector
        """
        gdal_ds, lyr = Vector._open_file(path, layer, driver, options, mode)
        options = [str(arg) for arg in options]
        _ = conv.of_of_mode(mode)
        consts = Vector._Constants(
            self, gdal_ds=gdal_ds, lyr=lyr, open_options=options, mode=mode, layer=layer,
        )
        prox = Vector(self, consts, gdal_ds, lyr)
        self._register([], prox)
        self._register_new_activated(prox)
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
        >>> lines = ds.acreate_vector('/path/to.shp', 'linestring')

        >>> fields = [
            {'name': 'name', 'type': str},
            {'name': 'count', 'type': 'int32'},
            {'name': 'area', 'type': np.float64, 'width': 5, precision: 18},
            {'name': 'when', 'type': np.datetime64},
        ]
        >>> ds.create_vector('zones', '/path/to.shp', 'polygon', fields)

        """
        self._validate_key(key)
        gdal_ds, lyr = Vector._create_file(
            path, geometry, fields, layer, driver, options, sr
        )
        options = [str(arg) for arg in options]
        consts = Vector._Constants(
            self, gdal_ds=gdal_ds, lyr=lyr, open_options=options, mode='w', layer=layer,
        )
        prox = Vector(self, consts, gdal_ds, lyr)
        self._register([key], prox)
        self._register_new_activated(prox)
        return prox

    def acreate_vector(self, path, geometry, fields=(), layer=None,
                       driver='ESRI Shapefile', options=(), sr=None):
        """Create a vector file anonymously in this DataSource. Only metadata are kept in memory.

        See DataSource.create_vector
        """
        gdal_ds, lyr = Vector._create_file(
            path, geometry, fields, layer, driver, options, sr
        )
        options = [str(arg) for arg in options]
        consts = Vector._Constants(
            self, gdal_ds=gdal_ds, lyr=lyr, open_options=options, mode='w', layer=layer,
        )
        prox = Vector(self, consts, gdal_ds, lyr)
        self._register([], prox)
        self._register_new_activated(prox)
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

    # Activation mechanisms ********************************************************************* **
    def activate_all(self):
        """Activate all sources.
        May raise an exception if the number of sources is greater than `max_activated`
        """
        if self._max_activated < len(self._keys_of_proxy):
            raise RuntimeError("Can't activate all sources at the same time: {} sources and max_activated is {}".format(
                len(self._keys_of_proxy), self._max_activated,
            ))
        for prox in self._keys_of_proxy.keys():
            if not prox.activated:
                prox.activate()
                assert prox.activated

    def deactivate_all(self):
        """Deactivate all sources. Useful to flush all files to disk
        The sources that can't be deactivated (i.e. a raster with the `MEM` driver) are ignored.
        """
        if self._locked_count != 0:
            raise RuntimeError("Can't deactivate all sources: some are forced to stay activated (are you iterating on geometries?)")
        for prox in self._keys_of_proxy.keys():
            if not prox.deactivable:
                continue
            if prox.activated:
                prox.deactivate()
                assert not prox.activated

    # Copy ************************************************************************************** **
    def copy(self):
        f, args = self.__reduce__()
        return f(*args)

    def __reduce__(self):
        params = {}
        params['sr_work'] = self._sr_work
        params['sr_fallback'] = self._sr_fallback
        params['sr_forced'] = self._sr_forced
        params['analyse_transformation'] = self._analyse_transformations
        params['ogr_layer_lock'] = self._ogr_layer_lock
        params['allow_none_geometry'] = self._allow_none_geometry
        params['allow_interpolation'] = self._allow_interpolation
        params['max_activated'] = self._max_activated
        params['assert_no_change_on_activation'] = self._assert_no_change_on_activation

        proxies = []
        for prox, keys in self._keys_of_proxy.items():

            if prox.picklable:
                consts = dict(prox._c.__dict__) # Need to recreate dict for cloudpickling
                proxies.append((
                    keys, consts, prox.__class__
                ))

        return (_restore, (params, proxies))

    # The end *********************************************************************************** **
    # ******************************************************************************************* **

deprecation_pool.add_deprecated_method(DataSource, 'aopen_raster', 'open_araster', '0.4.4')
deprecation_pool.add_deprecated_method(DataSource, 'acreate_raster', 'create_araster', '0.4.4')
deprecation_pool.add_deprecated_method(DataSource, 'aopen_vector', 'open_avector', '0.4.4')
deprecation_pool.add_deprecated_method(DataSource, 'acreate_vector', 'create_avector', '0.4.4')
deprecation_pool.add_deprecated_method(DataSource, 'acreate_recipe_raster', 'create_recipe_araster', '0.4.4')

def _restore(params, proxies):
    ds = DataSource(**params)

    for keys, consts, classobj in proxies:
        consts = classobj._Constants(ds, **consts)
        prox = classobj(ds, consts)
        ds._register(keys, prox)
    return ds
