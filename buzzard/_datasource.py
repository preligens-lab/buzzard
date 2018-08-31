""">>> help(buzz.DataSource)"""

# pylint: disable=too-many-lines
import ntpath
import numbers
import sys

from osgeo import osr
import numpy as np

from buzzard._tools import conv, deprecation_pool
from buzzard._footprint import Footprint
from buzzard import _tools
from buzzard._datasource_back import BackDataSource
from buzzard._a_proxy import AProxy
from buzzard._gdal_file_raster import GDALFileRaster, BackGDALFileRaster
from buzzard._gdal_file_vector import GDALFileVector, BackGDALFileVector
from buzzard._gdal_mem_raster import GDALMemRaster
from buzzard._gdal_memory_vector import GDALMemoryVector
from buzzard._datasource_register import DataSourceRegisterMixin
from buzzard._numpy_raster import NumpyRaster

class DataSource(DataSourceRegisterMixin):
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
    max_active: nbr >= 1
        Maximum number of sources active at the same time.
    assert_no_change_on_activation: bool
        When activating a deactivated file, check that the definition did not change
        (see `Sources activation / deactivation` below)

    Example
    -------
    >>> import buzzard as buzz

    Creating DataSource
    >>> ds = buzz.DataSource()

    Opening
    >>> ds.open_vector('roofs', 'path/to/roofs.shp')
    >>> feature_count = len(ds.roofs)

    >>> ds.open_raster('dem', 'path/to/dem.tif')
    >>> data_type = ds.dem.dtype

    Opening with context management
    >>> with ds.open_raster('rgb', 'path/to/rgb.tif').close:
    ...     data_type = ds.rgb.fp
    ...     arr = ds.rgb.get_data()

    >>> with ds.aopen_raster('path/to/rgb.tif').close as rgb:
    ...     data_type = rgb.dtype
    ...     arr = rgb.get_data()

    Creation
    >>> ds.create_vector('targets', 'path/to/targets.geojson', 'point', driver='GeoJSON')
    >>> geometry_type = ds.targets.type

    >>> with ds.acreate_raster('/tmp/cache.tif', ds.dem.fp, 'float32', 1).delete as cache:
    ...     file_footprint = cache.fp
    ...     cache.set_data(dem.get_data())

    On the fly re-projections in buzzard
    ------------------------------------
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
    `sr_forced`: A `sr_virtual` provided by user to ignore all `sr_stored`. This is for exemple
        useful when the `sr` stored in the input files are corrupted.
    `sr_fallback`: A `sr_virtual` provided by user to be used when `sr_stored` is missing. This is
        for exemple useful when an input file can't store a `sr (e.g. DFX).

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
    mode 1 - No conversions at all
    >>> ds = buzz.DataSource()

    mode 2 - Working with WGS84 coordinates
    >>> ds = buzz.DataSource(
            sr_work='WGS84',
        )

    mode 3 - Working in UTM with DXF files in WGS84 coordinates
    >>> ds = buzz.DataSource(
            sr_work='EPSG:32632',
            sr_fallback='WGS84',
        )

    mode 4 - Working in UTM with unreliable LCC input files
    >>> ds = buzz.DataSource(
            sr_work='EPSG:32632',
            sr_forced='EPSG:27561',
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
                 allow_none_geometry=False,
                 allow_interpolation=False,
                 max_active=np.inf,
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
        max_active, kwargs = deprecation_pool.streamline_with_kwargs(
            new_name='max_active', old_names={'max_activated': '0.5.0'}, context='DataSource.__init__',
            new_name_value=max_active,
            new_name_is_provided=max_active != np.inf,
            user_kwargs=kwargs,
        )
        if kwargs: # pragma: no cover
            raise TypeError("__init__() got an unexpected keyword argument '{}'".format(
                list(kwargs.keys())[0]
            ))

        mode = (sr_work is not None, sr_fallback is not None, sr_forced is not None)
        if mode == (False, False, False):
            pass
        elif mode == (True, False, False):
            sr_work = osr.GetUserInputAsWKT(sr_work)
        elif mode == (True, True, False):
            sr_work = osr.GetUserInputAsWKT(sr_work)
            sr_fallback = osr.GetUserInputAsWKT(sr_fallback)
        elif mode == (True, False, True):
            sr_work = osr.GetUserInputAsWKT(sr_work)
            sr_forced = osr.GetUserInputAsWKT(sr_forced)
        else:
            raise ValueError('Bad combination of `sr_*` parameters') # pragma: no cover

        if max_active < 1: # pragma: no cover
            raise ValueError('`max_active` should be greater than 1')

        allow_interpolation = bool(allow_interpolation)
        allow_none_geometry = bool(allow_none_geometry)
        analyse_transformation = bool(analyse_transformation)

        self._back = BackDataSource(
            wkt_work=sr_work,
            wkt_fallback=sr_fallback,
            wkt_forced=sr_forced,
            analyse_transformation=analyse_transformation,
            allow_none_geometry=allow_none_geometry,
            allow_interpolation=allow_interpolation,
            max_active=max_active,
        )
        super(DataSource, self).__init__()

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
        mode: one of {'r', 'w'}

        Returns
        -------
        GDALFileRaster

        Example
        -------
        >>> ds.open_raster('ortho', '/path/to/ortho.tif')
        >>> file_proj4 = ds.ortho.proj4_stored

        >>> ds.open_raster('dem', '/path/to/dem.tif', mode='w')
        >>> nodata_value = ds.dem.nodata

        """
        # Parameter checking ***************************************************
        self._validate_key(key)
        path = str(path)
        driver = str(driver)
        options = [str(arg) for arg in options]
        _ = conv.of_of_mode(mode)

        # Construction dispatch ************************************************
        if driver.lower() == 'mem': # pragma: no cover
            raise ValueError("Can't open a MEM raster, user create_raster")
        elif True:
            allocator = lambda: BackGDALFileRaster.open_file(
                path, driver, options, mode
            )
            prox = GDALFileRaster(self, allocator, options, mode)
        else:
            pass

        # DataSource Registering ***********************************************
        self._register([key], prox)
        return prox

    def aopen_raster(self, path, driver='GTiff', options=(), mode='r'):
        """Open a raster file anonymously in this DataSource. Only metadata are kept in memory.

        See DataSource.open_raster

        Example
        ------
        >>> ortho = ds.aopen_raster('/path/to/ortho.tif')
        >>> file_wkt = ds.ortho.wkt_stored

        """
        # Parameter checking ***************************************************
        path = str(path)
        driver = str(driver)
        options = [str(arg) for arg in options]
        _ = conv.of_of_mode(mode)

        # Construction dispatch ************************************************
        if driver.lower() == 'mem': # pragma: no cover
            raise ValueError("Can't open a MEM raster, user acreate_raster")
        elif True:
            allocator = lambda: BackGDALFileRaster.open_file(
                path, driver, options, mode
            )
            prox = GDALFileRaster(self, allocator, options, mode)
        else:
            pass

        # DataSource Registering ***********************************************
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

        Returns
        -------
        one of {GDALFileRaster, GDALMemRaster}
            depending on the `driver` parameter

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
        >>> file_footprint = ds.out.fp

        Caveat
        ------
        When using the GTiff driver, specifying a `mask` or `interpretation` field may lead to unexpected results.

        """
        # Parameter checking ***************************************************
        self._validate_key(key)
        path = str(path)
        if not isinstance(fp, Footprint): # pragma: no cover
            raise TypeError('`fp` should be a Footprint')
        dtype = np.dtype(dtype)
        band_count = int(band_count)
        band_schema = _tools.sanitize_band_schema(band_schema, band_count)
        driver = str(driver)
        options = [str(arg) for arg in options]
        if sr is not None:
            sr = osr.GetUserInputAsWKT(sr)

        if sr is not None:
            fp = self._back.convert_footprint(fp, sr)

        # Construction dispatch ************************************************
        if driver.lower() == 'mem':
            # TODO: Check not concurrent
            prox = GDALMemRaster(
                self, fp, dtype, band_count, band_schema, options, sr
            )
        elif True:
            allocator = lambda: BackGDALFileRaster.create_file(
                path, fp, dtype, band_count, band_schema, driver, options, sr
            )
            prox = GDALFileRaster(self, allocator, options, 'w')
        else:
            pass

        # DataSource Registering ***********************************************
        self._register([key], prox)
        return prox

    def acreate_raster(self, path, fp, dtype, band_count, band_schema=None,
                       driver='GTiff', options=(), sr=None):
        """Create a raster file anonymously in this DataSource. Only metadata are kept in memory.

        See DataSource.create_raster

        Example
        -------
        >>> mask = ds.acreate_raster('mask.tif', ds.dem.fp, bool, 1, options=['SPARSE_OK=YES'])
        >>> open_options = mask.open_options

        >>> band_schema = {
        ...     'nodata': -32767,
        ...     'interpretation': ['blackband', 'cyanband'],
        ... }
        >>> out = ds.acreate_raster('output.tif', ds.dem.fp, 'float32', 2, band_schema)
        >>> band_interpretation = out.band_schema['interpretation']

        """
        # Parameter checking ***************************************************
        path = str(path)
        if not isinstance(fp, Footprint): # pragma: no cover
            raise TypeError('`fp` should be a Footprint')
        dtype = np.dtype(dtype)
        band_count = int(band_count)
        band_schema = _tools.sanitize_band_schema(band_schema, band_count)
        driver = str(driver)
        options = [str(arg) for arg in options]
        if sr is not None:
            sr = osr.GetUserInputAsWKT(sr)

        if sr is not None:
            fp = self._back.convert_footprint(fp, sr)

        # Construction dispatch ************************************************
        if driver.lower() == 'mem':
            # TODO: Check not concurrent
            prox = GDALMemRaster(
                self, fp, dtype, band_count, band_schema, options, sr
            )
        elif True:
            allocator = lambda: BackGDALFileRaster.create_file(
                path, fp, dtype, band_count, band_schema, driver, options, sr
            )
            prox = GDALFileRaster(self, allocator, options, 'w')
        else:
            pass

        # DataSource Registering ***********************************************
        self._register([], prox)
        return prox

    def wrap_numpy_raster(self, key, fp, array, band_schema=None, sr=None, mode='w'):
        """Register a numpy array as a raster under `key` in this DataSource.

        Parameters
        ----------
        key: hashable (like a string)
            File identifier within DataSource
        fp: Footprint of shape (Y, X)
            Description of the location and size of the raster to create.
        array: ndarray of shape (Y, X) or (Y, X, B)
        band_schema: dict or None
            Band(s) metadata. (see `Band fields` below)
        sr: string or None
            Spatial reference of the new file

            if None: don't set a spatial reference
            if string:
                if path: Use same projection as file at `path`
                if textual spatial reference:
                    http://gdal.org/java/org/gdal/osr/SpatialReference.html#SetFromUserInput-java.lang.String-
        mode: one of {'r', 'w'}

        Returns
        -------
        NumpyRaster

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

        """
        # Parameter checking ***************************************************
        self._validate_key(key)
        if not isinstance(fp, Footprint): # pragma: no cover
            raise TypeError('`fp` should be a Footprint')
        array = np.asarray(array)
        if array.shape[:2] != tuple(fp.shape): # pragma: no cover
            raise ValueError('Incompatible shape between `array` and `fp`')
        if array.ndim not in [2, 3]: # pragma: no cover
            raise ValueError('Array should have 2 or 3 dimensions')
        band_count = 1 if array.ndim == 2 else array.shape[-1]
        band_schema = _tools.sanitize_band_schema(band_schema, band_count)
        if sr is not None:
            sr = osr.GetUserInputAsWKT(sr)
        _ = conv.of_of_mode(mode)

        if sr is not None:
            fp = self._back.convert_footprint(fp, sr)

        # Construction *********************************************************
        prox = NumpyRaster(self, fp, array, band_schema, sr, mode)

        # DataSource Registering ***********************************************
        self._register([key], prox)
        return prox

    def awrap_numpy_raster(self, fp, array, band_schema=None, sr=None, mode='w'):
        """Register a numpy array as a raster anonymously in this DataSource.

        See DataSource.wrap_numpy_raster
        """
        # Parameter checking ***************************************************
        if not isinstance(fp, Footprint): # pragma: no cover
            raise TypeError('`fp` should be a Footprint')
        array = np.asarray(array)
        if array.shape[:2] != tuple(fp.shape): # pragma: no cover
            raise ValueError('Incompatible shape between `array` and `fp`')
        if array.ndim not in [2, 3]: # pragma: no cover
            raise ValueError('Array should have 2 or 3 dimensions')
        band_count = 1 if array.ndim == 2 else array.shape[-1]
        band_schema = _tools.sanitize_band_schema(band_schema, band_count)
        if sr is not None:
            sr = osr.GetUserInputAsWKT(sr)
        _ = conv.of_of_mode(mode)

        if sr is not None:
            fp = self._back.convert_footprint(fp, sr)

        # Construction *********************************************************
        prox = NumpyRaster(self, fp, array, band_schema, sr, mode)

        # DataSource Registering ***********************************************
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
        mode: one of {'r', 'w'}

        Returns
        -------
        GDALFileVector

        Example
        -------
        >>> ds.open_vector('trees', '/path/to.shp')
        >>> feature_count = len(ds.trees)

        >>> ds.open_vector('roofs', '/path/to.json', driver='GeoJSON', mode='w')
        >>> fields_list = ds.roofs.fields

        """
        # Parameter checking ***************************************************
        self._validate_key(key)
        path = str(path)
        if layer is None:
            layer = 0
        elif isinstance(layer, numbers.Integral):
            layer = int(layer)
        else:
            layer = str(layer)
        driver = str(driver)
        options = [str(arg) for arg in options]
        _ = conv.of_of_mode(mode)

        # Construction dispatch ************************************************
        if driver.lower() == 'memory': # pragma: no cover
            raise ValueError("Can't open a MEMORY vector, user create_vector")
        elif True:
            allocator = lambda: BackGDALFileVector.open_file(
                path, layer, driver, options, mode
            )
            prox = GDALFileVector(self, allocator, options, mode)
        else:
            pass

        # DataSource Registering ***********************************************
        self._register([key], prox)
        return prox

    def aopen_vector(self, path, layer=None, driver='ESRI Shapefile', options=(), mode='r'):
        """Open a vector file anonymously in this DataSource. Only metadata are kept in memory.

        See DataSource.open_vector

        Example
        -------
        >>> trees = ds.aopen_vector('/path/to.shp')
        >>> features_bounds = trees.bounds

        """
        path = str(path)
        if layer is None:
            layer = 0
        elif isinstance(layer, numbers.Integral):
            layer = int(layer)
        else:
            layer = str(layer)
        driver = str(driver)
        options = [str(arg) for arg in options]
        _ = conv.of_of_mode(mode)

        # Construction dispatch ************************************************
        if driver.lower() == 'memory': # pragma: no cover
            raise ValueError("Can't open a MEMORY vector, user create_vector")
        elif True:
            allocator = lambda: BackGDALFileVector.open_file(
                path, layer, driver, options, mode
            )
            prox = GDALFileVector(self, allocator, options, mode)
        else:
            pass

        # DataSource Registering ***********************************************
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

        Returns
        -------
        one of {GDALFileVector, GDALMemoryVector} depending on the `driver` parameter

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
        >>> geometry_type = ds.lines.type

        >>> fields = [
            {'name': 'name', 'type': str},
            {'name': 'count', 'type': 'int32'},
            {'name': 'area', 'type': np.float64, 'width': 5, precision: 18},
            {'name': 'when', 'type': np.datetime64},
        ]
        >>> ds.create_vector('zones', '/path/to.shp', 'polygon', fields)
        >>> field0_type = ds.zones.fields[0]['type']

        """
        # Parameter checking ***************************************************
        self._validate_key(key)
        path = str(path)
        geometry = conv.str_of_wkbgeom(conv.wkbgeom_of_str(geometry))
        fields = _tools.normalize_fields_defn(fields)
        if layer is None:
            layer = '.'.join(ntpath.basename(path).split('.')[:-1])
        else:
            layer = str(layer)
        driver = str(driver)
        options = [str(arg) for arg in options]
        if sr is not None:
            sr = osr.GetUserInputAsWKT(sr)

        # Construction dispatch ************************************************
        if driver.lower() == 'memory':
            # TODO: Check not concurrent
            allocator = lambda: BackGDALFileVector.create_file(
                '', geometry, fields, layer, 'Memory', options, sr
            )
            prox = GDALMemoryVector(self, allocator, options)
        elif True:
            allocator = lambda: BackGDALFileVector.create_file(
                path, geometry, fields, layer, driver, options, sr
            )
            prox = GDALFileVector(self, allocator, options, 'w')
        else:
            pass

        # DataSource Registering ***********************************************
        self._register([key], prox)
        return prox

    def acreate_vector(self, path, geometry, fields=(), layer=None,
                       driver='ESRI Shapefile', options=(), sr=None):
        """Create a vector file anonymously in this DataSource. Only metadata are kept in memory.

        See DataSource.create_vector

        Example
        -------
        >>> lines = ds.acreate_vector('/path/to.shp', 'linestring')
        >>> file_proj4 = lines.proj4_stored

        """
        # Parameter checking ***************************************************
        path = str(path)
        geometry = conv.str_of_wkbgeom(conv.wkbgeom_of_str(geometry))
        fields = _tools.normalize_fields_defn(fields)
        if layer is None:
            layer = '.'.join(ntpath.basename(path).split('.')[:-1])
        else:
            layer = str(layer)
        driver = str(driver)
        options = [str(arg) for arg in options]
        if sr is not None:
            sr = osr.GetUserInputAsWKT(sr)

        # Construction dispatch ************************************************
        if driver.lower() == 'memory':
            # TODO: Check not concurrent
            allocator = lambda: BackGDALFileVector.create_file(
                '', geometry, fields, layer, 'Memory', options, sr
            )
            prox = GDALMemoryVector(self, allocator, options)
        elif True:
            allocator = lambda: BackGDALFileVector.create_file(
                path, geometry, fields, layer, driver, options, sr
            )
            prox = GDALFileVector(self, allocator, options, 'w')
        else:
            pass

        # DataSource Registering ***********************************************
        self._register([], prox)
        return prox

    # Proxy getters ********************************************************* **
    def __getitem__(self, key):
        """Retrieve a proxy from its key"""
        return self._proxy_of_key[key]

    def __contains__(self, item):
        """Is key or proxy registered in DataSource"""
        if isinstance(item, AProxy):
            return item in self._keys_of_proxy
        return item in self._proxy_of_key

    def __len__(self):
        """Retrieve proxy count registered in this DataSource"""
        return len(self._keys_of_proxy)

    # Spatial reference getters ********************************************* **
    @property
    def proj4(self):
        """DataSource's work spatial reference in WKT proj4.
        Returns None if none set.
        """
        if self._back.wkt_work is None:
            return None
        return osr.SpatialReference(self._back.wkt_work).ExportToProj4()

    @property
    def wkt(self):
        """DataSource's work spatial reference in WKT format.
        Returns None if none set.
        """
        return self._back.wkt_work

    # Activation mechanisms ********************************************************************* **
    @property
    def active_count(self):
        """Count how many driver objects are currently active"""
        return self._back.active_count()

    def activate_all(self):
        """Activate all deactivable proxies.
        May raise an exception if the number of sources is greater than `max_activated`
        """
        if self._back.max_active < len(self._keys_of_proxy):
            raise RuntimeError("Can't activate all sources at the same time: {} sources and max_activated is {}".format(
                len(self._keys_of_proxy), self._back.max_active,
            ))
        for prox in self._keys_of_proxy.keys():
            if not prox.active:
                prox.activate()

    def deactivate_all(self):
        """Deactivate all deactivable proxies. Useful to flush all files to disk"""
        for prox in self._keys_of_proxy.keys():
            if prox.active:
                prox.deactivate()


    # Deprecation ******************************************************************************* **
    open_araster = deprecation_pool.wrap_method(
        aopen_raster,
        '0.4.4'
    )
    create_araster = deprecation_pool.wrap_method(
        acreate_raster,
        '0.4.4'
    )
    open_avector = deprecation_pool.wrap_method(
        aopen_vector,
        '0.4.4'
    )
    create_avector = deprecation_pool.wrap_method(
        acreate_vector,
        '0.4.4'
    )

    # The end *********************************************************************************** **
    # ******************************************************************************************* **

if sys.version_info < (3, 6):
    # https://www.python.org/dev/peps/pep-0487/
    for k, v in DataSource.__dict__.items():
        if hasattr(v, '__set_name__'):
            v.__set_name__(DataSource, k)

def open_raster(*args, **kwargs):
    """Shortcut for `DataSource().aopen_raster`"""
    return DataSource().aopen_raster(*args, **kwargs)

def open_vector(*args, **kwargs):
    """Shortcut for `DataSource().aopen_vector`"""
    return DataSource().aopen_vector(*args, **kwargs)

def create_raster(*args, **kwargs):
    """Shortcut for `DataSource().acreate_raster`"""
    return DataSource().acreate_raster(*args, **kwargs)

def create_vector(*args, **kwargs):
    """Shortcut for `DataSource().acreate_vector`"""
    return DataSource().acreate_vector(*args, **kwargs)

def wrap_numpy_raster(*args, **kwargs):
    """Shortcut for `DataSource().awrap_numpy_raster`"""
    return DataSource().awrap_numpy_raster(*args, **kwargs)
