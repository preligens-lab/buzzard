""">>> help(buzz.DataSource)"""

# pylint: disable=too-many-lines
import ntpath
import numbers
import sys
import pathlib
import itertools

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
from buzzard._cached_raster_recipe import CachedRasterRecipe
from buzzard._a_pooled_emissary import APooledEmissary
import buzzard.utils

class DataSource(DataSourceRegisterMixin):
    """DataSource is a class that stores references to sources. A source is either a raster, or a
    vector. It allows quick manipulations by assigning a key to each registered source. It also
    allows inter-sources operations, like:
    - spatial reference harmonization (see `On the fly re-projections in buzzard` below)
    - workload scheduling on pools (buzzard v0.5)
    - other features in the future (like data visualization)

    For actions specific to opened sources, see those classes:
    - GDALFileRaster,
    - GDALMemRaster,
    - NumpyRaster,
    - CachedRasterRecipe,
    TODO for 0.5.0: Add others
    - GDALFileVector,
    - GDALMemoryVector.

    Parameters
    ----------
    sr_work: None or string (see `On the fly re-projections in buzzard` below)
    sr_fallback: None or string (see `On the fly re-projections in buzzard` below)
    sr_forced: None or string (see `On the fly re-projections in buzzard` below)
    analyse_transformation: bool
        Whether or not to perform a basic analysis on two `sr` to check their compatibility.
        if True: Read the `buzz.env.significant` variable and raise an exception if a spatial
            reference conversions is too lossy in precision.
        if False: Skip all checks.
        (see `On the fly re-projections in buzzard` below)
    allow_none_geometry: bool
        Whether or not a vector geometry should raise an exception when encountering a None geometry
    allow_interpolation: bool
        Whether or not a raster geometry should raise an exception when remapping with interpolation
        is necessary.
    max_active: nbr >= 1
        Maximum number of pooled sources active at the same time.
        (see `Sources activation / deactivation` below)
    debug_observers: sequence of object
        Entry points to observe what is happening in the DataSource's sheduler.

    Example
    -------
    >>> import buzzard as buzz

    Creating a DataSource
    >>> ds = buzz.DataSource()

    Opening two files
    >>> ds.open_vector('roofs', 'path/to/roofs.shp')
    ... feature_count = len(ds.roofs)

    >>> ds.open_raster('dem', 'path/to/dem.tif')
    ... data_type = ds.dem.dtype

    Opening, reading and closing two raster files with context management
    >>> with ds.open_raster('rgb', 'path/to/rgb.tif').close:
    ...     data_type = ds.rgb.fp
    ...     arr = ds.rgb.get_data()

    >>> with ds.aopen_raster('path/to/rgb.tif').close as rgb:
    ...     data_type = rgb.dtype
    ...     arr = rgb.get_data()

    Creating two files
    >>> ds.create_vector('targets', 'path/to/targets.geojson', 'point', driver='GeoJSON')
    ... geometry_type = ds.targets.type

    >>> with ds.acreate_raster('/tmp/cache.tif', ds.dem.fp, 'float32', 1).delete as cache:
    ...     file_footprint = cache.fp
    ...     cache.set_data(dem.get_data())

    Sources type
    ------------
    Raster sources:
    - numpy.ndarray
    - GDAL drivers http://www.gdal.org/formats_list.html
        (e.g. 'GTIff', 'JPEG', 'PNG', ...)
    Vector sources:
    - OGR drivers: https://www.gdal.org/ogr_formats.html
        (e.g. 'ESRI Shapefile', 'GeoJSON', 'DXF', ...)

    Sources registering
    -------------------
    There are always two ways to create source, with a key or anonymously.

    When creating a source using a key, said key (e.g. the string "my_source_name") must be provided
    by user. Each key identify one source and should thus be unique. There are then three ways to
    access that source:
    - from the object returned by the method that created the source,
    - from the DataSource with the attribute syntax: `ds.my_source_name`,
    - from the DataSource with the item syntax: ds["my_source_name"].
    All keys should be unique.

    When creating a source anonymously you don't have to provide a key, but the only way to access
    this source is to use the object returned by the method that created the source.

    Sources activation / deactivation
    ---------------------------------
    The sources that inherit from `APooledEmissary` (like `GDALFileVector` and `GDALFileRaster`) are
    flexible about their underlying driver object. Those sources may be temporary deactivated
    (useful to limit the number of file descriptors active), or activated multiple time at the
    same time (useful to perfom concurrent reads).

    Those sources are automatically activated and deactivated given the current needs and
    constraints. Setting a `max_activated` lower than `np.inf` in the DataSource constructor, will
    ensure that no more than `max_activated` driver objects are active at the same time, by
    deactivating the LRU ones.

    On the fly re-projections in buzzard
    ------------------------------------
    A DataSource may perform spatial reference conversions on the fly, like a GIS does. Several
    modes are available, a set of rules define how each mode work. Those conversions concern both
    read operations and write operations, all are performed by the OSR library.

    Those conversions are only perfomed on vector's data/metadata and raster's Footprints.
    This implies that classic raster warping is not included (yet) in those conversions, only raster
    shifting/scaling/rotation work.

    The `z` coordinates of vectors geometries are also converted, on the other hand elevations are
    not converted in DEM rasters.

    If `analyse_transformation` is set to `True` (default), all coordinates conversions are
    tested against `buzz.env.significant` on file opening to ensure their feasibility or
    raise an exception otherwise. This system is naive and very restrictive, a smarter
    version is planned. Use with caution.

    ### Terminology
    `sr`: Spatial reference
    `sr_work`: The sr of all interactions with a DataSource (i.e. Footprints, extents, Polygons...),
        may be None.
    `sr_stored`: The sr that can be found in the metadata of a raster/vector storage, may be None.
    `sr_virtual`: The sr considered to be written in the metadata of a raster/vector storage, it is
        often the same as `sr_stored`. When a raster/vector is read, a conversion is performed from
        `sr_virtual` to `sr_work`. When writing vector data, a conversion is performed from
        `sr_work` to `sr_virtual`.
    `sr_forced`: A `sr_virtual` provided by user to ignore all `sr_stored`. This is for example
        useful when the `sr` stored in the input files are corrupted.
    `sr_fallback`: A `sr_virtual` provided by user to be used when `sr_stored` is missing. This is
        for example useful when an input file can't store a `sr` (e.g. DFX).

    ### DataSource parameters and modes
    | mode | sr_work | sr_fallback | sr_forced | How is the `sr_virtual` of a source determined                                  |
    |------|---------|-------------|-----------|---------------------------------------------------------------------------------|
    | 1    | None    | None        | None      | Use `sr_stored`, no conversion is performed for the lifetime of this DataSource |
    | 2    | string  | None        | None      | Use `sr_stored`, if None raises an exception                                    |
    | 3    | string  | string      | None      | Use `sr_stored`, if None it is considered to be `sr_fallback`                   |
    | 4    | string  | None        | string    | Use `sr_forced`                                                                 |

    ### Use cases
    - If all opened files are known to be written in a same sr in advance, use `mode 1`. No
        conversions will be performed, this is the safest way to work.
    - If all opened files are known to be written in the same sr but you wish to work in a different
        sr, use `mode 4`. The huge benefit of this mode is that the `driver` specific behaviors
        concerning spatial references have no impacts on the data you manipulate.
    - And the other hand if you don't have a priori information on files' `sr`, `mode 2` or
       `mode 3` should be used.
       Side note: Since the GeoJSON driver cannot store a `sr`, it is impossible to open or
           create a GeoJSON file in `mode 2`.

    ### Examples
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

    Scheduler
    ---------
    As soon as you create an *async raster*, a thread is spawned to manage requests made to the
    *async rasters*. It will live until the DataSource is closed or collected. If one of your script
    called by the scheduler raises an exception, the scheduler will stop and the exception will be
    propagated to the main thread as soon as possible.

    """

    def __init__(self, sr_work=None, sr_fallback=None, sr_forced=None,
                 analyse_transformation=True,
                 allow_none_geometry=False,
                 allow_interpolation=False,
                 max_active=np.inf,
                 debug_observers=(),
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
        self._ds_closed = False
        self._back = BackDataSource(
            wkt_work=sr_work,
            wkt_fallback=sr_fallback,
            wkt_forced=sr_forced,
            analyse_transformation=analyse_transformation,
            allow_none_geometry=allow_none_geometry,
            allow_interpolation=allow_interpolation,
            max_active=max_active,
            ds_id=id(self),
            debug_observers=debug_observers,
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
        if not isinstance(key, _AnonymousSentry):
            self._register([key], prox)
        else:
            self._register([], prox)
        return prox

    def aopen_raster(self, path, driver='GTiff', options=(), mode='r'):
        """Open a raster file anonymously in this DataSource. Only metadata are kept in memory.

        See DataSource.open_raster

        Example
        ------
        >>> ortho = ds.aopen_raster('/path/to/ortho.tif')
        >>> file_wkt = ds.ortho.wkt_stored

        """
        return self.open_raster(_AnonymousSentry(), path, driver, options, mode)

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

        Example
        -------
        >>> ds.create_raster('out', 'output.tif', ds.dem.fp, 'float32', 1)
        >>> file_footprint = ds.out.fp

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

        Caveat
        ------
        When using the GTiff driver, specifying a `mask` or `interpretation` field may lead to unexpected results.

        """
        # Parameter checking ***************************************************
        path = str(path)
        if not isinstance(fp, Footprint): # pragma: no cover
            raise TypeError('`fp` should be a Footprint')
        dtype = np.dtype(dtype)
        band_count = int(band_count)
        if band_count <= 0:
            raise ValueError('`band_count` should be >0')
        band_schema = _tools.sanitize_band_schema(band_schema, band_count)
        driver = str(driver)
        options = [str(arg) for arg in options]
        if sr is not None:
            sr = osr.GetUserInputAsWKT(sr)

        if sr is not None:
            fp = self._back.convert_footprint(fp, sr)

        # Construction dispatch ************************************************
        if driver.lower() == 'mem':
            # TODO for 0.5.0: Check async_ is False
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
        if not isinstance(key, _AnonymousSentry):
            self._register([key], prox)
        else:
            self._register([], prox)
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
        return self.create_raster(_AnonymousSentry(), path, fp, dtype, band_count, band_schema,
                                  driver, options, sr)

    def wrap_numpy_raster(self, key, fp, array, band_schema=None, sr=None, mode='w'):
        """Register a numpy array as a raster under `key` in this DataSource.

        Parameters
        ----------
        key: hashable (like a string)
            File identifier within DataSource
        fp: Footprint of shape (Y, X)
            Description of the location and size of the raster to create.
        array: ndarray of shape (Y, X) or (Y, X, C)
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
        if not isinstance(key, _AnonymousSentry):
            self._register([key], prox)
        else:
            self._register([], prox)
        return prox

    def awrap_numpy_raster(self, fp, array, band_schema=None, sr=None, mode='w'):
        """Register a numpy array as a raster anonymously in this DataSource.

        See DataSource.wrap_numpy_raster
        """
        return self.wrap_numpy_raster(_AnonymousSentry(), fp, array, band_schema, sr, mode)

    def create_raster_recipe(
            self, key,

            # raster attributes
            fp, dtype, band_count, band_schema=None, sr=None,

            # callbacks running on pool
            compute_array=None, merge_arrays=buzzard.utils.concat_arrays,

            # primitives
            queue_data_per_primitive={}, convert_footprint_per_primitive=None,

            # pools
            computation_pool='cpu', merge_pool='cpu', resample_pool='cpu',

            # misc
            computation_tiles=None, max_computation_size=None,
            max_resampling_size=None, automatic_remapping=True,
            debug_observers=()
    ):
        """Create a *raster recipe* and register it under `key` in this DataSource.

        A *raster recipe* implements the same interfaces as all other rasters, but internally it
        computes data on the fly by calling a callback. The main goal of the *raster recipes* is to
        provide a boilerplate-free interface that automatize those cumbersome tasks: tiling,
        parallelism, caching, file reads, resampling, lazy evaluation, backpressure prevention and
        optimised task scheduling.

        If you are familiar with `create_cached_raster_recipe` two parameters are new here:
        `automatic_remapping` and `max_computation_size`.

        Parameters
        ----------
        key:
            see `create_raster` method
        fp:
            see `create_raster` method
        dtype:
            see `create_raster` method
        band_count:
            see `create_raster` method
        band_schema:
            see `create_raster` method
        sr:
            see `create_raster` method
        compute_array: callable
            see `Computation Function` below
        merge_arrays: callable
            see `Merge Function` below
        queue_data_per_primitive: dict of hashable (like a string) to a `queue_data` method pointer
            see `Primitives` below
        convert_footprint_per_primitive: None or dict of hashable (like a string) to a callable
            see `Primitives` below
        computation_pool:
            see `Pools` below
        merge_pool:
            see `Pools` below
        resample_pool:
            see `Pools` below
        computation_tiles: None or (int, int) or numpy.ndarray of Footprint
            see `Computation Tiling` below
        max_computation_size:  None or int or (int, int)
            see `Computation Tiling` below
        max_resampling_size: None or int or (int, int)
            Optionally define a maximum resampling size. If a larger resampling has to be performed,
            it will be performed tile by tile in parallel.
        automatic_remapping: bool
            see `Automatic Remapping` below
        debug_observers: sequence of object
            Entry points to observe what is happening with this raster in the DataSource's sheduler.

        Returns
        -------
        NocacheRasterRecipe

        Computation Function
        --------------------
        The function that will map a Footprint to a numpy.ndarray. If `queue_data_per_primitive`
        is not empty, it will map a Footprint and primitive arrays to a numpy.ndarray.

        It will be called in parallel according to the `computation_pool` parameter provided at
        construction.

        The function will be called with the following positional parameters:
        - fp: Footprint of shape (Y, X)
            The location at which the pixels should be computed
        - primitive_fps: dict of hashable to Footprint
            For each primitive defined through the `queue_data_per_primitive` parameter, the input
            Footprint.
        - primitive_arrays: dict of hashable to numpy.ndarray
            For each primitive defined through the `queue_data_per_primitive` parameter, the input
            numpy.ndarray that was automatically computed.
        - raster: CachedRasterRecipe or None
            The Raster object of the ongoing computation.

        It should return either:
        - a single ndarray of shape (Y, X) if only one band was computed
        - a single ndarray of shape (Y, X, C) if one or more bands were computed

        If `computation_pool` points to a process pool, the `compute_array` function must be
        picklable and the `raster` parameter will be None.

        Computation Tiling
        ------------------
        You may sometimes want to have control on the Footprints that are requested to the
        `compute_array` function, for exemple:
        - If pixels computed by `compute_array` are long to compute, you want to tile to increase
          parallelism.
        - If the `compute_array` function scales badly in term of memory or time, you want to tile
          to reduce complexity.
        - If `compute_array` can work only on certain Footprints, you want a hard constraint on the
          set of Footprint that can be queried from `compute_array`. (This may happen with
          *convolutional neural networks*)

        To do so use the `computation_tiles` or `max_computation_size` parameter (not both).

        If `max_computation_size` is provided, a Footprint to be computed will be tiled given this
        parameter.

        If `computation_tiles` is a numpy.ndarray of Footprint, it should be a tiling of the `fp`
        parameter, only the Footprints contained in this tiling will be asked to the
        `computation_tiles`.
        If `computation_tiles` is (int, int), a tiling will be constructed using Footprint.tile
        using those two ints.

        Merge Function
        --------------
        The function that will map several pairs of Footprint/numpy.ndarray to a single
        numpy.ndarray. If the `computation_tiles` is None, it will never be called.

        It will be called in parallel according to the `merge_pool` parameter provided at
        construction.

        The function will be called with the following positional parameters:
        - fp: Footprint of shape (Y, X)
            The location at which the pixels should be computed.
        - array_per_fp: dict of Footprint to numpy.ndarray
            The pairs of Footprint/numpy.ndarray of each arrays that were computed by
            `compute_array` and that overlap with `fp`.
        - raster: CachedRasterRecipe or None
            The Raster object of the ongoing computation.

        It should return either:
        - a single ndarray of shape (Y, X) if only one band was computed
        - a single ndarray of shape (Y, X, C) if one or more bands were computed

        If `merge_pool` points to a process pool, the `merge_array` function must be picklable and
        the `raster` parameter will be None.

        Automatic Remapping
        -------------------
        When creating a recipe you give a _Footprint_ through the `fp` parameter. When calling your
        `compute_array` function the scheduler will only ask for slices of `fp`. This means that the
        scheduler takes care of those boilerplate steps:
        - If you request a *Footprint* on a different grid in a `get_data()` call, the scheduler
          __takes care of resampling__ the outputs of your `compute_array` function.
        - If you request a *Footprint* partially or fully outside of the raster's extent, the
          scheduler will call your `compute_array` function to get the interior pixels and then
          __pad the output with nodata__.

        This system is flexible and can be deactivated by passing `automatic_remapping=False` to
        the constructor of a _NocacheRasterRecipe_, in this case the scheduler will call your
        `compute_array` function for any kind of _Footprint_; thus your function must be able to
        comply with any request.

        Primitives
        ----------
        The `queue_data_per_primitive` and `convert_footprint_per_primitive` parameters can be used
        to create dependencies between `dependee` *async rasters* and the *raster recipe* being
        created. The dependee/dependent relation is called primitive/derived throughout buzzard.
        A derived recipe can itself be the primitive of another raster. Pipelines of any depth and
        width can be instanciated that way.

        In `queue_data_per_primitive` you declare a `dependee` by giving it a key of your choice and
        the pointer to the `queue_data` method of `dependee` raster. You can parameterize the
        connection by *currying* the `band`, `dst_nodata`, `interpolation` and `max_queue_size`
        parameters using `functools.partial`.

        The `convert_footprint_per_primitive` dict should contain the same keys as
        `queue_data_per_primitive`. A value in the dict should be a function that maps as Footprint
        to another Footprint. It can be used for exemple to request larger rectangles of primitives
        data to compute a derived array.

        e.g. If the primitive raster is an `rgb` image, and the derived raster only need the green
        band but with a context of 10 additional pixels on all 4 sides:
        >>> derived = ds.create_raster_recipe(
        ...     # other parameters
        ...     queue_data_per_primitive={'green': functools.partial(primitive.queue_data, band=2)},
        ...     convert_footprint_per_primitive={'green': lambda fp: fp.dilate(10)},
        ... )

        Pools
        -----
        The `*_pool` parameters can be used to select where certain computation occur. Those
        parameters can be of the following types:
        - A _multiprocessing.pool.ThreadPool_, should be the default choice.
        - A _multiprocessing.pool.Pool_, a process pool. Useful for computations that requires the
          GIL or that leaks memory.
        - `None`, to request the scheduler thread to perform the tasks itself. Should be used when
          the computation is very light.
        - A _hashable_ (like a _string_), that will map to a pool registered in the _DataSource_. If
          that key is missing from the _DataSource_, a _ThreadPool_ with
          `multiprocessing.cpu_count()` workers will be automatically instanciated. When the
          DataSource is closed, the pools instanciated that way will be joined.
        """
        assert False, 'TODO for 0.5.0: implement'

    def create_cached_raster_recipe(
            self, key,

            # raster attributes
            fp, dtype, band_count, band_schema=None, sr=None,

            # callbacks running on pool
            compute_array=None, merge_arrays=buzzard.utils.concat_arrays,

            # filesystem
            cache_dir=None, ow=False,

            # primitives
            queue_data_per_primitive={}, convert_footprint_per_primitive=None,

            # pools
            computation_pool='cpu', merge_pool='cpu', io_pool='io', resample_pool='cpu',

            # misc
            cache_tiles=(512, 512), computation_tiles=None, max_resampling_size=None,
            debug_observers=()
    ):
        """Create a *cached raster recipe* and register it under `key` in this DataSource.

        Compared to a `NocacheRasterRecipe`, in a `CachedRasterRecipe` the pixels are never computed
        twice. Cache files are used to store and reuse pixels from computations. The cache can even
        be reused between python sessions.

        If you are familiar with `create_raster_recipe` four parameters are new here: `io_pool`,
        `cache_tiles`, `cache_dir` and `ow`. They are all related to file system operations.

        see `create_raster_recipe` method, since it shares most of the features.

        Parameters
        ----------
        key:
            see `create_raster` method
        fp:
            see `create_raster` method
        dtype:
            see `create_raster` method
        band_count:
            see `create_raster` method
        band_schema:
            see `create_raster` method
        sr:
            see `create_raster` method
        compute_array:
            see `create_raster_recipe` method
        merge_arrays:
            see `create_raster_recipe` method
        cache_dir: str or pathlib.Path
            Path to the directory that holds the cache files associated with this raster. If cache
            files are present, they will be reused (or erased if corrupted). If a cache file is
            needed and missing, it will be computed.
        ow: bool
            Overwrite. Whether or not to erase the old cache files contained in `cache_dir`.
        queue_data_per_primitive:
            see `create_raster_recipe` method
        convert_footprint_per_primitive:
            see `create_raster_recipe` method
        computation_pool:
            see `create_raster_recipe` method
        merge_pool:
            see `create_raster_recipe` method
        io_pool:
            see `create_raster_recipe` method
        resample_pool:
            see `create_raster_recipe` method
        cache_tiles: (int, int) or numpy.ndarray of Footprint
            A tiling of the `fp` parameter. Each tile will correspond to one cache file.
            if (int, int): Construct the tiling by calling Footprint.tile with this parameter
        computation_tiles:
            if None: Use the same tiling as `cache_tiles`
            else: see `create_raster_recipe` method
        max_resampling_size: None or int or (int, int)
            see `create_raster_recipe` method
        debug_observers: sequence of object
            see `create_raster_recipe` method

        Returns
        -------
        CachedRasterRecipe

        """
        # Parameter checking ***************************************************
        # Classic RasterProxy parameters *******************
        if not isinstance(fp, Footprint): # pragma: no cover
            raise TypeError('`fp` should be a Footprint')
        dtype = np.dtype(dtype)
        band_count = int(band_count)
        if band_count <= 0:
            raise ValueError('`band_count` should be >0')
        band_schema = _tools.sanitize_band_schema(band_schema, band_count)
        if sr is not None:
            sr = osr.GetUserInputAsWKT(sr)
        if sr is not None:
            fp = self._back.convert_footprint(fp, sr)

        # Callables ****************************************
        if compute_array is None:
            raise ValueError('Missing `compute_array` parameter')
        if not callable(compute_array):
            raise TypeError('`compute_array` should be callable')
        if not callable(merge_arrays):
            raise TypeError('`merge_arrays` should be callable')

        # Primitives ***************************************
        if convert_footprint_per_primitive is None:
            convert_footprint_per_primitive = {
                name: (lambda fp: fp)
                for name in queue_data_per_primitive.keys()
            }

        if queue_data_per_primitive.keys() != convert_footprint_per_primitive.keys():
            err = 'There should be the same keys in `queue_data_per_primitive` and '
            err += '`convert_footprint_per_primitive`.'
            if queue_data_per_primitive.keys() - convert_footprint_per_primitive.keys():
                err += '\n{} are missing from `convert_footprint_per_primitive`.'.format(
                    queue_data_per_primitive.keys() - convert_footprint_per_primitive.keys()
                )
            if convert_footprint_per_primitive.keys() - queue_data_per_primitive.keys():
                err += '\n{} are missing from `queue_data_per_primitive`.'.format(
                    convert_footprint_per_primitive.keys() - queue_data_per_primitive.keys()
                )
            raise ValueError(err)

        primitives_back = {}
        primitives_kwargs = {}
        for name, met in queue_data_per_primitive.items():
            primitives_back[name], primitives_kwargs[name] = _tools.shatter_queue_data_method(met, name)
            if primitives_back[name].back_ds is not self._back:
                raise ValueError('The `{}` primitive comes from another DataSource'.format(
                    name
                ))

        for name, func in convert_footprint_per_primitive.items():
            if not callable(func):
                raise TypeError('convert_footprint_per_primitive[{}] should be callable'.format(
                    name
                ))

        # Pools ********************************************
        computation_pool = self._back.pools_container._normalize_pool_parameter(
            computation_pool, 'computation_pool'
        )
        merge_pool = self._back.pools_container._normalize_pool_parameter(
            merge_pool, 'merge_pool'
        )
        io_pool = self._back.pools_container._normalize_pool_parameter(
            io_pool, 'io_pool'
        )
        resample_pool = self._back.pools_container._normalize_pool_parameter(
            resample_pool, 'resample_pool'
        )

        # Tilings ******************************************
        if isinstance(cache_tiles, np.ndarray) and cache_tiles.dtype == np.object:
            if not _tools.is_tiling_covering_fp(
                    cache_tiles, fp,
                    allow_outer_pixels=False, allow_overlapping_pixels=False,
            ):
                raise ValueError("`cache_tiles` should be a tiling of raster's Footprint, " +\
                                "without overlap, with `boundary_effect='shrink'`"
                )
        else:
            # Defer the parameter checking to fp.tile
            cache_tiles = fp.tile(cache_tiles, 0, 0, boundary_effect='shrink')

        if computation_tiles is None:
            computation_tiles = cache_tiles
        elif isinstance(computation_tiles, np.ndarray) and computation_tiles.dtype == np.object:
            if not _tools.is_tiling_covering_fp(
                    cache_tiles, fp,
                    allow_outer_pixels=True, allow_overlapping_pixels=True,
            ):
                raise ValueError("`computation_tiles` should be a tiling covering raster's Footprint")
        else:
            # Defer the parameter checking to fp.tile
            computation_tiles = fp.tile(computation_tiles, 0, 0, boundary_effect='shrink')

        # Misc *********************************************
        if max_resampling_size is not None:
            max_resampling_size = int(max_resampling_size)
            if max_resampling_size <= 1:
                raise ValueError('`max_resampling_size` should be >0')

        if cache_dir is None:
            raise ValueError('Missing `cache_dir` parameter')
        if not isinstance(cache_dir, (str, pathlib.Path)):
            raise TypeError('cache_dir should be a string')
        cache_dir = str(cache_dir)
        overwrite = bool(ow)
        del ow

        # Construction *********************************************************
        prox = CachedRasterRecipe(
            self,
            fp, dtype, band_count, band_schema, sr,
            compute_array, merge_arrays,
            cache_dir, overwrite,
            primitives_back, primitives_kwargs, convert_footprint_per_primitive,
            computation_pool, merge_pool, io_pool, resample_pool,
            cache_tiles,computation_tiles,
            max_resampling_size,
            debug_observers,
        )

        # DataSource Registering ***********************************************
        if not isinstance(key, _AnonymousSentry):
            self._register([key], prox)
        else:
            self._register([], prox)
        return prox

    def acreate_cached_raster_recipe(
            self,

            # raster attributes
            fp, dtype, band_count, band_schema=None, sr=None,

            # callbacks running on pool
            compute_array=None, merge_arrays=buzzard.utils.concat_arrays,

            # filesystem
            cache_dir=None, ow=False,

            # primitives
            queue_data_per_primitive={}, convert_footprint_per_primitive=None,

            # pools
            computation_pool='cpu', merge_pool='cpu', io_pool='io', resample_pool='cpu',

            # misc
            cache_tiles=(512, 512), computation_tiles=None, max_resampling_size=None,
            debug_observers=()
    ):
        """Create a cached raster reciped anonymously in this DataSource.

        See DataSource.create_cached_raster_recipe
        """
        return self.create_cached_raster_recipe(
            _AnonymousSentry(),
            fp, dtype, band_count, band_schema, sr,
            compute_array, merge_arrays,
            cache_dir, ow,
            queue_data_per_primitive, convert_footprint_per_primitive,
            computation_pool, merge_pool, io_pool, resample_pool,
            cache_tiles, computation_tiles, max_resampling_size,
            debug_observers,
        )

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
        if not isinstance(key, _AnonymousSentry):
            self._register([key], prox)
        else:
            self._register([], prox)
        return prox

    def aopen_vector(self, path, layer=None, driver='ESRI Shapefile', options=(), mode='r'):
        """Open a vector file anonymously in this DataSource. Only metadata are kept in memory.

        See DataSource.open_vector

        Example
        -------
        >>> trees = ds.aopen_vector('/path/to.shp')
        >>> features_bounds = trees.bounds

        """
        return self.open_vector(_AnonymousSentry(), path, layer, driver, options, mode)

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
            # TODO for 0.5.0: Check async_ is False
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
        if not isinstance(key, _AnonymousSentry):
            self._register([key], prox)
        else:
            self._register([], prox)
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
        return self.create_vector(_AnonymousSentry(), path, geometry, fields, layer,
                                  driver, options, sr)

    # Proxy infos ******************************************************************************* **
    def __getitem__(self, key):
        """Retrieve a proxy from its key"""
        return self._proxy_of_key[key]

    def __contains__(self, item):
        """Is key or proxy registered in DataSource"""
        if isinstance(item, AProxy):
            return item in self._keys_of_proxy
        return item in self._proxy_of_key

    def items(self):
        """Generate the pair of (keys_of_proxy, proxy) for all proxies"""
        for proxy, keys in self._keys_of_proxy.items():
            yield list(keys), proxy

    def values(self):
        """Generate all proxies"""
        for proxy, _ in self._keys_of_proxy.items():
            yield proxy

    def __len__(self):
        """Retrieve proxy count registered in this DataSource"""
        return len(self._keys_of_proxy)

    # Pools infos ******************************************************************************* **
    @property
    def pools(self):
        return self._back.pools_container

    # Cleanup *********************************************************************************** **
    def __del__(self):
        if not self._ds_closed:
            self.close()

    @property
    def close(self):
        """Close the DataSource with a call or a context management.
        The `close` attribute returns an object that can be both called and used in a with statement

        The DataSource can be closed manually or automatically when garbage collected, it is safer
        to do it manually. The steps are:
        - Stopping the scheduler
        - Joining the mp.Pool that have been automatically allocated
        - Close all sources

        Examples
        --------
        >>> ds = buzz.DataSource()
        ... # code...
        ... ds.close()

        >>> with buzz.DataSource().close as ds
        ...     # code...

        Caveat
        ------
        When using a scheduler, some memory leaks may still occur after closing. Possible origins:
        - https://bugs.python.org/issue34172 (update your python to >=3.6.7)
        - Gdal cache not flushed (not a leak)
        - The gdal version
        - https://stackoverflow.com/a/1316799 (not a leak)
        - Some unknown leak in the python `threading` or `multiprocessing` standard library
        - Some unknown library leaking memory on the `C` side
        - Some unknown library storing data in global variables

        You can use a `debug_observer` with an `on_object_allocated` method to track large objects
        allocated in the scheduler. It will most likely not be the source of the problem. If you
        even find a source of leaks please contact the buzzard team.
        https://github.com/airware/buzzard/issues

        """
        if self._ds_closed:
            raise RuntimeError("DataSource already closed")

        def _close():
            if self._ds_closed:
                raise RuntimeError("DataSource already closed")
            self._ds_closed = True

            # Tell scheduler to stop, wait until it is done
            self._back.stop_scheduler()

            # Safely release all resources
            self._back.pools_container._close()
            for proxy in list(self._keys_of_proxy.keys()):
                proxy.close()

        return _CloseRoutine(self, _close)

    # Spatial reference getters ***************************************************************** **
    @property
    def proj4(self):
        """DataSource's work spatial reference in WKT proj4.
        Returns None if `mode 1`.
        """
        if self._back.wkt_work is None:
            return None
        return osr.SpatialReference(self._back.wkt_work).ExportToProj4()

    @property
    def wkt(self):
        """DataSource's work spatial reference in WKT format.
        Returns None if `mode 1`.
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
        proxs = [
            prox
            for prox in self._keys_of_proxy.keys()
            if isinstance(prox, APooledEmissary)
        ]
        total = len(proxs)

        if self._back.max_active < total:
            raise RuntimeError("Can't activate all pooled sources at the same time: {} pooled sources and max_activated is {}".format(
                total, self._back.max_active,
            ))

        # Hacky implementation to get the expected behavior
        # TODO: Implement that routine in the back driver pool. It is possible? We need to call `.activate`
        i = 0
        for prox in itertools.cycle(proxs):
            if i == total:
                break
            if not prox.active:
                prox.activate()
                i = 1
            else:
                i += 1

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

_CloseRoutine = type('_CloseRoutine', (_tools.CallOrContext,), {
    '__doc__': DataSource.close.__doc__,
})

class _AnonymousSentry(object):
    pass
