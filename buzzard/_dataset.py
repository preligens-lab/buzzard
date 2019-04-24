""">>> help(buzz.Dataset)"""

# pylint: disable=too-many-lines
import sys
import pathlib
import itertools
from types import MappingProxyType
import os

from osgeo import osr
import numpy as np

from buzzard._tools import conv, deprecation_pool
from buzzard._tools import GDALErrorCatcher as Catch

from buzzard._footprint import Footprint
from buzzard import _tools
from buzzard._dataset_back import BackDataset
from buzzard._a_source import ASource
from buzzard._gdal_file_raster import GDALFileRaster, BackGDALFileRaster
from buzzard._gdal_file_vector import GDALFileVector, BackGDALFileVector
from buzzard._gdal_mem_raster import GDALMemRaster
from buzzard._gdal_memory_vector import GDALMemoryVector
from buzzard._dataset_register import DatasetRegisterMixin
from buzzard._numpy_raster import NumpyRaster
from buzzard._cached_raster_recipe import CachedRasterRecipe
from buzzard._a_pooled_emissary import APooledEmissary
import buzzard.utils

class Dataset(DatasetRegisterMixin):
    """Dataset is a class that stores references to sources. A source is either a raster, or a
    vector. It allows quick manipulations by assigning a key to each registered source. It also
    allows inter-sources operations, like:
    - spatial reference harmonization (see `On the fly re-projections in buzzard` below)
    - workload scheduling on pools when using async rasters (see `Scheduler` below)
    - other features in the future (like data visualization)

    For actions specific to opened sources, see those classes:
    - GDALFileRaster
    - GDALMemRaster
    - NumpyRaster
    - CachedRasterRecipe
    - GDALFileVector
    - GDALMemoryVector

    /!\ This class is not equivalent to the `gdal.Dataset` class.

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
        Entry points to observe what is happening in the Dataset's sheduler.

    Example
    -------
    >>> import buzzard as buzz

    Creating a Dataset
    >>> ds = buzz.Dataset()

    Opening a file and registering it under the 'roofs' key
    >>> r = ds.open_vector('roofs', 'path/to/roofs.shp')
    ... feature_count = len(ds.roofs)
    ... feature_count = len(ds['roofs'])
    ... feature_count = len(ds.get('roofs'))
    ... feature_count = len(r)

    Opening a file anonymously
    >>> r = ds.aopen_raster('path/to/dem.tif')
    ... data_type = r.dtype

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
    - from the Dataset with the attribute syntax: `ds.my_source_name`,
    - from the Dataset with the item syntax: ds["my_source_name"].
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
    constraints. Setting a `max_activated` lower than `np.inf` in the Dataset constructor, will
    ensure that no more than `max_activated` driver objects are active at the same time, by
    deactivating the LRU ones.

    On the fly re-projections in buzzard
    ------------------------------------
    A Dataset may perform spatial reference conversions on the fly, like a GIS does. Several
    modes are available, a set of rules define how each mode work. Those conversions concern both
    read operations and write operations, all are performed by the OSR library.

    Those conversions are only perfomed on vector's data/metadata and raster's Footprints.
    This implies that classic raster warping is not included (yet) in those conversions, only raster
    shifting/scaling/rotation work.

    The `z` coordinates of vectors geometries are also converted, on the other hand elevations are
    not converted in DEM rasters.

    If `analyse_transformation` is set to `True` (default), all coordinates conversions are
    tested against `buzz.env.significant` on file opening to ensure their feasibility or
    raise an exception otherwise. This system is naive and very restrictive, use with caution.
    Although, disabling those tests is not recommended, ignoring floating point precision errors
    can create unpredictable behaviors at the pixel level deep in your code. Those bugs can be
    witnessed when zooming to infinity with tools like `qgis` or `matplotlib`.

    ### Terminology
    `sr`: Spatial reference
    `sr_work`: The sr of all interactions with a Dataset (i.e. Footprints, extents, Polygons...),
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

    ### Dataset parameters and modes
    | mode | sr_work | sr_fallback | sr_forced | How is the `sr_virtual` of a source determined                                  |
    |------|---------|-------------|-----------|---------------------------------------------------------------------------------|
    | 1    | None    | None        | None      | Use `sr_stored`, no conversion is performed for the lifetime of this Dataset |
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
    >>> ds = buzz.Dataset()

    mode 2 - Working with WGS84 coordinates
    >>> ds = buzz.Dataset(
            sr_work='WGS84',
        )

    mode 3 - Working in UTM with DXF files in WGS84 coordinates
    >>> ds = buzz.Dataset(
            sr_work='EPSG:32632',
            sr_fallback='WGS84',
        )

    mode 4 - Working in UTM with unreliable LCC input files
    >>> ds = buzz.Dataset(
            sr_work='EPSG:32632',
            sr_forced='EPSG:27561',
        )

    Scheduler
    ---------
    To handle *async rasters* living in a Dataset, a thread is to manage requests made to those
    rasters. It will start as soon as you create an *async raster* and stop when the Dataset is
    closed or collected. If one of your callbacks to be called by the scheduler raises an exception,
    the scheduler will stop and the exception will be propagated to the main thread as soon as
    possible.

    Thread-safety
    -------------
    Thread safety is one of the main concern of buzzard. Everything is thread-safe except:
    - The raster write methods
    - The vector write methods
    - The raster read methods when using the GDAL::MEM driver
    - The vector read methods when using the GDAL::Memory driver

    """

    def __init__(self, sr_work=None, sr_fallback=None, sr_forced=None,
                 analyse_transformation=True,
                 allow_none_geometry=False,
                 allow_interpolation=False,
                 max_active=np.inf,
                 debug_observers=(),
                 **kwargs):
        sr_fallback, kwargs = deprecation_pool.handle_param_renaming_with_kwargs(
            new_name='sr_fallback', old_names={'sr_implicit': '0.4.4'}, context='Dataset.__init__',
            new_name_value=sr_fallback,
            new_name_is_provided=sr_fallback is not None,
            user_kwargs=kwargs,
        )
        sr_forced, kwargs = deprecation_pool.handle_param_renaming_with_kwargs(
            new_name='sr_forced', old_names={'sr_origin': '0.4.4'}, context='Dataset.__init__',
            new_name_value=sr_forced,
            new_name_is_provided=sr_forced is not None,
            user_kwargs=kwargs,
        )
        max_active, kwargs = deprecation_pool.handle_param_renaming_with_kwargs(
            new_name='max_active', old_names={'max_activated': '0.5.0'}, context='Dataset.__init__',
            new_name_value=max_active,
            new_name_is_provided=max_active != np.inf,
            user_kwargs=kwargs,
        )
        if kwargs: # pragma: no cover
            raise TypeError("__init__() got an unexpected keyword argument '{}'".format(
                list(kwargs.keys())[0]
            ))

        mode = (sr_work is not None, sr_fallback is not None, sr_forced is not None)
        wkt_work, wkt_fallback, wkt_forced = None, None, None
        if mode == (False, False, False):
            pass
        elif mode == (True, False, False):
            success, payload = Catch(osr.GetUserInputAsWKT, nonzero_int_is_error=True)(sr_work)
            if not success:
                raise ValueError('Could not transform `sr_work` to `wkt` (gdal error: `{}`)'.format(
                    payload[1]
                ))
            wkt_work = payload
        elif mode == (True, True, False):
            success, payload = Catch(osr.GetUserInputAsWKT, nonzero_int_is_error=True)(sr_work)
            if not success:
                raise ValueError('Could not transform `sr_work` to `wkt` (gdal error: `{}`)'.format(
                    payload[1]
                ))
            wkt_work = payload
            success, payload = Catch(osr.GetUserInputAsWKT, nonzero_int_is_error=True)(sr_fallback)
            if not success:
                raise ValueError('Could not transform `sr_fallback` to `wkt` (gdal error: `{}`)'.format(
                    payload[1]
                ))
            wkt_fallback = payload
        elif mode == (True, False, True):
            success, payload = Catch(osr.GetUserInputAsWKT, nonzero_int_is_error=True)(sr_work)
            if not success:
                raise ValueError('Could not transform `sr_work` to `wkt` (gdal error: `{}`)'.format(
                    payload[1]
                ))
            wkt_work = payload
            success, payload = Catch(osr.GetUserInputAsWKT, nonzero_int_is_error=True)(sr_forced)
            if not success:
                raise ValueError('Could not transform `sr_forced` to `wkt` (gdal error: `{}`)'.format(
                    payload[1]
                ))
            wkt_forced = payload
        else:
            raise ValueError('Bad combination of `sr_*` parameters') # pragma: no cover
        del sr_work, sr_fallback, sr_forced

        if max_active < 1: # pragma: no cover
            raise ValueError('`max_active` should be greater than 1')

        allow_interpolation = bool(allow_interpolation)
        allow_none_geometry = bool(allow_none_geometry)
        analyse_transformation = bool(analyse_transformation)
        self._ds_closed = False
        self._back = BackDataset(
            wkt_work=wkt_work,
            wkt_fallback=wkt_fallback,
            wkt_forced=wkt_forced,
            analyse_transformation=analyse_transformation,
            allow_none_geometry=allow_none_geometry,
            allow_interpolation=allow_interpolation,
            max_active=max_active,
            ds_id=id(self),
            debug_observers=debug_observers,
        )
        super(Dataset, self).__init__()

    # Raster entry points *********************************************************************** **
    def open_raster(self, key, path, driver='GTiff', options=(), mode='r'):
        """Open a raster file in this Dataset under `key`. Only metadata are kept in memory.

        >>> help(GDALFileRaster)

        Parameters
        ----------
        key: hashable (like a string)
            File identifier within Dataset
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

        # Dataset Registering ***********************************************
        if not isinstance(key, _AnonymousSentry):
            self._register([key], prox)
        else:
            self._register([], prox)
        return prox

    def aopen_raster(self, path, driver='GTiff', options=(), mode='r'):
        """Open a raster file anonymously in this Dataset. Only metadata are kept in memory.

        See Dataset.open_raster

        Example
        ------
        >>> ortho = ds.aopen_raster('/path/to/ortho.tif')
        >>> file_wkt = ortho.wkt_stored

        """
        return self.open_raster(_AnonymousSentry(), path, driver, options, mode)

    def create_raster(self, key, path, fp, dtype, channel_count, channels_schema=None,
                      driver='GTiff', options=(), sr=None, ow=False, **kwargs):
        """Create a raster file and register it under `key` in this Dataset. Only metadata are
        kept in memory.

        The raster's values are initialized with `channels_schema['nodata']` or `0`.

        >>> help(GDALFileRaster)
        >>> help(GDALMemRaster)

        Parameters
        ----------
        key: hashable (like a string)
            File identifier within Dataset
        path: string
        fp: Footprint
            Description of the location and size of the raster to create.
        dtype: numpy type (or any alias)
        channel_count: integer
            number of channels
        channels_schema: dict or None
            Channel(s) metadata. (see `Channels schema fields` below)
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
        ow: bool
            Overwrite. Whether or not to erase the existing files.

        Example
        -------
        >>> ds.create_raster('dem_copy', 'dem_copy.tif', ds.dem.fp, ds.dsm.dtype, len(ds.dem))
        >>> array = ds.dem.get_data()
        >>> ds.dem_copy.set_data(array)

        Returns
        -------
        one of {GDALFileRaster, GDALMemRaster}
            depending on the `driver` parameter

        Channel schema fields
        ---------------------
        Fields:
            'nodata': None or number
            'interpretation': None or str
            'offset': None or number
            'scale': None or number
            'mask': None or str
        Interpretation values:
            undefined, grayindex, paletteindex, redband, greenband, blueband, alphaband, hueband,
            saturationband, lightnessband, cyanband, magentaband, yellowband, blackband
        Mask values:
            all_valid, per_dataset, alpha, nodata

        A field missing or None is kept to default value.
        A field can be passed as:
            a value: All bands are set to this value
            a sequence of length `channel_count` of value: All bands will be set to respective state

        Caveat
        ------
        When using the GTiff driver, specifying a `mask` or `interpretation` field may lead to unexpected results.

        """

        # Deprecated parameters ************************************************
        channels_schema, kwargs = deprecation_pool.handle_param_renaming_with_kwargs(
            new_name='channels_schema', old_names={'band_schema': '0.6.0'},
            context='Dataset.create_raster',
            new_name_value=channels_schema,
            new_name_is_provided=channels_schema is not None,
            user_kwargs=kwargs,
        )
        if kwargs: # pragma: no cover
            raise TypeError("create_raster() got an unexpected keyword argument '{}'".format(
                list(kwargs.keys())[0]
            ))

        # Parameter checking ***************************************************
        ow = bool(ow)
        path = str(path)
        if not isinstance(fp, Footprint): # pragma: no cover
            raise TypeError('`fp` should be a Footprint')
        dtype = np.dtype(dtype)
        channel_count = int(channel_count)
        if channel_count <= 0:
            raise ValueError('`channel_count` should be >0')
        channels_schema = _tools.sanitize_channels_schema(channels_schema, channel_count)
        driver = str(driver)
        options = [str(arg) for arg in options]

        if sr is not None:
            success, payload = Catch(osr.GetUserInputAsWKT, nonzero_int_is_error=True)(sr)
            if not success:
                raise ValueError('Could not transform `sr` to `wkt` (gdal error: `{}`)'.format(
                    payload[1]
                ))
            wkt = payload
        else:
            wkt = None
        del sr

        if wkt is not None:
            fp = self._back.convert_footprint(fp, wkt)

        # Construction dispatch ************************************************
        if driver.lower() == 'mem':
            # TODO for 0.5.0: Check async_ is False
            prox = GDALMemRaster(
                self, fp, dtype, channel_count, channels_schema, options, wkt,
            )
        elif True:
            allocator = lambda: BackGDALFileRaster.create_file(
                path, fp, dtype, channel_count, channels_schema, driver, options, wkt, ow,
            )
            prox = GDALFileRaster(self, allocator, options, 'w')
        else:
            pass

        # Dataset Registering ***********************************************
        if not isinstance(key, _AnonymousSentry):
            self._register([key], prox)
        else:
            self._register([], prox)
        return prox

    def acreate_raster(self, path, fp, dtype, channel_count, channels_schema=None,
                       driver='GTiff', options=(), sr=None, ow=False, **kwargs):
        """Create a raster file anonymously in this Dataset. Only metadata are kept in memory.

        See Dataset.create_raster

        Example
        -------
        >>> mask = ds.acreate_raster('mask.tif', ds.dem.fp, bool, 1, options=['SPARSE_OK=YES'])
        >>> open_options = mask.open_options

        >>> channels_schema = {
        ...     'nodata': -32767,
        ...     'interpretation': ['blackband', 'cyanband'],
        ... }
        >>> out = ds.acreate_raster('output.tif', ds.dem.fp, 'float32', 2, channels_schema)
        >>> band_interpretation = out.channels_schema['interpretation']

        """
        return self.create_raster(_AnonymousSentry(), path, fp, dtype, channel_count, channels_schema,
                                  driver, options, sr, ow, **kwargs)

    def wrap_numpy_raster(self, key, fp, array, channels_schema=None, sr=None, mode='w', **kwargs):
        """Register a numpy array as a raster under `key` in this Dataset.

        >>> help(NumpyRaster)

        Parameters
        ----------
        key: hashable (like a string)
            File identifier within Dataset
        fp: Footprint of shape (Y, X)
            Description of the location and size of the raster to create.
        array: ndarray of shape (Y, X) or (Y, X, C)
        channels_schema: dict or None
            Channel(s) metadata. (see `Channels schema fields` below)
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

        Channel schema fields
        ---------------------
        Fields:
            'nodata': None or number
            'interpretation': None or str
            'offset': None or number
            'scale': None or number
            'mask': None or str
        Interpretation values:
            undefined, grayindex, paletteindex, redband, greenband, blueband, alphaband, hueband,
            saturationband, lightnessband, cyanband, magentaband, yellowband, blackband
        Mask values:
            all_valid, per_dataset, alpha, nodata

        A field missing or None is kept to default value.
        A field can be passed as:
            a value: All bands are set to this value
            a sequence of length `channel_count` of value: All bands will be set to respective state

        """

        # Deprecated parameters ************************************************
        channels_schema, kwargs = deprecation_pool.handle_param_renaming_with_kwargs(
            new_name='channels_schema', old_names={'band_schema': '0.6.0'},
            context='Dataset.wrap_numpy_raster',
            new_name_value=channels_schema,
            new_name_is_provided=channels_schema is not None,
            user_kwargs=kwargs,
        )
        if kwargs: # pragma: no cover
            raise TypeError("wrap_numpy_raster() got an unexpected keyword argument '{}'".format(
                list(kwargs.keys())[0]
            ))

        # Parameter checking ***************************************************
        if not isinstance(fp, Footprint): # pragma: no cover
            raise TypeError('`fp` should be a Footprint')
        array = np.asarray(array)
        if array.shape[:2] != tuple(fp.shape): # pragma: no cover
            raise ValueError('Incompatible shape between `array` and `fp`')
        if array.ndim not in [2, 3]: # pragma: no cover
            raise ValueError('Array should have 2 or 3 dimensions')
        channel_count = 1 if array.ndim == 2 else array.shape[-1]
        channels_schema = _tools.sanitize_channels_schema(channels_schema, channel_count)
        if sr is not None:
            success, payload = Catch(osr.GetUserInputAsWKT, nonzero_int_is_error=True)(sr)
            if not success:
                raise ValueError('Could not transform `sr` to `wkt` (gdal error: `{}`)'.format(
                    payload[1]
                ))
            wkt = payload
        else:
            wkt = None
        del sr
        _ = conv.of_of_mode(mode)

        if wkt is not None:
            fp = self._back.convert_footprint(fp, wkt)

        # Construction *********************************************************
        prox = NumpyRaster(self, fp, array, channels_schema, wkt, mode)

        # Dataset Registering ***********************************************
        if not isinstance(key, _AnonymousSentry):
            self._register([key], prox)
        else:
            self._register([], prox)
        return prox

    def awrap_numpy_raster(self, fp, array, channels_schema=None, sr=None, mode='w', **kwargs):
        """Register a numpy array as a raster anonymously in this Dataset.

        See Dataset.wrap_numpy_raster
        """
        return self.wrap_numpy_raster(
            _AnonymousSentry(), fp, array, channels_schema, sr, mode, **kwargs
        )

    def create_raster_recipe(
            self, key,

            # raster attributes
            fp, dtype, channel_count, channels_schema=None, sr=None,

            # callbacks running on pool
            compute_array=None, merge_arrays=buzzard.utils.concat_arrays,

            # primitives
            queue_data_per_primitive=MappingProxyType({}), convert_footprint_per_primitive=None,

            # pools
            computation_pool='cpu', merge_pool='cpu', resample_pool='cpu',

            # misc
            computation_tiles=None, max_computation_size=None,
            max_resampling_size=None, automatic_remapping=True,
            debug_observers=(),
    ):
        """/!\ This method is not yet implemented. It is here for documentation purposes.

        Create a *raster recipe* and register it under `key` in this Dataset.

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
        channel_count:
            see `create_raster` method
        channels_schema:
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
            Entry points that observe what is happening with this raster in the Dataset's scheduler.

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
        - a single ndarray of shape (Y, X) if only one channel was computed
        - a single ndarray of shape (Y, X, C) if one or more channels were computed

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
        parameter. Only the Footprints contained in this tiling will be asked to the
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
        - a single ndarray of shape (Y, X) if only one channel was computed
        - a single ndarray of shape (Y, X, C) if one or more channels were computed

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
        connection by *currying* the `channels`, `dst_nodata`, `interpolation` and `max_queue_size`
        parameters using `functools.partial`.

        The `convert_footprint_per_primitive` dict should contain the same keys as
        `queue_data_per_primitive`. A value in the dict should be a function that maps a Footprint
        to another Footprint. It can be used for example to request larger rectangles of primitives
        data to compute a derived array.

        e.g. If the primitive raster is an `rgb` image, and the derived raster only needs the green
        channel but with a context of 10 additional pixels on all 4 sides:
        >>> derived = ds.create_raster_recipe(
        ...     # <other parameters>
        ...     queue_data_per_primitive={'green': functools.partial(primitive.queue_data, channels=1)},
        ...     convert_footprint_per_primitive={'green': lambda fp: fp.dilate(10)},
        ... )

        Pools
        -----
        The `*_pool` parameters can be used to select where certain computations occur. Those
        parameters can be of the following types:
        - A _multiprocessing.pool.ThreadPool_, should be the default choice.
        - A _multiprocessing.pool.Pool_, a process pool. Useful for computations that requires the
          GIL or that leaks memory.
        - `None`, to request the scheduler thread to perform the tasks itself. Should be used when
          the computation is very light.
        - A _hashable_ (like a _string_), that will map to a pool registered in the _Dataset_. If
          that key is missing from the _Dataset_, a _ThreadPool_ with
          `multiprocessing.cpu_count()` workers will be automatically instanciated. When the
          Dataset is closed, the pools instanciated that way will be joined.
        """
        raise NotImplementedError()

    def create_cached_raster_recipe(
            self, key,

            # raster attributes
            fp, dtype, channel_count, channels_schema=None, sr=None,

            # callbacks running on pool
            compute_array=None, merge_arrays=buzzard.utils.concat_arrays,

            # filesystem
            cache_dir=None, ow=False,

            # primitives
            queue_data_per_primitive=MappingProxyType({}), convert_footprint_per_primitive=None,

            # pools
            computation_pool='cpu', merge_pool='cpu', io_pool='io', resample_pool='cpu',

            # misc
            cache_tiles=(512, 512), computation_tiles=None, max_resampling_size=None,
            debug_observers=()
    ):
        """Create a *cached raster recipe* and register it under `key` in this Dataset.

        Compared to a `NocacheRasterRecipe`, in a `CachedRasterRecipe` the pixels are never computed
        twice. Cache files are used to store and reuse pixels from computations. The cache can even
        be reused between python sessions.

        If you are familiar with `create_raster_recipe` four parameters are new here: `io_pool`,
        `cache_tiles`, `cache_dir` and `ow`. They are all related to file system operations.

        see `create_raster_recipe` method, since it shares most of the features.

        >>> help(CachedRasterRecipe)

        Parameters
        ----------
        key:
            see `create_raster` method
        fp:
            see `create_raster` method
        dtype:
            see `create_raster` method
        channel_count:
            see `create_raster` method
        channels_schema:
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
            Overwrite. Whether or not to erase the old cache files contained in `cache_dir`. Warning: not only the tiles needed (hence computed) but all cached files in `cache_dir` will be deleted.
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
        # Classic RasterSource parameters *******************
        if not isinstance(fp, Footprint): # pragma: no cover
            raise TypeError('`fp` should be a Footprint')
        dtype = np.dtype(dtype)
        channel_count = int(channel_count)
        if channel_count <= 0:
            raise ValueError('`channel_count` should be >0')
        channels_schema = _tools.sanitize_channels_schema(channels_schema, channel_count)
        if sr is not None:
            success, payload = Catch(osr.GetUserInputAsWKT, nonzero_int_is_error=True)(sr)
            if not success:
                raise ValueError('Could not transform `sr` to `wkt` (gdal error: `{}`)'.format(
                    payload[1]
                ))
            wkt = payload
        else:
            wkt = None
        del sr
        if wkt is not None:
            fp = self._back.convert_footprint(fp, wkt)

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
                raise ValueError('The `{}` primitive comes from another Dataset'.format(
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
            if max_resampling_size <= 0:
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
            fp, dtype, channel_count, channels_schema, wkt,
            compute_array, merge_arrays,
            cache_dir, overwrite,
            primitives_back, primitives_kwargs, convert_footprint_per_primitive,
            computation_pool, merge_pool, io_pool, resample_pool,
            cache_tiles, computation_tiles,
            max_resampling_size,
            debug_observers,
        )

        # Dataset Registering ***********************************************
        if not isinstance(key, _AnonymousSentry):
            self._register([key], prox)
        else:
            self._register([], prox)
        return prox

    def acreate_cached_raster_recipe(
            self,

            # raster attributes
            fp, dtype, channel_count, channels_schema=None, sr=None,

            # callbacks running on pool
            compute_array=None, merge_arrays=buzzard.utils.concat_arrays,

            # filesystem
            cache_dir=None, ow=False,

            # primitives
            queue_data_per_primitive=MappingProxyType({}), convert_footprint_per_primitive=None,

            # pools
            computation_pool='cpu', merge_pool='cpu', io_pool='io', resample_pool='cpu',

            # misc
            cache_tiles=(512, 512), computation_tiles=None, max_resampling_size=None,
            debug_observers=()
    ):
        """Create a cached raster reciped anonymously in this Dataset.

        See Dataset.create_cached_raster_recipe
        """
        return self.create_cached_raster_recipe(
            _AnonymousSentry(),
            fp, dtype, channel_count, channels_schema, sr,
            compute_array, merge_arrays,
            cache_dir, ow,
            queue_data_per_primitive, convert_footprint_per_primitive,
            computation_pool, merge_pool, io_pool, resample_pool,
            cache_tiles, computation_tiles, max_resampling_size,
            debug_observers,
        )

    # Vector entry points *********************************************************************** **
    def open_vector(self, key, path, layer=None, driver='ESRI Shapefile', options=(), mode='r'):
        """Open a vector file in this Dataset under `key`. Only metadata are kept in memory.

        >>> help(GDALFileVector)

        Parameters
        ----------
        key: hashable (like a string)
            File identifier within Dataset
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
        elif np.all(np.isreal(layer)):
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

        # Dataset Registering ***********************************************
        if not isinstance(key, _AnonymousSentry):
            self._register([key], prox)
        else:
            self._register([], prox)
        return prox

    def aopen_vector(self, path, layer=None, driver='ESRI Shapefile', options=(), mode='r'):
        """Open a vector file anonymously in this Dataset. Only metadata are kept in memory.

        See Dataset.open_vector

        Example
        -------
        >>> trees = ds.aopen_vector('/path/to.shp')
        >>> features_bounds = trees.bounds

        """
        return self.open_vector(_AnonymousSentry(), path, layer, driver, options, mode)

    def create_vector(self, key, path, type, fields=(), layer=None,
                      driver='ESRI Shapefile', options=(), sr=None, ow=False):
        """Create an empty vector file and register it under `key` in this Dataset. Only metadata
        are kept in memory.

        >>> help(GDALFileVector)
        >>> help(GDALMemoryVector)

        Parameters
        ----------
        key: hashable (like a string)
            File identifier within Dataset
        path: string
        type: string
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
        ow: bool
            Overwrite. Whether or not to erase the existing files.

        Returns
        -------
        one of {GDALFileVector, GDALMemoryVector} depending on the `driver` parameter

        Example
        -------
        >>> ds.create_vector('lines', '/path/to.shp', 'linestring')
        >>> geometry_type = ds.lines.type
        >>> ds.lines.insert_data([[0, 0], [1, 1], [1, 2]])

        >>> fields = [
            {'name': 'name', 'type': str},
            {'name': 'count', 'type': 'int32'},
            {'name': 'area', 'type': np.float64, 'width': 5, precision: 18},
            {'name': 'when', 'type': np.datetime64},
        ]
        >>> ds.create_vector('zones', '/path/to.shp', 'polygon', fields)
        >>> field0_type = ds.zones.fields[0]['type']
        >>> ds.zones.insert_data(shapely.geometry.box(10, 10, 15, 15))

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
        type_ = type
        del type

        # Parameter checking ***************************************************
        path = str(path)
        type_ = conv.str_of_wkbgeom(conv.wkbgeom_of_str(type_))
        fields = _tools.normalize_fields_defn(fields)
        if layer is None:
            layer = '.'.join(os.path.basename(path).split('.')[:-1])
        else:
            layer = str(layer)
        driver = str(driver)
        options = [str(arg) for arg in options]
        ow = bool(ow)
        if sr is None:
            wkt = None
        else:
            success, payload = Catch(osr.GetUserInputAsWKT, nonzero_int_is_error=True)(sr)
            if not success:
                raise ValueError('Could not transform `sr` to `wkt` (gdal error: `{}`)'.format(
                    payload[1]
                ))
            wkt = payload

        # Construction dispatch ************************************************
        if driver.lower() == 'memory':
            allocator = lambda: BackGDALFileVector.create_file(
                '', type_, fields, layer, 'Memory', options, wkt, False,
            )
            prox = GDALMemoryVector(self, allocator, options)
        elif True:
            allocator = lambda: BackGDALFileVector.create_file(
                path, type_, fields, layer, driver, options, wkt, ow
            )
            prox = GDALFileVector(self, allocator, options, 'w')
        else:
            pass

        # Dataset Registering ***********************************************
        if not isinstance(key, _AnonymousSentry):
            self._register([key], prox)
        else:
            self._register([], prox)
        return prox

    def acreate_vector(self, path, type, fields=(), layer=None,
                       driver='ESRI Shapefile', options=(), sr=None, ow=False):
        """Create a vector file anonymously in this Dataset. Only metadata are kept in memory.

        See Dataset.create_vector

        Example
        -------
        >>> lines = ds.acreate_vector('/path/to.shp', 'linestring')
        >>> file_proj4 = lines.proj4_stored

        """
        return self.create_vector(_AnonymousSentry(), path, type, fields, layer,
                                  driver, options, sr, ow)

    # Source infos ******************************************************************************* **
    def __getitem__(self, key):
        """Retrieve a source from its key"""
        return self._source_of_key[key]

    def __contains__(self, item):
        """Is key or source registered in Dataset"""
        if isinstance(item, ASource):
            return item in self._keys_of_source
        return item in self._source_of_key

    def items(self):
        """Generate the pair of (keys_of_source, source) for all proxies"""
        for source, keys in self._keys_of_source.items():
            yield list(keys), source

    def keys(self):
        """Generate all source keys"""
        for source, keys in self._keys_of_source.items():
            for key in keys:
                yield key

    def values(self):
        """Generate all proxies"""
        for source, _ in self._keys_of_source.items():
            yield source

    def __len__(self):
        """Retrieve source count registered in this Dataset"""
        return len(self._keys_of_source)

    # Pools infos ******************************************************************************* **
    @property
    def pools(self):
        """Get the Pool Container.

        >>> help(PoolsContainer)

        """
        return self._back.pools_container

    # Cleanup *********************************************************************************** **
    def __del__(self):
        if not self._ds_closed:
            self.close()

    @property
    def close(self):
        """Close the Dataset with a call or a context management.
        The `close` attribute returns an object that can be both called and used in a with statement

        The Dataset can be closed manually or automatically when garbage collected, it is safer
        to do it manually. The steps are:
        - Stopping the scheduler
        - Joining the mp.Pool that have been automatically allocated
        - Close all sources

        Examples
        --------
        >>> ds = buzz.Dataset()
        ... # code...
        ... ds.close()

        >>> with buzz.Dataset().close as ds
        ...     # code...

        Caveat
        ------
        When using a scheduler, some memory leaks may still occur after closing a Dataset.
        Possible origins:
        - https://bugs.python.org/issue34172 (update your python to >=3.6.7)
        - Gdal cache not flushed (not a leak)
        - The gdal version
        - https://stackoverflow.com/a/1316799 (not a leak)
        - Some unknown leak in the python `threading` or `multiprocessing` standard library
        - Some unknown library leaking memory on the `C` side
        - Some unknown library storing data in global variables

        You can use a `debug_observer` with an `on_object_allocated` method to track large objects
        allocated in the scheduler. It will likely not be the source of the problem. If you
        even find a source of leaks please contact the buzzard team.
        https://github.com/airware/buzzard/issues

        """
        if self._ds_closed:
            raise RuntimeError("Dataset already closed")

        def _close():
            if self._ds_closed:
                raise RuntimeError("Dataset already closed")
            self._ds_closed = True

            # Tell scheduler to stop, wait until it is done
            self._back.stop_scheduler()

            # Safely release all resources
            self._back.pools_container._close()
            for source in list(self._keys_of_source.keys()):
                source.close()

        return _CloseRoutine(self, _close)

    # Spatial reference getters ***************************************************************** **
    @property
    def proj4(self):
        """Dataset's work spatial reference in WKT proj4.
        Returns None if `mode 1`.
        """
        if self._back.wkt_work is None:
            return None
        return osr.SpatialReference(self._back.wkt_work).ExportToProj4()

    @property
    def wkt(self):
        """Dataset's work spatial reference in WKT format.
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
            for prox in self._keys_of_source.keys()
            if isinstance(prox, APooledEmissary)
        ]
        total = len(proxs)

        if self._back.max_active < total:
            raise RuntimeError("Can't activate all pooled sources at the same time: {} pooled sources and max_activated is {}".format(
                total, self._back.max_active,
            ))

        # Hacky implementation to get the expected behavior
        # TODO: Implement that routine in the back driver pool. Is it possible? We need to call `.activate`
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
        for prox in self._keys_of_source.keys():
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
    for k, v in Dataset.__dict__.items():
        if hasattr(v, '__set_name__'):
            v.__set_name__(Dataset, k)

def open_raster(*args, **kwargs):
    """Shortcut for `Dataset().aopen_raster`

    >>> help(Dataset.open_raster)
    """
    return Dataset().aopen_raster(*args, **kwargs)

def open_vector(*args, **kwargs):
    """Shortcut for `Dataset().aopen_vector`

    >>> help(Dataset.open_vector)
    """
    return Dataset().aopen_vector(*args, **kwargs)

def create_raster(*args, **kwargs):
    """Shortcut for `Dataset().acreate_raster`

    >>> help(Dataset.create_raster)
    """
    return Dataset().acreate_raster(*args, **kwargs)

def create_vector(*args, **kwargs):
    """Shortcut for `Dataset().acreate_vector`

    >>> help(Dataset.create_vector)
    """
    return Dataset().acreate_vector(*args, **kwargs)

def wrap_numpy_raster(*args, **kwargs):
    """Shortcut for `Dataset().awrap_numpy_raster`

    >>> help(Dataset.wrap_numpy_raster)
    """
    return Dataset().awrap_numpy_raster(*args, **kwargs)

_CloseRoutine = type('_CloseRoutine', (_tools.CallOrContext,), {
    '__doc__': Dataset.close.__doc__,

})

DataSource = deprecation_pool.wrap_class(Dataset, 'DataSource', '0.6.0')

class _AnonymousSentry(object):
    """Sentry object used to instanciate anonymous proxies"""
