"""Welcome to buzzard, https://github.com/airware/buzzard

buzzard should always be imported the first time from the main thread
"""

__version__ = "0.6.0"

# Import osgeo before cv2
import osgeo as _
import cv2 as _

# Public classes
from buzzard._footprint import Footprint
from buzzard._dataset import (
    Dataset,
    open_raster,
    open_vector,
    create_raster,
    create_vector,
    wrap_numpy_raster
)
from buzzard._dataset import (
    DataSource, # Deprecated
)

from buzzard._env import Env

# Source's abstract classes
# Public methods, but always instanciated by Dataset, never by user.
from buzzard._a_source import ASource
from buzzard._a_source_raster import ASourceRaster
from buzzard._a_source_vector import ASourceVector

from buzzard._a_stored import AStored
from buzzard._a_stored_raster import AStoredRaster
from buzzard._a_stored_vector import AStoredVector

from buzzard._a_emissary import AEmissary
from buzzard._a_emissary_raster import AEmissaryRaster
from buzzard._a_emissary_vector import AEmissaryVector

from buzzard._a_pooled_emissary import APooledEmissary
from buzzard._a_pooled_emissary_raster import APooledEmissaryRaster
from buzzard._a_pooled_emissary_vector import APooledEmissaryVector

from buzzard._a_async_raster import AAsyncRaster
from buzzard._a_raster_recipe import ARasterRecipe

# Source's concrete classes
# Public methods, but always instanciated by Dataset, never by user.
from buzzard._gdal_file_raster import GDALFileRaster
from buzzard._gdal_mem_raster import GDALMemRaster
from buzzard._numpy_raster import NumpyRaster

from buzzard._gdal_file_vector import GDALFileVector
from buzzard._gdal_memory_vector import GDALMemoryVector

from buzzard._cached_raster_recipe import CachedRasterRecipe

# Misc classes
# Public methods, but always instanciated by Dataset, never by user.
from buzzard._dataset_pools_container import PoolsContainer

# Misc
from buzzard._env import env

# Public submodules
import buzzard.utils
import buzzard.srs
import buzzard.algo
