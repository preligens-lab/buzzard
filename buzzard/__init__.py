"""Welcome to buzzard, https://github.com/airware/buzzard

buzzard should always be imported the first time from the main thread
"""

# Import osgeo before cv2
import osgeo as _
import cv2 as _

from buzzard._footprint import Footprint
from buzzard._datasource import (
    DataSource,
    open_raster,
    open_vector,
    create_raster,
    create_vector,
    wrap_numpy_raster
)

from buzzard._gdal_file_raster import GDALFileRaster
from buzzard._gdal_mem_raster import GDALMemRaster
from buzzard._numpy_raster import NumpyRaster
from buzzard._gdal_file_vector import GDALFileVector
from buzzard._gdal_memory_vector import GDALMemoryVector

from buzzard._env import Env, env
import buzzard.srs
import buzzard.algo

__version__ = "0.5.0b0"
