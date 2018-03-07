"""Welcome to buzzard, https://github.com/airware/buzzard

buzzard should always be imported the first time from the main thread
"""

# Import osgeo before cv2
import osgeo as _
import cv2 as _

from buzzard._footprint import Footprint
from buzzard._datasource import DataSource

from buzzard._proxy import Proxy
from buzzard._raster import Raster
from buzzard._raster_physical import RasterPhysical
from buzzard._raster_recipe import RasterRecipe

from buzzard._env import Env, env
import buzzard.srs
import buzzard.algo

__version__ = "0.3.2"
