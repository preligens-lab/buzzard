This `TODO.md` file includes both short and long-term feature requests and ideas. It also provides a glimpse to the priorities we are assuming for our developments.

Table of Contents
=================

  * [TODO](#todo)
    * [TODO General](#todo-general)
      * [Code Cleaning](#code-cleaning)
        * [Code Cleaning (Major)](#code-cleaning-major)
        * [Code Cleaning (Medium)](#code-cleaning-medium)
        * [Code Cleaning (Minor)](#code-cleaning-minor)
      * [New Features](#new-features)
        * [New Features (Major)](#new-features-major)
        * [New Features (Medium)](#new-features-medium)
        * [New Features (Minor)](#new-features-minor)
      * [Tests](#tests)
        * [Tests (Major)](#tests-major)
        * [Tests (Medium)](#tests-medium)
        * [Tests (Minor)](#tests-minor)
    * [TODO Footprint](#todo-footprint)
      * [Code Cleaning](#code-cleaning-1)
        * [Code Cleaning (Major)](#code-cleaning-major-1)
        * [Code Cleaning (Medium)](#code-cleaning-medium-1)
        * [Code Cleaning (Minor)](#code-cleaning-minor-1)
      * [New Features](#new-features-1)
        * [New Features (Major)](#new-features-major-1)
        * [New Features (Medium)](#new-features-medium-1)
        * [New Features (Minor)](#new-features-minor-1)
      * [Tests](#tests-1)
        * [Tests (Major)](#tests-major-1)
        * [Tests (Medium)](#tests-medium-1)
        * [Tests (Minor)](#tests-minor-1)
    * [TODO DataSource](#todo-datasource)
      * [Code Cleaning](#code-cleaning-2)
        * [Code Cleaning (Major)](#code-cleaning-major-2)
        * [Code Cleaning (Medium)](#code-cleaning-medium-2)
        * [Code Cleaning (Minor)](#code-cleaning-minor-2)
      * [New Features](#new-features-2)
        * [New Features (Major)](#new-features-major-2)
        * [New Features (Medium)](#new-features-medium-2)
        * [New Features (Minor)](#new-features-minor-2)
      * [Tests](#tests-2)
        * [Tests (Major)](#tests-major-2)
        * [Tests (Medium)](#tests-medium-2)
        * [Tests (Minor)](#tests-minor-2)
    * [TODO FileProxy](#todo-fileproxy)
      * [Code Cleaning](#code-cleaning-3)
        * [Code Cleaning (Major)](#code-cleaning-major-3)
        * [Code Cleaning (Medium)](#code-cleaning-medium-3)
        * [Code Cleaning (Minor)](#code-cleaning-minor-3)
      * [New Features](#new-features-3)
        * [New Features (Major)](#new-features-major-3)
        * [New Features (Medium)](#new-features-medium-3)
        * [New Features (Minor)](#new-features-minor-3)
      * [Tests](#tests-3)
        * [Tests (Major)](#tests-major-3)
        * [Tests (Medium)](#tests-medium-3)
        * [Tests (Minor)](#tests-minor-3)
    * [TODO RasterProxy](#todo-rasterproxy)
      * [Code Cleaning](#code-cleaning-4)
        * [Code Cleaning (Major)](#code-cleaning-major-4)
        * [Code Cleaning (Medium)](#code-cleaning-medium-4)
        * [Code Cleaning (Minor)](#code-cleaning-minor-4)
      * [New Features](#new-features-4)
        * [New Features (Major)](#new-features-major-4)
        * [New Features (Medium)](#new-features-medium-4)
        * [New Features (Minor)](#new-features-minor-4)
      * [Tests](#tests-4)
        * [Tests (Major)](#tests-major-4)
        * [Tests (Medium)](#tests-medium-4)
        * [Tests (Minor)](#tests-minor-4)
    * [TODO VectorProxy](#todo-vectorproxy)
      * [Code Cleaning](#code-cleaning-5)
        * [Code Cleaning (Major)](#code-cleaning-major-5)
        * [Code Cleaning (Medium)](#code-cleaning-medium-5)
        * [Code Cleaning (Minor)](#code-cleaning-minor-5)
      * [New Features](#new-features-5)
        * [New Features (Major)](#new-features-major-5)
        * [New Features (Medium)](#new-features-medium-5)
        * [New Features (Minor)](#new-features-minor-5)
      * [Tests](#tests-5)
        * [Tests (Major)](#tests-major-5)
        * [Tests (Medium)](#tests-medium-5)
        * [Tests (Minor)](#tests-minor-5)
    * [TODO algo/conv/srs](#todo-algoconvsrs)
      * [Code Cleaning](#code-cleaning-6)
        * [Code Cleaning (Major)](#code-cleaning-major-6)
        * [Code Cleaning (Medium)](#code-cleaning-medium-6)
        * [Code Cleaning (Minor)](#code-cleaning-minor-6)
      * [New Features](#new-features-6)
        * [New Features (Major)](#new-features-major-6)
        * [New Features (Medium)](#new-features-medium-6)
        * [New Features (Minor)](#new-features-minor-6)
      * [Tests](#tests-6)
        * [Tests (Major)](#tests-major-6)
        * [Tests (Medium)](#tests-medium-6)
        * [Tests (Minor)](#tests-minor-6)
    * [TODO Env](#todo-env)
      * [Code Cleaning](#code-cleaning-7)
        * [Code Cleaning (Major)](#code-cleaning-major-7)
        * [Code Cleaning (Medium)](#code-cleaning-medium-7)
        * [Code Cleaning (Minor)](#code-cleaning-minor-7)
      * [New Features](#new-features-7)
        * [New Features (Major)](#new-features-major-7)
        * [New Features (Medium)](#new-features-medium-7)
        * [New Features (Minor)](#new-features-minor-7)
      * [Tests](#tests-7)
        * [Tests (Major)](#tests-major-7)
        * [Tests (Medium)](#tests-medium-7)
        * [Tests (Minor)](#tests-minor-7)
  * [IDEAS](#ideas)
    * [Game changers](#game-changers)
    * [Miscelaneous](#miscelaneous)
    * [Gdal/ogr dataset/datasource](#gdalogr-datasetdatasource)
    * [TODO Details](#todo-details)
      * [Footprint\.\_\_init\_\_](#footprint__init__)
      * [Raster Attribute Table](#raster-attribute-table)
      * [Raster overviews](#raster-overviews)
    * [Open modes](#open-modes)
      * [Vector read](#vector-read)
      * [Vector write](#vector-write)
      * [Raster read](#raster-read)

# TODO

This section lists all item that are meant to be executed or implemented into `buzzard`.

## TODO General
- State of the art, review other libraries for additional or extended features (or simply trigger new ideas or design enhancements):
  - Crawl `GeoPandas`
  - Crawl `GEOS` for features missing in `shapely`
- Open sourcing
  - Documentation
    - With pdoc? (ex: http://pythonhosted.org/PyGeoj/, http://pythonhosted.org/PyCRS/)
    - With readthedocs? (ex: http://docs.readthedocs.io/en/latest/getting_started.html)
  - Make it `pip` installable
  - Make it `conda` installable
  - Replace `buzzard-M.m.p.targ.gz` per a "standard" new line in our Mono repo `requirements.txt`
  - Implement https://requires.io
  - Add python2.7 tests in CI

### Code Cleaning

#### Code Cleaning (Major)
#### Code Cleaning (Medium)
- Improve all raised error verbosity
  - Update `nodata error` message in `get_data` in order to be more explicit
- Improve all parameters controls
  - Add to `_tools/parameters.py` to allow standalone unit testings
  - float abc/interval
  - int abc/is_integer/interval
  - iterables of len/abc/interval

#### Code Cleaning (Minor)

### New Features
#### New Features (Major)
- `GDAL` binary packing
  - Provide wheels for `GDAL` (and `OpenCV`?)
- Vector Recipe
  - New proxy class for abstract vector data
  - Recipe could return a geopandas to only merge polygons with the same `kind` tag
- Built-in recipes in `buzz.utils`
  - Slopes using `GDAL`
  - Contour using `GDAL`
- Add info functions to all important classes
  - __str__
  - __format__
- Wrap algorithms from various libraries, without making (necessarily) those libraries requirements
  - `cv2`, `skimage`, `pil`
  - `networkx`, `pysal`
  - `gdal`, `grass`
  - `rtree`, `qhull`
  - `scipy.*`
  - `tensorflow`, `numba`, `opencl`
  - Exemple: `Footprint.remap` can be performed by multiple libraries

#### New Features (Medium)
- Add support for custom reprojections

#### New Features (Minor)
n/a

### Tests

#### Tests (Major)
n/a

#### Tests (Medium)
n/a

#### Tests (Minor)
n/a

------------------------------------------------------------------------------------------------------------------------

## TODO `Footprint`

### Code Cleaning
#### Code Cleaning (Major)
#### Code Cleaning (Medium)
#### Code Cleaning (Minor)
- Move some implementations to dedicated files
- Make `.meshgrid*` functions lighter

### New Features
#### New Features (Major)
- Factorize and build new `raster/vector` conversions
  - Implement
    - `find_points` -> `custom`
    - `find_lines` -> `gdal.GenerateContours`
    - `find_contours` -> `cv2.find_contours`
    - `find_polygons` -> `gdal.Polygonize`
    - `burn_points` / `burn_points_in` -> `gdal.Rasterize`
    - `burn_lines` / `burn_lines_in` -> `gdal.Rasterize`
    - `burn_contours` / `burn_contours_in` -> `cv2.fill_poly`
    - `burn_polygons` / `burn_polygons_in` -> `gdal.Rasterize`
  - Implement
    - `sample_point_in` -> `cv2.remap`
    - `sample_line_in` -> `cv2.remap`
    - `sample_contours_in` -> ??
    - `sample_polygon_in` -> ??
  - Add many options to `burn_*`
    - `all_touched`
    - `dtype`
    - `fill`
    - `value`
    - `labelize`
    - `connectivity 0/4/8`
    - interpolation?
    - burn attributes if geopandas
    - options from gdal
  - Add many options to `find_*`
    - options from gdal
    - smoothing (keep topo?)
    - approximation (keep topo?)
- Factorize and build new `constructors` (a `constructor` is a method that returns 1 `Footprint`)
  - Rewrite and merge `of_polygon` constructor
  - Provide a way to clone footprint to new scale
  - Provide ways to choose between preserving self.tl/self.br/self.size/self.reso/self.rsize
  - Provide a way to clone footprint to new rsize
  - Create `fp_union`
  - Keep in mind the existence of `clip`, `dilate`, `erode`, `fp_intersection`, `move` and `of_extent`

#### New Features (Medium)
- Factorize and build new `binary predicates`
  - Add more binary operations like `share_area`
- A new tiling function to tile a Footprint given a grid
  - Will be used to tile a Footprint along raster's blocks
  - naming: `tile_grid`
- Add more methods from shapely
- Update spatial/raster conversion functions
  - Take as input non-homogeneous lists of coordinates
  - Take as input shapely geometries to convert
  - Take as input 3d coordinates

#### New Features (Minor)
- Add `.t`, `.yx`, `.yxt`, `.geom` properties
  - Returning a wrapped Footprint in which methods return `xy tuples`/`yx numpy arrays`/`yx tuples`/`shapely geometry`
  - Exemple: `arr[fp.yxt.rc]`
  - Exemple: `(fp & fp.geom.c).dilate(100)`


### Tests
#### Tests (Major)
n/a

#### Tests (Medium)
n/a

#### Tests (Minor)
n/a

------------------------------------------------------------------------------------------------------------------------

## TODO `DataSource`

### Code Cleaning
#### Code Cleaning (Major)
n/a

#### Code Cleaning (Medium)
n/a

#### Code Cleaning (Minor)
n/a

### New Features

#### New Features (Major)
- Revamp the reprojection handling
  - The user should choose between several opening modes
  - Add a way to rewarp rasters on the fly

#### New Features (Medium)
- New methods to create a tif/shp/geojon directly (specialization of create_raster)
  - Explicit parameters from drivers

#### New Features (Minor)
n/a

### Tests
#### Tests (Major)
n/a

#### Tests (Medium)
n/a

#### Tests (Minor)
n/a

------------------------------------------------------------------------------------------------------------------------

## TODO `Proxies`

### Code Cleaning
#### Code Cleaning (Major)
n/a

#### Code Cleaning (Medium)
n/a

#### Code Cleaning (Minor)

### New Features
#### New Features (Major)
- Implement algorithms that exist in Footprint but specialized to act on a full file (raster/vector)

#### New Features (Medium)
- Actively control multithreading accesses to datasets
  - Gather informations
  - https://lists.osgeo.org/pipermail/gdal-dev/2016-September/045155.html

#### New Features (Minor)
- Decide if deletion prior to creation should use `os.remove` or not (now disabled)
  - tags: brainstorm
  - Idea: Add `override` parameter to `create_*` functions

### Tests

#### Tests (Major)
n/a

#### Tests (Medium)
n/a

#### Tests (Minor)
n/a

------------------------------------------------------------------------------------------------------------------------

## TODO `Raster` / `RasterConcrete`

### Code Cleaning

#### Code Cleaning (Major)
- Rework `remap` method
  - Add gdal transform functions side by side with opencv ones (and scipy, skimage?)
  - Add Env variable to select remapping algorithm
    - naming: `('down_cv2_area', 'up_gdal_truc')`
  - Tile computation because of `SHRT_MAX`
  - Compile cv2 map arrays
  - recenter coordinates before, restore after

#### Code Cleaning (Medium)
n/a

#### Code Cleaning (Minor)
n/a

### New Features
#### New Features (Major)
- `.get_data_xy()`
- Provide a way to read a png without having it fliped upside down
  - Idea: `open_image`

#### New Features (Medium)
- Add features to work with raster blocks
- Add virtual memory support with the `__getitem__` syntax
- Provide a way to have `get_data` to always return a certain type
  - Problem: Opening an rgb image and having `int8` data

#### New Features (Minor)
- In `_create_file` split `options` and `options_layer`
- Add algorithm to compute raster's shrinked Footprint to data pixels only (excluding nodata)

### Tests
#### Tests (Major)
#### Tests (Medium)
- Split tests to gain more confidence
  - Test `remap` alone (with complicated tests)
  - Test `get_data`/`set_data` alone (with simple tests)

#### Tests (Minor)

------------------------------------------------------------------------------------------------------------------------

## TODO `RasterRecipe`

### Code Cleaning
#### Code Cleaning (Major)
n/a

#### Code Cleaning (Medium)
n/a

#### Code Cleaning (Minor)
n/a

### New Features
#### New Features (Major)
n/a

#### New Features (Medium)
n/a

#### New Features (Minor)
- Add more parameters to pixel functions
  - Like `output`, `bordersize`, ...

### Tests
#### Tests (Major)
n/a

#### Tests (Medium)
n/a

#### Tests (Minor)
n/a

------------------------------------------------------------------------------------------------------------------------

## TODO `VectorProxy`

### Code Cleaning

#### Code Cleaning (Major)
n/a

#### Code Cleaning (Medium)
n/a

#### Code Cleaning (Minor)
n/a

### New Features

#### New Features (Major)
n/a

#### New Features (Medium)
n/a

#### New Features (Minor)
- Force a geometry type on reading
  - multi/simple
  - See ogr conversion fonctions
- Add insert_geojson
- Handle cases when geometry conversion can't occur because of VectorProxy.type (like unknown)
  - Can't transform from coordinates to ogr.Geometry in `insert_data`

### Tests
#### Tests (Major)
n/a

#### Tests (Medium)
n/a

#### Tests (Minor)
n/a

------------------------------------------------------------------------------------------------------------------------

## TODO `algo/conv/srs/_utils`

### Code Cleaning
#### Code Cleaning (Major)
n/a

#### Code Cleaning (Medium)
- Improve `create_slopes`
  - Idea: Use `GDAL` or provide different implementations
  - Idea: Move to Footprint since it requires resolution informations
  - Be sensitive to nodata like `_remap function`

#### Code Cleaning (Minor)
- Use gdal's type conversion functions inside home made ones

### New Features
#### New Features (Major)
n/a

#### New Features (Medium)
n/a

#### New Features (Minor)
- Add flattening shapely geometry iterator
  - in `buzzard/algo` ?
  - naming: `geom_iterator(shapely_type)`

### Tests
#### Tests (Major)
n/a

#### Tests (Medium)
n/a

#### Tests (Minor)
n/a

------------------------------------------------------------------------------------------------------------------------

## TODO `Env`

### Code Cleaning
#### Code Cleaning (Major)
n/a

#### Code Cleaning (Medium)
n/a

#### Code Cleaning (Minor)
n/a

### New Features
#### New Features (Major)
- Add option for floating point coordinates dtype
  - tags: brainstorm find-loopholes
  - naming: `coordinates_dtype`
  - Reimplement `affine` library

#### New Features (Medium)
- More `GDAL` states
  - https://trac.osgeo.org/gdal/wiki/ConfigOptions

#### New Features (Minor)
- Debug printing level / canal

### Tests
#### Tests (Major)
n/a

#### Tests (Medium)
n/a

#### Tests (Minor)
n/a

------------------------------------------------------------------------------------------------------------------------

# IDEAS

Every item which is not yet a **TODO**

## Game changers
- Fully handle non-northup footprints
- Point cloud / lidar handling / LAS / PDAL support
- Support for tiled spatial raster
- Dataviz support, add `im`/`imshow`/`imwrite` utilities functions:
  - Using `matplotlib`
  - Using `descartes`?
  - In `buzzard`, in `DataSource`, in `Footprint` only? Everywhere?

## Miscelaneous
- Virtual File System handling
  - AWS credentials in Env
- Provide some kind of data compression
  - http://www.blosc.org/
- Provide discretization tools like skeletization, line to points, polygon to points
- Provide spatial clustering tools, triangulation, voronoi
- Add `lru` caching option to FileProxy data retrieving functions
  - http://boltons.readthedocs.io/en/latest/cacheutils.html
- Support for wgs84 coordinates conversion
- RasterProxy.iter_data to geopandas
- Find a use case with sparse tif
- Adaptative Footprint constructor
  - With the following ideas (see examples bellow)
  - Constraint programming? (see: https://labix.org/python-constraint)
- `DWG` and `DXF` read / write support:
  - https://www.gisconvert.com/
  - https://www.ibm.com/developerworks/opensource/library/os-autocad/
  - http://www.osgeo.org/node/1789
  - https://github.com/codelibs/libdxfrw
  - https://trac.osgeo.org/gdal/wiki/DxfDwg
- Perf improvement (computation time)
  - `RasterProxy.get_data`
  - `RasterProxy.set_data`
  - `Footprint`
    - A cython implementation for north-easting
	- A cython implementation for the general case
	  - Store the align information for quick compatibility check
  - Compile some parts using `cython`? `Numba`? `Pypy`? ...
  - Parallelize using `Joblib`? `Dask`? ...
  - Add tests to ensure it is quicker

## Gdal/ogr dataset/datasource
- Support raster attribute tables
  - http://gdal.org/python/osgeo.gdal.RasterAttributeTable-class.html
  - http://gdal.org/java/org/gdal/gdal/RasterAttributeTable.html
  - http://www.gdal.org/gdal_8h.html#a810154ac91149d1a63c42717258fe16e
  - http://www.gdal.org/gdal_8h.html#a27bf786b965d5227da1acc2a4cab69a1
- Support raster styles
- Support subdatasets
  - http://gdal.org/python/osgeo.gdal-pysrc.html#Dataset.GetSubDatasets
- Support GCPs
- Support gdal raster info/statistics
- Support raster masks (see interface below)
- Support raster overviews (see interface below)
- Support raster band categories
- Support raster band color interpretation / ColorEntry palette / color table
- Support raster async reader
- Support raster virtual mem, use gdalarray?
- Support raster transactions

------------------------------------------------------------------------------------------------------------------------
------------------------------------------------------------------------------------------------------------------------

## TODO Details

### `Footprint.__init__`
```
"""
Exemple: To build this representation of size (80, 10) and pxsize (160, 20)
****************************
* (0, 10)       (80, 10)   *
*   ||||||||||||||||       *
*   ||||||||||||||||       *
* (0, 0)        (80, 0)    *
****************************
Footprint(tl=(0, 10), size=(80, 10),    pxsize=(160, 20)) # Assumed north-up
Footprint(tl=(0, 10), size=(80, 10),    shape=(20, 160)) # Assumed north-up
Footprint(tl=(0, 10), size=(80, 10),    reso=(0.5, -0.5)) # Assumed north-up
Footprint(tl=(0, 10), size=(80, 10),    resox=0.5) # Assumed north-up & resoy == -resox
Footprint(tl=(0, 10), size=(80, 10),    pxbr=(159, 19)) # Assumed north-up
Footprint(tl=(0, 10), size=(80, 10),    pxrx=159, pxby=19) # Assumed north-up

Footprint(tl=(0, 10), size=(80, 10),    pxsize=(160, 20))
Footprint(tl=(0, 10), vec=(80, -10),    pxsize=(160, 20))
Footprint(tl=(0, 10), br=(80, 0),       pxsize=(160, 20))

Footprint(c=(5, 5), size=(80, 10),      pxsize=(160, 20))
Footprint(tlx=0, tly=10, brx=80, bry=0, pxsize=(160, 20))
Footprint(lx=0, ty=10, rx=80, by=0,     pxsize=(160, 20))

Footprint(gt=(0, 0.5, 0, 10, -0.5, 0),  size=(80, 10))
Footprint(gt=(0, 0.5, 0, 10, -0.5, 0),  pxsize=(160, 20))
Footprint(gt=(0, 0.5, 0, 10, -0.5, 0),  br=(80, 0))
"""
```

------------------------------------------------------------------------------------------------------------------------

### Raster Attribute Table
```py
import numpy as np
from osgeo import gdal, gdalconst
import buzzard as buzz

path = ''
gb = buzz.DataSource.from_tif('src', path)
print(gb.src.fp)
gb.create_tif('ici.tif', gb.src.fp, 'uint8', key='out', mode='w')
print(gb.out.fp)
gb.out.set_data(gb.src.get_data())
rat = gdal.RasterAttributeTable()
rat.CreateColumn('name', gdalconst.GFT_String, gdalconst.GFU_Name)
rat.CreateColumn('val', gdalconst.GFT_Integer, gdalconst.GFU_MinMax)
rat.CreateColumn('r', gdalconst.GFT_Integer, gdalconst.GFU_Red)
rat.CreateColumn('g', gdalconst.GFT_Integer, gdalconst.GFU_Green)
rat.CreateColumn('b', gdalconst.GFT_Integer, gdalconst.GFU_Blue)
rat.SetRowCount(12)
rat.WriteArray(np.asarray(range(3)), 1)
rat.WriteArray(np.asarray(['veg', 'water', 'stocks']), 0)
colors = np.asarray([
    [0, 255, 0], # vegetation
    [0, 0, 255], # water
    [176, 196, 222], # stocks
])
rat.WriteArray(colors[:, 0], 2)
rat.WriteArray(colors[:, 1], 3)
rat.WriteArray(colors[:, 2], 4)

gb.out.gdal.GetRasterBand(1).SetDefaultRAT(rat)
gb.unregister('out')

gb.register_tif('out', 'ici.tif')
rat = gb.out.gdal.GetRasterBand(1).GetDefaultRAT()
print(rat.ReadAsArray(0))
print(rat.ReadAsArray(1))
gb.unregister('out')
```

------------------------------------------------------------------------------------------------------------------------

### Raster overviews
```py

# Overview
RasterProxy.build_overviews(factors, alg='nearest'):
    """

    Parameters
    ----------
    factors: iterator of int
    alg: str or None
        if str: one of ('nearest', 'gauss', 'average', 'average_magphase' or 'none')
            Uppercast accepted too
        if None: same as string 'none'

    Returns
    -------
    None
    """

@property
RasterProxy.overview_count(self)

RasterProxy.get_overview_fp(seld, oid)

RasterProxy.get_data_overview(self, oid, ...same.parameters.as.get_data...)
```

------------------------------------------------------------------------------------------------------------------------

## Open modes

### Vector read
| File srs vs Work srs | Transform when | Open type                                         | buzzard read operations |
|----------------------|----------------|---------------------------------------------------|-------------------------|
| Same                 | Never          | Regular                                           | Read, yield             |
| Different            | On read        | Regular                                           | Read, transform, yield  |
| Different            | On read        | VRT                                               | Read, yield             |
| Different            | On open        | Transform to `Memory` dataset                     | Read, yield             |
| Different            | On open        | Transform to `ESRI Shapefile` dataset to tempfile | Read, yield             |

### Vector write
| File srs vs Work srs | Transform when | Open type | buzzard write operations |
|----------------------|----------------|-----------|--------------------------|
| Same                 | Never          | Regular   | Get, write               |
| Different            | On write       | Regular   | Get, transform, write    |

### Raster read


------------------------------------------------------------------------------------------------------------------------
------------------------------------------------------------------------------------------------------------------------
