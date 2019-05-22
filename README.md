# `buzzard`

In a nutshell, `buzzard` reads and writes geospatial raster and vector data.

<div align="center">
  <img src="img/buzzard.png"><br><br>
</div>

[![license](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://github.com/airware/buzzard/blob/master/LICENSE)
[![CircleCI](https://circleci.com/gh/airware/buzzard/tree/master.svg?style=shield&circle-token=9d41310f0eb3f8ff120a7103ba2d7ee5d5d628b7)](https://circleci.com/gh/airware/buzzard/tree/master)
[![codecov](https://codecov.io/gh/airware/buzzard/branch/master/graph/badge.svg?token=FbWmLGplCq)](https://codecov.io/gh/airware/buzzard)

Table of Contents
-----------------
+ [buzzard is](#buzzard-is)
+ [buzzard contains](#buzzard-contains)
+ [Simple example](#simple-example)
+ [Advanced (and fun) examples](#advanced-and-fun--examples)
+ [Features](#features)
+ [Future features summary](#future-features-summary)
+ [Dependencies](#dependencies)
+ [How to install](#how-to-install)
  + [Manually](#manually)
  + [Anaconda](#anaconda)
+ [Supported Python versions](#supported-python-versions)
+ [How to test](#how-to-test)
+ [Contributions and feedback](#contributions-and-feedback)
+ [License and Notice](#license-and-notice)
+ [Other pages](#other-pages)

## `buzzard` is
- a python library
- a `gdal`/`ogr`/`osr` wrapper
- designed to hide all cumbersome operations while working with GIS files
- designed for data science workflows
- under active development (see [`todo`](https://www.notion.so/buzzard/2c94ef6ee8da4d6280834129cc00f4d2?v=334ead18796342feb32ba85ccdfcf69f))
- tested with `pytest` with python 3.4 and python 3.7

## `buzzard` contains
- a class to open/read/write/create GIS files: [`Dataset`](./buzzard/_dataset.py)
- a toolbox class designed to locate a rectangle in both image space and geometry space: [`Footprint`](./buzzard/_footprint.py)

## Simple example
This example illustrates visualization of a raster based on polygons.

```py
import buzzard as buzz
import numpy as np
import matplotlib.pyplot as plt

rgb_path = 'path/to/raster.file'
polygons_path = 'path/to/vector.file'

ds = buzz.Dataset()
ds.open_raster('rgb', rgb_path)
ds.open_vector('polygons', polygons_path)

# Iterate over the polygons as shapely objects
for poly in ds.polygons.iter_data(None):

    # Compute the Footprint bounding poly
    fp = ds.rgb.fp.intersection(poly)

    # Read rgb at `fp` to a numpy array
    rgb = ds.rgb.get_data(fp=fp, channels=(0, 1, 2)).astype('uint8')
    alpha = ds.rgb.get_data(fp=fp, channels=3).astype('uint8')

    # Create a boolean mask as a numpy array from a shapely polygon
    mask = np.invert(fp.burn_polygons(poly))

    # Darken pixels outside of polygon, set nodata pixels to red
    rgb[mask] = (rgb[mask] * 0.5).astype(np.uint8)
    rgb[alpha == 0] = [255, 0, 0]

    plt.imshow(rgb)
    plt.show()
```

## Advanced (and fun 😊) examples
Additional examples can be found here: [basic examples](https://github.com/airware/buzzard/blob/master/doc/examples.ipynb), [async rasters](https://github.com/airware/buzzard/blob/master/doc/notebook2/async_rasters.ipynb)

## Features
- Raster and vector files opening
- Raster and vector files reading to `numpy.ndarray`, `shapely` objects, `geojson` and raw coordinates
- Raster and vector files writing from `numpy.ndarray`, `shapely` objects, `geojson` and raw coordinates
- Raster and vector files creation
- Powerful manipulations of raster windows
- Spatial reference homogenization between opened files like a `GIS software`

## Future features summary
- Wheels with `osgeo` binaries included
- Advanced spatial reference homogenization using `gdal` warping functions
- More tools, syntaxes and algorithms to work with raster datasets that don't fit in memory
- Strong support of non north-up / west-left footprints
- Data visualization tools
- Strong performance improvements
- Floating point precision loss handling improvements

## Dependencies

The following table lists dependencies along with the minimum version, their status for the project and the related license.

| Library          | Version  | Mandatory | License                                                                              | Comment                                                       |
|------------------|----------|-----------|--------------------------------------------------------------------------------------|---------------------------------------------------------------|
| gdal             | >=2.3.3  | Yes       | [MIT/X](https://github.com/OSGeo/gdal/blob/trunk/gdal/LICENSE.TXT)                   | Hard to install. Will be included in `buzzard` wheels         |
| opencv-python    | >=3.1.0  | Yes       | [3-clause BSD](http://opencv.org/license.html)                                       | Easy to install with `opencv-python` wheels. Will be optional |
| shapely          | >=1.6.1  | Yes       | [3-clause BSD](https://github.com/Toblerity/Shapely/blob/master/LICENSE.txt)         |                                                               |
| affine           | >=2.0.0  | Yes       | [3-clause BSD](https://github.com/sgillies/affine/blob/master/LICENSE.txt)           |                                                               |
| numpy            | >=1.15.0 | Yes       | [numpy](https://docs.scipy.org/doc/numpy-1.10.0/license.html)                        |                                                               |
| scipy            | >=0.19.1 | Yes       | [scipy](https://www.scipy.org/scipylib/license.html)                                 |                                                               |
| pint             | >=0.8.1  | Yes       | [3-clause BSD](https://github.com/hgrecco/pint/blob/master/LICENSE)                  |                                                               |
| six              | >=1.11.0 | Yes       | [MIT](https://github.com/benjaminp/six/blob/master/LICENSE)                          |                                                               |
| sortedcontainers | >=1.5.9  | Yes       | [apache](https://github.com/grantjenks/python-sortedcontainers/blob/master/LICENSE)  |                                                               |
| Rtree            | >=0.8.3  | Yes       | [MIT](https://github.com/Toblerity/rtree/blob/master/LICENSE.txt)                    |                                                               |
| scikit-image     | >=0.14.0 | Yes       | [scikit-image](https://github.com/scikit-image/scikit-image/blob/master/LICENSE.txt) |                                                               |
| chainmap         | >=1.0.2  | Yes       | [Python 2.7 license](https://bitbucket.org/jeunice/chainmap)                         | Only for python <3.2                                          |
| pytest           | >=3.2.2  | No        | [MIT](https://docs.pytest.org/en/latest/license.html)                                | Only for tests                                                |
| attrdict         | >=2.0.0  | No        | [MIT](https://github.com/bcj/AttrDict/blob/master/LICENSE.txt)                       | Only for tests                                                |

## How to install from terminal
### Anaconda and pip
```sh
# Step 1 - Install Anaconda
# https://www.anaconda.com/download/

# Step 2 - Create env
conda create -n buzz python gdal>=2.3.3 shapely rtree -c 'conda-forge'

# Step 3 - Activate env
conda activate buzz

# Step 4 - Install buzzard
pip install buzzard
```

### Docker
```sh
docker build -t buzz --build-arg PYTHON_VERSION=3.7 https://raw.githubusercontent.com/airware/buzzard/master/.circleci/images/base-python/Dockerfile
docker run -it --rm buzz bash
pip install buzzard

```

### Package manager and pip
```sh
# Step 1 - Install GDAL and rtree
# Windows:
# https://www.lfd.uci.edu/~gohlke/pythonlibs/#gdal
# https://www.lfd.uci.edu/~gohlke/pythonlibs/#rtree

# MacOS:
brew install gdal
brew tap osgeo/osgeo4mac
brew tap --repair
brew install gdal2
brew install spatialindex
export PATH="/usr/local/opt/gdal2/bin:$PATH"
python -m pip install 'gdal==2.3.3'

# Ubuntu:
sudo add-apt-repository ppa:ubuntugis/ppa
sudo apt-get update
sudo apt-get install gdal-bin
sudo apt-get install libgdal-dev
sudo apt-get install python3-rtree
export CPLUS_INCLUDE_PATH=/usr/include/gdal
export C_INCLUDE_PATH=/usr/include/gdal
pip install GDAL

# Step 2 - Install buzzard
python -m pip install buzzard
```

## Supported Python versions
To enjoy the latest buzzard features, update your python!

### Full python support
- Latest supported version: `3.7` (June 2018)
- Oldest supported version: `3.4` (March 2014)

### Partial python support
- `2.7`: use buzzard version `0.4.4`


## How to test
```sh
git clone https://github.com/airware/buzzard
pip install -r buzzard/requirements-dev.txt
pytest buzzard/buzzard/test
```

## Documentation
Hosted soon, in the meantime
- look at docstrings in code
- get familiar with the [public classes](https://github.com/airware/buzzard/wiki/Public-classes)
- play with the examples in [examples.ipynb](./doc/examples.ipynb)

## Contributions and feedback
Welcome to the `buzzard` project! We appreciate any contribution and feedback, your proposals and pull requests will be considered and responded to. For more information, see the [`CONTRIBUTING.md`](./CONTRIBUTING.md) file.

## Authors
See [AUTHORS](./AUTHORS.md)

## License and Notice
See [LICENSE](./LICENSE) and [NOTICE](./NOTICE).

## Other pages
- [examples](./doc/examples.ipynb)
- [classes](https://github.com/airware/buzzard/wiki/Classes)
- [wiki](https://github.com/airware/buzzard/wiki)
- [todo](https://www.notion.so/buzzard/2c94ef6ee8da4d6280834129cc00f4d2?v=334ead18796342feb32ba85ccdfcf69f)

------------------------------------------------------------------------------------------------------------------------
