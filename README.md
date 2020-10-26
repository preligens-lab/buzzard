# `buzzard`
In a nutshell, the `buzzard` library provides powerful abstractions to manipulate together images and geometries that come from different kind of sources (`GeoTIFF`, `PNG`, `GeoJSON`, `Shapefile`, `numpy array`, `buzzard pipelines`, ...).

<div align="center">
  <img src="https://github.com/earthcube-lab/buzzard/raw/master/img/buzzard.png"><br><br>
</div>

[![license](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://github.com/earthcube-lab/buzzard/blob/master/LICENSE)[![CircleCI](https://circleci.com/gh/earthcube-lab/buzzard/tree/master.svg?style=shield&circle-token=9d41310f0eb3f8ff120a7103ba2d7ee5d5d628b7)](https://circleci.com/gh/earthcube-lab/buzzard/tree/master)[![codecov](https://codecov.io/gh/earthcube-lab/buzzard/branch/master/graph/badge.svg?token=FbWmLGplCq)](https://codecov.io/gh/earthcube-lab/buzzard)[![readthedoc](https://readthedocs.org/projects/buzzard/badge/?version=latest&style=flat)](https://buzzard.readthedocs.io/en/latest)

[
![Join us on Slack!](https://cdn.brandfolder.io/5H442O3W/as/pl54cs-bd9mhs-3jsgg0/btn-add-to-slack_1x.png?height=25)
](https://join.slack.com/t/buzzard-python/shared_invite/enQtNjY0NDQ2MzU3MzgzLTJhNTZhNjAwOGIyM2RkOTdkZGE5MGUwZGEzZGQwODkyMzY2N2YwMTg5ZmI1NDc2MjY2MGM2ZTdhNDc3M2E1YTI)

<!-- [![Requirements Status](https://requires.io/github/airware/buzzard/requirements.svg?branch=master)](https://requires.io/github/airware/buzzard/requirements/?branch=master) -->

## `buzzard` is
- A _python_ library.
- Primarily designed to hide all cumbersome operations when doing data-science with [GIS](https://en.wikipedia.org/wiki/Geographic_information_system) files.
- A Multipurpose computer vision library, it can be used in all kind of situations where images or geometries are involved.
- A pythonic wrapper for _osgeo_'s _gdal_/_ogr_/_osr_.
- A solution to work with arbitrary large images by simplifying and automating the manipulation of image slices.

## `buzzard` contains
- A [`Dataset`](https://buzzard.readthedocs.io/en/latest/dataset.html) class that oversees all opened raster and vector files in order to share resources.
- An immutable toolbox class, the [`Footprint`](https://buzzard.readthedocs.io/en/latest/footprint.html), designed to locate a rectangle in both image space and geometry space.

## How to open and read files
This example demonstrates how to visualize a large raster polygon per polygon.

```py
import buzzard as buzz
import numpy as np
import matplotlib.pyplot as plt

# Open the files. Only files' metadata are read so far
r = buzz.open_raster('path/to/rgba-image.tif')
v = buzz.open_vector('path/to/polygons.geojson', driver='GeoJSON')


# Load the polygons from disk one by one as shapely objects
for poly in v.iter_data():

    # Compute the Footprint bounding `poly`
    fp = r.fp.intersection(poly)
    print(fp)

    # Load the image from disk at `fp` to a numpy array
    rgb = r.get_data(fp=fp, channels=(0, 1, 2))
    alpha = r.get_data(fp=fp, channels=3)

    # Create a boolean mask as a numpy array from the shapely polygon
    mask = np.invert(fp.burn_polygons(poly))

    # Darken pixels outside of polygon, set transparent pixels to orange
    rgb[mask] = (rgb[mask] * 0.5).astype(np.uint8)
    rgb[alpha == 0] = [236, 120, 57]

    # Show the result with matplotlib
    plt.imshow(rgb)
    plt.show()

```
Images from the [ISPRS's Potsdam dataset](http://www2.isprs.org/commissions/comm3/wg4/2d-sem-label-potsdam.html).

`Footprint(tl=(3183.600000, -914.550000), br=(3689.700000, -1170.450000), size=(506.100000, 255.900000), rsize=(3374, 1706))`

<div align="center">
  <img src="https://github.com/earthcube-lab/buzzard/raw/master/img/ex0-img0.jpg" width="80%"><br><br>
</div>

`Footprint(tl=(3171.600000, -1321.500000), br=(4553.400000, -2400.000000), size=(1381.800000, 1078.500000), rsize=(9212, 7190))`

<div align="center">
  <img src="https://github.com/earthcube-lab/buzzard/raw/master/img/ex0-img1.jpg" width="70%"><br><br>
</div>

## How to create files and manipulate _Footprints_
```py
import buzzard as buzz
import numpy as np
import matplotlib.pyplot as plt
import keras

r = buzz.open_raster('path/to/rgba-image.tif')
km = keras.models.load_model('path/to/deep-learning-model.hdf5')

# Chunk the raster's Footprint to Footprints of size
# 1920 x 1080 pixel stored in a 2d numpy array
tiles = r.fp.tile(1920, 1080)

all_roads = []

for i, fp in enumerate(tiles.flat):
    rgb = r.get_data(fp=fp, channels=(0, 1, 2))

    # Perform pixelwise semantic segmentation with a keras model
    predictions_heatmap = km.predict(rgb[np.newaxis, ...])[0]
    predictions_top1 = np.argmax(predictions_heatmap, axis=-1)

    # Save the prediction to a `geotiff`
    with buzz.create_raster(path='predictions_{}.tif'.format(i), fp=fp,
                            dtype='uint8', channel_count=1).close as out:
        out.set_data(predictions_top1)

    # Extract the road polygons by transforming a numpy boolean mask to shapely polygons
    road_polygons = fp.find_polygons(predictions_top1 == 3)
    all_roads += road_polygons

    # Show the result with matplotlib for one tile
    if i == 2:
        plt.imshow(rgb)
        plt.imshow(predictions_top1)
        plt.show()

# Save all roads found to a single `shapefile`
with buzz.create_vector(path='roads.shp', type='polygon').close as out:
    for poly in all_roads:
        out.inser_data(poly)

```

<div align="center">
  <img src="https://github.com/earthcube-lab/buzzard/raw/master/img/ex1-img0.jpg" width="80%"><br><br>
</div>

<div align="center">
  <img src="https://github.com/earthcube-lab/buzzard/raw/master/img/ex1-img1.jpg" width="80%"><br><br>
</div>

## Advanced examples
Additional examples can be found here:
- [Files and _Footprints_ in depth](https://github.com/earthcube-lab/buzzard/blob/master/doc/examples.ipynb)
- [_async rasters_ in depth](https://github.com/earthcube-lab/buzzard/blob/master/doc/notebook2/async_rasters.ipynb)

## `buzzard` allows
- Opening and creating [raster](https://buzzard.readthedocs.io/en/latest/dataset_raster.html) and [vector](https://buzzard.readthedocs.io/en/latest/dataset_vector.html) files. Supports all [GDAL drivers (GTiff, PNG, ...)](https://www.gdal.org/formats_list.html) and all [OGR drivers (GeoJSON, DXF, Shapefile, ...)](https://www.gdal.org/ogr_formats.html).
- [Reading](https://buzzard.readthedocs.io/en/latest/source_gdal_file_raster.html#raster-file-get-data) raster files pixels from disk to _numpy.ndarray_.
  - _Options:_ `sub-rectangle reading`, `rotated and scaled sub-rectangle reading (thanks to on-the-fly remapping with OpenCV)`, `automatic parallelization of read and remapping (soon)`, `async (soon)`, `be the source of an image processing pipeline (soon)`.
  - _Properties:_ `thread-safe`
- [Writing](https://buzzard.readthedocs.io/en/latest/source_gdal_file_raster.html#raster-file-set-data) raster files pixels to disk from _numpy.ndarray_.
  - _Options:_ `sub-rectangle writing`, `rotated and scaled sub-rectangle writing (thanks to on-the-fly remapping with OpenCV)`, `masked writing`.
- [Reading](https://buzzard.readthedocs.io/en/latest/source_gdal_file_vector.html#vector-file-iter-data) vector files geometries from disk to _shapely objects_, _geojson dict_ and _raw coordinates_.
  - _Options:_ `masking`.
  - _Properties:_ `thread-safe`
- [Writing](https://buzzard.readthedocs.io/en/latest/source_gdal_file_vector.html#vector-file-insert-data) vector files geometries to disk from _shapely objects_, _geojson dict_ and _raw coordinates_.
- Powerful manipulations of [raster windows](https://buzzard.readthedocs.io/en/latest/footprint.html)
- [Instantiation](https://buzzard.readthedocs.io/en/latest/dataset_recipe.html#buzzard.Dataset.create_raster_recipe) of image processing pipelines where each node is a raster, and each edge is a user defined python function working on _numpy.ndarray_ (beta, partially implemented).
  - _Options:_ `automatic parallelization using user defined thread or process pools`, `disk caching`.
  - _Properties:_ `lazy evaluation`, `deterministic`, `automatic tasks chunking into tiles`, `fine grain task prioritization`, `backpressure prevention`.
- [Spatial reference homogenization](https://buzzard.readthedocs.io/en/latest/dataset.html#on-the-fly-re-projections-in-buzzard) between opened files like a GIS software does (beta)

## Documentation
https://buzzard.readthedocs.io/

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
docker build -t buzz --build-arg PYTHON_VERSION=3.7 https://raw.githubusercontent.com/earthcube-lab/buzzard/master/.circleci/images/base-python/Dockerfile
docker run -it --rm buzz bash
pip install buzzard

```

### Package manager and pip
```sh
# Step 1 - Install GDAL and rtree ******************************************* **
# Windows
# https://www.lfd.uci.edu/~gohlke/pythonlibs/#gdal
# https://www.lfd.uci.edu/~gohlke/pythonlibs/#rtree

# MacOS
brew install gdal
brew tap osgeo/osgeo4mac
brew tap --repair
brew install gdal2
brew install spatialindex
export PATH="/usr/local/opt/gdal2/bin:$PATH"
python3 -m pip install 'gdal==2.3.3'

# Ubuntu
# Run the commands from the following Dockerfile:
# https://github.com/earthcube-lab/buzzard/blob/master/doc/ubuntu_install/Dockerfile

# Step 2 - Install buzzard ************************************************** **
python3 -m pip install buzzard

```

## Supported Python versions
To enjoy the latest buzzard features, update your python!

#### Full python support
- Latest supported version: `3.8` (June 2018)
- Oldest supported version: `3.6` (Sept 2015)

#### Partial python support
- `2.7`: use buzzard version `0.4.4`
- `3.4`: use buzzard version `0.6.3`
- `3.5`: use buzzard version `0.6.4`
- `3.6`: use buzzard version `0.6.4`

## Slack
You want some help? You have a question? You want to contribute? Join us on Slack!

[
![Join us on Slack!](https://cdn.brandfolder.io/5H442O3W/as/pl54cs-bd9mhs-3jsgg0/btn-add-to-slack_1x.png?height=42)
](https://join.slack.com/t/buzzard-python/shared_invite/enQtNjY0NDQ2MzU3MzgzLTJhNTZhNjAwOGIyM2RkOTdkZGE5MGUwZGEzZGQwODkyMzY2N2YwMTg5ZmI1NDc2MjY2MGM2ZTdhNDc3M2E1YTI)

## How to test
```sh
git clone https://github.com/earthcube-lab/buzzard
pip install -r buzzard/requirements-dev.txt
pytest buzzard/buzzard/test
```

## How to build documentation
```sh
cd docs
make html
open _build/html/index.html
```

## Contributions and feedback
Welcome to the `buzzard` project! We appreciate any contribution and feedback, your proposals and pull requests will be considered and responded to. For more information, see the [`CONTRIBUTING.md`](./CONTRIBUTING.md) file.

## Authors
See [AUTHORS](./AUTHORS.md)

## License and Notice
See [LICENSE](./LICENSE) and [NOTICE](./NOTICE).

## Other pages
- [TODO](https://www.notion.so/buzzard/2c94ef6ee8da4d6280834129cc00f4d2?v=334ead18796342feb32ba85ccdfcf69f) on `notion.so`

------------------------------------------------------------------------------------------------------------------------
