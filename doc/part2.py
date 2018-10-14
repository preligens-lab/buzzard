
"""
# Part 2: Deriving the slopes from a dem using a recipe
There exist 3 types of scheduled rasters in buzzard
- GDALScheduledFileRaster
- RasterRecipe
- CachedRasterRecipe
A RasterRecipe is a scheduled raster that computes data on the fly, it may depends on some other scheduled rasters.

In the following example, `ds.slopes` is a RasterRecipe that depends on `ds.elevation` a GDALScheduledFileRaster. To declare de dependancy of `ds.slopes` on `ds.elevation`, in the constructor of the slopes you must pass 'queue_data_per_primitive={'some_key': ds.elevation.queue_data}', to allow the scheduler to issue queries to elevation when the slopes requires it.

A recipe may depend on multiple rasters, and a recipe that depends on a scheduled raster may also be needed by another recipe. This means that recipes can be assembled to form computation graphs of any width and any depth.
<br/>

The computation intensive and io-bound steps of the scheduler are defered to thread pools by default. You can configure the pools in the scheduled raster constructors. Those parameters can be the following:
- A multiprocessing.pool.ThreadPool.
- A multiprocessing.pool.Pool, a process pool.
- None, to get the tasks performed by the scheduler
- A hashable (like a string), that will map to a ThreadPool automatically instanciated
<br/>

The scheduler is designed to scale O(nlogn) in the number of active raters at once and in the number of queries active at once. Be careful though, the scheduler is coded in python and subject to the GIL, it is not designed for real time use cases. Porting the implementation to c++ is conceivable.
"""

import time
import multiprocessing as mp
import multiprocessing.pool

import buzzard as buzz
import scipy.ndimage
import numpy as np

import example_tools
from part1 import test_raster

def slopes_of_elevation(fp, primitive_fps, primitive_arrays, slopes):
   print('slopes_of_elevation from shape {} to {}'.format(
       primitive_fps['dem'].shape, fp.shape,
   ))
   arr = primitive_arrays['dem']
   kernel = [
       [0, 1, 0],
       [1, 1, 1],
       [0, 1, 0],
   ]
   arr = (
       scipy.ndimage.maximum_filter(arr, None, kernel) -
       scipy.ndimage.minimum_filter(arr, None, kernel)
   )
   arr = arr[1:-1, 1:-1]
   arr = np.arctan(arr / fp.pxsizex)
   arr = arr / np.pi * 180.
   return arr


if __name__ == '__main__':
    path = example_tools.create_random_elevation_gtiff()
    ds = buzz.DataSource()

    # Pool to parallelize:
    # - `ds.slopes` computations
    # - `ds.elevation` resamplings
    cpu_pool = mp.pool.ThreadPool(mp.cpu_count())

    # Pool to parallelize:
    # - `ds.elevation` disk reads
    io_pool = mp.pool.ThreadPool(4)

    ds.open_raster(
        'elevation',
        path=path,
        scheduled={'io_pool': io_pool, 'resample_pool': cpu_pool},
    )
    ds.create_raster_recipe(
        'slopes',

        # The next 6 lines can be replaced by **buzz.algo?.slopes(ds.elevation)
        fp=ds.elevation.fp,
        dtype='float32',
        band_count=1,
        compute_array=slopes_of_elevation,
        queue_data_per_primitive={'dem': ds.elevation.queue_data},
        convert_footprint_per_primitive={'dem': lambda fp: fp.dilate(1)},

        computation_pool=cpu_pool,
    )

    # Test 1 - Perform basic tests ****************************************** **
    test_raster(ds.slopes)

    # Test 2 - Multiple iterations at the same time ************************* **
    tiles = ds.elevation.fp.tile_count(2, 2)
    dem_iterator = ds.elevation.iter_data(tiles.flat)
    slopes_iterator = ds.slopes.iter_data(tiles.flat)
    for tile, dem, slopes in zip(tiles.flat, dem_iterator, slopes_iterator):
        print(f'Showing dem and slopes at {tile}')
        example_tools.show_several_images(
            ('elevation (dem)', tile, dem),
            ('slopes', tile, slopes),
        )

    # Test 3 - Backpressure prevention ************************************** **
    tiles = ds.slopes.tile_count(3, 3)

    print('creating a slopes iterator on 9 tiles')
    it = ds.slopes.iter_data(
        tiles.flat, max_queue_size=1,
    )

    print('sleeping to allow 6/9 dem arrays to be read, and 1/9 slopes array to be ready')
    time.sleep(4)

    with example_tools.Timer() as t:
        arr = next(it)
    print(f'getting the first array took {t}, this was instant because it was ready')

    with example_tools.Timer() as t:
        for _ in range(5):
            next(it)
    print(f'getting the next 5 arrays took {t}, it was quick because the dems were ready')

    with example_tools.Timer() as t:
        for _ in range(3):
            next(it)
    print(f'getting the last 4 arrays took {t}, it was long because nothing was ready')
