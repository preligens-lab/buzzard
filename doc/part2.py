"""
# Part 2: Deriving the slopes from a dem using a _raster recipe_
In _buzzard_ there are 3 types of raster managed by the _DataSource_'s scheduler:
- _AsyncStoredRaster_, seen in `Part 1`,
- _NocacheRasterRecipe_, seen in this part,
- _CachedRasterRecipe_, seen in `Part 4`.

All those rasters are called _async rasters_.

### A new type of raster: _recipes_
A _recipe_ is an _async raster_ that __computes data on the fly__ by calling the `compute_array` function provided in the constructor. This function takes a _Footprint_ that defines a rectangle to compute, and it returns a _ndarray_ containing the pixels computed at this location. This function will be called in parallel given the `computation_pool` parameter provided in the constructor.

A _recipe_ may __depend on some other *async rasters*__. In this example, `ds.slopes` is a _NocacheRasterRecipe_ that depends on `ds.elevation`, an _AsyncStoredRaster_. To declare the dependancy of `ds.slopes` on `ds.elevation`, in the constructor of `ds.slopes` you must provide `queue_data_per_primitive={'some_key': ds.elevation.queue_data}`, to allow the scheduler to issue queries to elevation when the slopes requires it. The `compute_array` function of `ds.slopes` will take as parameter the _ndarray_ of `ds.dem` previously extracted.

A _recipe_ may depend on more than one _async raster_, and a _recipe_ that depends on an _async raster_ may also be needed by another recipe. This means that recipes can be assembled to form __computation graphs__ of any width and any depth.

### Parallelization within _async rasters_
The computation intensive and io-bound steps of the scheduler are __defered to thread pools__ by default. You can configure the pools in the _async rasters_ constructors. Those parameters can be of the following types:
- A _multiprocessing.pool.ThreadPool_, should be the default choice.
- A _multiprocessing.pool.Pool_, a process pool. Useful for computations that requires the [GIL](https://en.wikipedia.org/wiki/Global_interpreter_lock) or that leaks memory.
- `None`, to request the scheduler thread to perform the tasks itself. Should be used when the computation is very light.
- A _hashable_ (like a _string_), that will map to a pool registered in the _DataSource_. If that key is missing from the _DataSource_, a _ThreadPool_ with `multiprocessing.cpu_count()` workers will be automatically instanciated.

"""

import os
import time
import multiprocessing as mp
import multiprocessing.pool

import buzzard as buzz
import numpy as np
import scipy.ndimage

import example_tools
from part1 import test_raster

def main():
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
        async_={'io_pool': io_pool, 'resample_pool': cpu_pool},
    )
    ds.create_raster_recipe(
        'slopes',
        computation_pool=cpu_pool,

        # The next 6 lines can be replaced by **buzz.algo.slopes(ds.elevation) TODO: algo?
        fp=ds.elevation.fp,
        dtype='float32',
        band_count=1,
        compute_array=slopes_of_elevation,
        queue_data_per_primitive={'dem': ds.elevation.queue_data},
        convert_footprint_per_primitive={'dem': lambda fp: fp.dilate(1)},
    )

    # Test 1 - Perform basic tests ****************************************** **
    test_raster(ds.slopes)

    # Test 2 - Multiple iterations at the same time ************************* **
    tiles = ds.elevation.fp.tile_count(2, 2).flatten()
    dem_iterator = ds.elevation.iter_data(tiles)
    slopes_iterator = ds.slopes.iter_data(tiles)
    for tile, dem, slopes in zip(tiles, dem_iterator, slopes_iterator):
        print(f'Showing dem and slopes at:\n {tile}')
        example_tools.show_several_images(
            ('elevation (dem)', tile, dem),
            ('slopes', tile, slopes),
        )

    # Test 3 - Backpressure prevention ************************************** **
    tiles = ds.slopes.tile_count(3, 3).flatten()

    print('Creating a slopes iterator on 9 tiles')
    it = ds.slopes.iter_data(tiles, max_queue_size=1)
    print('  At most 5 dem arrays can be ready between `ds.elevation` and '
          '`ds.slopes`')
    print('  At most 1 slopes array can be ready out of the slopes iterator')

    print('Sleeping several seconds to let the scheduler create 6/9 dem '
          'arrays, and 1/9 slopes arrays.')
    time.sleep(4)

    with example_tools.Timer() as t:
        arr = next(it)
    print(f'Getting the first array took {t}, this was instant because it was '
          'ready')

    with example_tools.Timer() as t:
        for _ in range(5):
            next(it)
    print(f'Getting the next 5 arrays took {t}, it was quick because the dems '
          'were ready')

    with example_tools.Timer() as t:
        for _ in range(3):
            next(it)
    print(f'Getting the last 4 arrays took {t}, it was long because nothing was'
          ' ready')

    # Cleanup *************************************************************** **
    ds.close()
    os.remove(path)

def slopes_of_elevation(fp, primitive_fps, primitive_arrays, slopes):
    """A function to be fed to `compute_array` when constructing a recipe"""
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
    main()
