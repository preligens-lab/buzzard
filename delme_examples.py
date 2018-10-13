def lol(): pass

"""
# Part 1: A GDAL elevation file opened in the scheduler
By default in buzzard when calling `get_data()` on a file opened with a gdal driver, all the data is read at once (using one `gdal.Band.ReadAsArray`), and all the optional resampling is performed at once (using one `cv2.remap` for example). When performing this operation on a large chunk of data it would be much more efficient to read and resample tile by tile in parallel, and then perform reading and resampling at the same time. To do so, the `scheduled` parameter in `open_raster` (or `create_raster`) should be `True` (or `{params}`).

Another feature unlocked by using a sheduled raster to read a file is the `iter_data()` method. This method does not return an `ndarray` but an `iterator of ndarray` and it takes as parameter not one `Footprint` but a `list of Footprint` to generate. Using this method allows the system to prepare data in advance.

It also takes an optional `max_queue_size=5` parameter to determine how much `ndarray` should be made available in advance. This features allows you to prevent backpressure if you consume the `iterator of ndarray` too slowly.
"""

import time

import buzzard as buzz

import example_tools

def test_raster(r):
    print('Test 1 - Print raster informations') # *************************** **
    fp = r.fp
    fp_lowres = fp.intersection(fp, scale=fp.scale * 2)
    print(f'  type: {type(r)}')
    print(f'  dtype: {r.dtype}')
    print(f'  Footprint: center:{fp.c}, scale:{fp.scale}')
    print(f'             size:{fp.size}, raster-size:{fp.rsize}')

    print('Test 2 - Reading/computing the full raster') # ******************* **
    with example_tools.Timer() as t:
        arr = r.get_data()
    print(f'  array: px-width:{}, dtype:{}, shape:{}, mean-value:{:3.3f}'.format(
        fp.pxsizex, arr.dtype, arr.shape arr.mean(),
    ))
    print(f'  took {t:.1f}')

    print('Test 3 - Reading/computing and downsampling the full raster') # ** **
    with example_tools.Timer() as t:
        arr = r.get_data(fp=fp_lowres)
    print(f'  array: px-width:{}, dtype:{}, shape:{}, mean-value:{:3.3f}'.format(
        fp_lowres.pxsizex, arr.dtype, arr.shape arr.mean(),
    ))
    print(f'  took {t:.1f}')

    print('Test 4 - Test reading/computing 9 consecutive arrays') # ********* **
    tiles = fp.tile_count(3, 3, boundary_effect='shrink')
    if hasattr(r, 'iter_data'):
        # Using `iter_data` of scheduled rasters
        arr_iterator = r.iter_data(tiles.flat)
    else:
        # Making up an `iter_data` for classic rasters
        arr_iterator = (
            r.get_data(fp=tile)
            for tile in tiles.flat
        )
    with example_tools.Timer() as t:
        for tile, arr in zip(tiles.flat, arr_iterator):
            print(f'  array: px-width:{}, dtype:{}, shape:{}, mean-value:{:3.3f}'.format(
                tile.pxsizex, arr.dtype, arr.shape arr.mean(),
            ))
            time.sleep(0.1) # Simulate a blocking task on this thread
    print(f'  took {t:.1f}\n')


if __name__ == '__main__':
    path = example_tools.create_random_elevation_gtiff()
    ds = buzz.DataSource(allow_interpolation=True)

    with ds.open_raster(path).close as r:
        # Disk reads are not tiled
        # Resampling operations are not tiled
        test_raster(r)

    with ds.open_raster(
            path,
            scheduled=True,
            # Using `True` for `scheduled` is equivalent to
            # `{io_pool='io', resample_pool='cpu', max_resampling_size=512, max_read_size=512}`
    ).close as r:
        # Disk reads are automatically tiled and parallelized
        # Resampling operations are automatically tiled and parallelized
        # `t.iter_data` internally fills a bounded queue with the results
        test_raster(r)




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
        'elevation'
        path=path,
        scheduled={'io_pool'=io_pool, 'resample_pool'=cpu_pool},
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



"""
# Part 3: Mandelbrot set computed on the fly

In the CachedRasterRecipe and by default in the RasterRecipe, the Footprints to be computed by your `compute_array` are on the same grid and inside the raster's Footprint. This means that:
- If a Footprint on a different grid is queried (like in a `get_data` call), the scheduler takes care of resampling as needed after calling your `compute_array` function.
- If a Footprint partially or fully outside of the raster's extent is queried, the scheduler will call your `compute_array` function to get the interior pixels and then pad the output with nodata.

In the following example 6 rasters are instanciated that way.
<br/>

The above mentioned system can be deactivated by passing 'automatic_remapping=False' to the constructor of a RasterRecipe, in this case the scheduler will call your `compute_array` function for any kind of Footprint, your function must be able to comply with any request. In `Part 2` the slopes could have been opened that way without changing the rest of the code, the resampling operations would have been deferred to the `elevation` raster.

In the following example ds.mand is instanciated that way.

"""

import buzzard as buzz
import numpy as np
from numba import jit
import shapely.geometry

import example_tools
from part1 import test_raster

@jit(nopython=True, nogil=True, cache=True)
def mandelbrot_jit(array, tl, scale, maxit):
    """Compute a https://en.wikipedia.org/wiki/Mandelbrot_set"""
    for j in range(array.shape[0]):
        y0 = tl[1] + j * scale[1]
        for i in range(array.shape[1]):
            x0 = tl[0] + i * scale[0]
            x, y, x1, y2 = 0., 0., 0., 0.
            iteration = 0
            while x2 + y2 < 4 and iteration < maxit:
                y = 2 * x * y + y0
                x = x2 - y2 + x0
                x2 = x * x
                y2 = y * y
                iteration += 1
            array[j][i] = iteration

def mandelbrot_of_footprint(fp, *_):
    maxit = 4 / fp.pxsizex
    print('Computing {} pixels at scale {} with {} maximum iterations.'.format(
        fp.rarea, fp.pxsizex, maxit,
    ))
    array = np.empty(fp.shape, 'uint32')
    mandelbrot_jit(array, fp.tl, fp.scale, maxit)
    array = array.astype('float32') / maxit
    return array


def example():
    ds = buzz.DataSource(allow_interpolation=True)
    rwidths = {
        'mand_100px': 10,
        'mand_10kpx': 100,
        'mand_1mpx': 1_000,
        'mand_100mpx': 10_000,
        'mand_10gpx': 100_000,
        'mand_1tpx': 1_000_000,
    }

    # Instanciate 6 fixed scale mandelbrot rasters
    for key, rwidth in rwidth.items():
        # Create a Footprint that ranges from -2 to 2 on both x and y axes
        fp = buzz.Footprint(
            gt=(-2, 4 / rwidth, 0, -2, 0, 4 / rwidth),
            rsize=(rwidth, rwidth),
        )
        ds.create_raster_recipe(
            key,
            fp=fp,
            dtype='float32',
            band_count=1,
            compute_array=mandelbrot_of_footprint,
            automatic_remapping=True, # default value
            max_computation_size=64,
        )

    # Instanciate 1 flexible scale mandelbrot raster
    ds.mand = ds.create_raster_recipe(
        key,
        fp=ds.mand_10kpx.fp, # Scale of Footprint does not mean much here, extent is still important
        dtype='float32',
        band_count=1,
        compute_array=mandelbrot_of_footprint,
        automatic_remapping=False,
        max_computation_size=64,
    )

    # Test 1 - Perform basic tests ****************************************** **
    test_raster(ds.mand_100px)
    test_raster(ds.mand_10kpx)

    # Test 2 - Play with resampling with the non-flexible scale rasters ***** **
    fp100k = buzz.Footprint(
        gt=(-2, 4 / 316, 0, -2, 0, 4 / 316),
        rsize=(316, 316),
    )
    example_tools.show_several_images(
        ('10kpx', ds.mand_10kpx.fp, ds.mand_10kpx.get_data()),
        ('1mpx', ds.mand_1mpx.fp, ds.mand_1mpx.get_data()),
        ('10kpx to 100kpx', fp100k, ds.mand_10kpx.get_data(fp=fp100k)), # upsample * 10
        ('1mpx to 100kpx', fp100k, ds.mand_1mpx.get_data(fp=fp100k)), # downsample * 10
    )

    # Test 3 - Play with with the flexible scale raster ********************* **
    example_tools.show_several_images(
        ('10kpx', ds.mand_10kpx.fp, ds.mand.get_data(fp=ds.mand_10kpx.fp)),
        ('1mpx', ds.mand_1mpx.fp, ds.mand.get_data(fp=ds.mand_1mpx.fp)),
    )

    # Test 4 - Zoom to a point ********************************************** **
    focus = shapely.geometry.Point(-1.1172, -0.221103)
    for key in rwidths.items():
        fp = ds[key].fp
        fp = fp.dilate(250) & focus.buffer(fp.pxsizex * 250)
        arr = ds[key].get_data(fp=fp)
        title = f'{fp.rw}x{fp.rh} rect of the {key[5:]} image'
        example_tools.show_several_images((title, fp, arr))

if __name__ == '__main__':
    with buzz.Env(allow_complex_footprint=True, warnings=False):
        example()




"""
# Part 4: Mandelbrot 10 mega pixels computed on the fly and cached on disk
By using the DataSource.create_cached_raster_recipe factory you can create a recipe that caches the results tile by tile in a directory.

The CachedRasterRecipe takes two tilings of the raster's Footprint as parameter, those tilings can be computed by using the `tile*` methods of the Footprint class:
- The `cache_tiles` tiling will define what pixels go into which cache file. To correctly reopen a cached raster after closing it the `cache_tiles` parameter should be identical from the time it was created. A cache file is saved with its md5 hash stored in its file name, if a cache file was corrupted in any way between two openings, the file will be removed and recomputed. By default tiles of size 512x512 are generated.
- The `computation_tiles` tiling defines the exhaustive set of Footprints that can be queried to the `compute_array` function. This can for example be used to get some overlap in the computations or to enforce a constant size in the Footprints to compute. By default this tiling will be the same as `cache_tiles`.
"""

from part3 import mandelbrot_of_footprint

def colorize_mandelbrot(fp, primitive_fps, primitive_arrays, raster):
    arr = primitive_arrays['mand']

    # Colorize converging pixels
    arr[arr == 1.] = 0.

    # Colorize diverging pixels
    maxit = 4 / fp.pxsizex
    arr = np.floor((arr * maxit * 7.) % 256.)
    # arr = np.floor((maxit * 100. / arr) % 256.)

    # Turn to rgb
    res = np.zeros(np.r_[fp.shape, 3], 'uint8')
    res[..., 0] = arr
    return res

def example():
    ds = buzz.DataSource

    # Create a Footprint that ranges from -2 to 2 on both x and y axes
    fp = buzz.Footprint(
        gt=(-2, 4 / 3162, 0, -2, 0, 4 / 3162),
        rsize=(3162, 3162),
    )
    cache_dir = 'mandelbrot_10mpx_tiles'
    cache_tiling = fp.tile((512, 512), boundary_effect='shrink')
    computation_tiling = fp.tile((128, 128), boundary_effect='shrink')
    cached_recipe_params = dict(
        key='mand_10mpx',

        fp=fp,
        dtype='float32',
        band_count=1,
        compute_array=mandelbrot_of_footprint,

        cache_dir=cache_dir,
        cache_tiles=cache_tiling,
        computation_tiles=computation_tiling,
    )
    ds.create_cached_raster_recipe(**cached_recipe_params)

    # Test 1 - Timings before and after caching ***************************** **
    fp = ds.mand_10mpx.fp
    fp = fp & shapely.geometry.Point(-1.1172, -0.221103).buffer(fp.pxsizex * 300)
    print(f'Getting Footprint at {fp.c}...')
    with example_tools.Timer() as t:
        ds.mand_10mpx.get_data(fp=fp)
    print(f'  took {t}')
    print(f'Getting Footprint at {fp.c}...')
    with example_tools.Timer() as t:
        ds.mand_10mpx.get_data(fp=fp)
    print(f'  took {t}')
    print('Tiles in {}:\n{}'.format(
        cache_dir,
        '\n'.join(example_tools.list_cache_files_path_in_dir(cache_dir)),
    ))

    # Test 2 - Corrupt one cache file and try to reuse the file ************* **
    ds.mand_10mpx.close()

    # Pick one cache file and append one byte to it
    one_cache_tile_path = example_tools.list_cache_files_path_in_dir(cache_dir)[0]
    print(f'Corrupting {one_cache_tile_path}...')
    with open(one_cache_tile_path, 'ba') as f:
        f.write(b'\0x42')
    ds.create_cached_raster_recipe(**cached_recipe_params)

    print(f'Getting Footprint at {fp.c}...')
    with example_tools.Timer() as t:
        ds.mand_10mpx.get_data(fp=fp)
    print(f'  took {t}')

    # Test 3 - Colorize mandelbrot 10mpx ************************************ **
    ds.create_raster_recipe(
        'mand_red',
        fp=fp,
        dtype='uint8',
        band_count=3,
        compute_array=colorize_mandelbrot,
        queue_data_per_primitive={'mand': ds.mand_10mpx.queue_data},
        computation_tiles=computation_tiling,
        automatic_remapping=False,
    )
    example_tools.show_several_images((
        'part of mandelbrot 10 mega pixels in red',
        fp,
        ds.mand_red.get_data(fp=fp),
    ))

if __name__ == '__main__':
    with buzz.Env(allow_complex_footprint=True, warnings=False):
        example()

Automatic tiling and parallelization of algorithms that can be tiled
Automatic tiling and parallelization of disk read operations
Automatic tiling and parallelization of resampling operations

The tool should allow disk caching of pixels expensive to compute
The tool should explicitly prevent back pressure

Scale to any number of rasters
Scale to any raster size

The tool should maximize gpu and cpu usage
The overhead induced by the tool should be low
The memory footprint should be as low as possible
The computation of pixels should be lazy
