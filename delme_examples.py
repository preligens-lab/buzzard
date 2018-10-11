

# Example 1: A GDAL file opened in the scheduler


import time

import buzzard as buzz

import example_tools

path = example_tools.create_random_elevation_gtiff()
ds = buzz.DataSource(allow_interpolation=True)

def test_raster(r):
    # Step 1 - Dump informations
    fp = r.fp
    fp_lowres = fp.intersection(fp, scale=fp.scale * 2)
    print(type(r))
    print(r.dtype)
    print(fp)
    print(fp_lowres)

    # Step 2 - Test read
    with example_tools.Timer() as t:
        arr = r.get_data()
    print(f'Array: top_left:{}, dtype:{}, shape:{}, mean-value:{:3.1f}'.format(
        fp.tl, arr.dtype, arr.shape arr.mean()
    ))
    print(f' Fetching the full raster took {t:.1f}')

    # Step 3 - Test read and resampling
    with example_tools.Timer() as t:
        arr = r.get_data(fp=fp_lowres)
    print(f'Array: top_left:{}, dtype:{}, shape:{}, mean-value:{:3.1f}'.format(
        fp_lowres.tl, arr.dtype, arr.shape arr.mean()
    ))
    print(f'Fetching and downsampling took {t:.1f}')

    # Step 4 - Test retrieval of multiple consecutive arrays
    tiles = fp.tile_count(3, 3, boundary_effect='shrink')
    if hasattr(r, 'iter_data'):
        arr_iterator = r.iter_data(tiles.flat)
    else:
        arr_iterator = (
            r.get_data(fp=tile)
            for tile in tiles.flat
        )
    with example_tools.Timer() as t:
        for tile, arr in zip(tiles.flat, arr_iterator):
            print(f'Array: top_left:{}, dtype:{}, shape:{}, mean-value:{:3.1f}'.format(
                tile.tl, arr.dtype, arr.shape arr.mean()
            ))
            time.sleep(0.1) # Simulate a blocking task
    print(f'         Fetching 9 tiles took {t:.1f}\n')


with ds.open_raster(path).close as r:
    # Disk reads are not tiled
    # Resampling operations are not tiled
    test_raster(r)

with ds.open_raster(
        path,
        scheduled=True,
        # `True` is equivalent to
       # `{io_pool='io', resample_pool='cpu', max_resampling_size=512, max_read_size=512}`
    ).close as r:
    # Disk reads are automatically tiled and parallelized
    # Resampling operations are automatically tiled and parallelized
    # `t.iter_data` iternally fills a bounded queue with the results
    test_raster(r)





# Example 2: Deriving a scheduled raster with a recipe
There exit 3 type of scheduled rasters
- GDALScheduledFileRaster
- RasterRecipe
- CachedRasterRecipe
A RasterRecipe is a scheduled raster that computes data on the fly, it may depends on some other
scheduled rasters.
In the following example, `slopes_raster` is a RasterRecipe that depends on `elevation_raster` a GDALScheduledFileRaster.


import time
import multiprocessing as mp
import multiprocessing.pool

import buzzard as buzz
import scipy.ndimage as ndi
import numpy as np

import example_tools

def slopes_of_elevation(fp, primitive_fps, primitive_arrays, slopes_raster):
    print('slopes_of_elevation from shape {} to {}'.format(
        primitive_fps['dem'].shape, fp.shape,
    ))
    arr = primitive_arrays['dem']
    kernel = [
        [0, 1, 0],
        [1, 1, 1],
        [0, 1, 0],
    ]
    arr = ndi.maximum_filter(arr, None, kernel) - ndi.minimum_filter(arr, None, kernel)
    arr = arr[1:-1, 1:-1]
    arr = np.arctan(arr / fp.pxsizex)
    arr = arr / np.pi * 180.
    return arr

path = example_tools.create_random_elevation_gtiff()
ds = buzz.DataSource()

# For `slopes_raster` computations
# For `elevation_raster` disk resamplings
cpu_pool = mp.pool.ThreadPool(mp.cpu_count())

# For `elevation_raster` disk reads 
io_pool = mp.pool.ThreadPool(4) 

params = dict(
    path=path,
    scheduled={io_pool=io_pool, resample_pool=cpu_pool},
)
with ds.open_raster(**params).close as elevation_raster:
    params = dict(
        # The next 6 lines can be replaced by **buzz.algo?.slopes(elevation_raster)
        fp=elevation_raster.fp,
        dtype='float32',
        band_count=1,
        compute_array=slopes_of_elevation,
        queue_data_per_primitive={'dem': elevation_raster.queue_data},
        convert_footprint_per_primitive={'dem': lambda fp: fp.dilate(1)},

        computation_pool=cpu_pool,
    )
    with ds.create_raster_recipe(**params).close as slopes_raster:
        # Test 1 - 
        tiles = elevation_raster.fp.tile_count(2, 2)
        dem_iterator = elevation_raster.iter_data(tiles.flat)
        slopes_iterator = slopes_raster.iter_data(tiles.flat)
        for tile, dem, slopes in zip(tiles.flat, dem_iterator, slopes_iterator):
            print(f'Showing dem and slopes at {tile}')
            example_tools.show_several_images(tile, dem, slopes)

        # Test 2 - Show backpressure prevention




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

