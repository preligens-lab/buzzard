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
    print('  array: px-width:{}, dtype:{}, shape:{}, mean-value:{:3.3f}'.format(
        fp.pxsizex, arr.dtype, arr.shape, arr.mean(),
    ))
    print(f'  took {t:.1f}')

    print('Test 3 - Reading/computing and downsampling the full raster') # ** **
    with example_tools.Timer() as t:
        arr = r.get_data(fp=fp_lowres)
    print('  array: px-width:{}, dtype:{}, shape:{}, mean-value:{:3.3f}'.format(
        fp_lowres.pxsizex, arr.dtype, arr.shape, arr.mean(),
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
            print('  array: px-width:{}, dtype:{}, shape:{}, mean-value:{:3.3f}'.format(
                tile.pxsizex, arr.dtype, arr.shape, arr.mean(),
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
