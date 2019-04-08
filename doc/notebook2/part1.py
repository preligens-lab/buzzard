import time
import os

import buzzard as buzz

import example_tools

def main():
    path = example_tools.create_random_elevation_gtiff()
    ds = buzz.DataSource(allow_interpolation=True)

    print('Classic opening')
    # Features:
    # - Disk reads are not tiled
    # - Resampling operations are not tiled
    with ds.aopen_raster(path).close as r:
        test_raster(r)

    return # The NEXT features are not yet implemented

    print('Opening within scheduler')
    # Features:
    # - Disk reads are automatically tiled and parallelized
    # - Resampling operations are automatically tiled and parallelized
    # - `iter_data()` method is available
    with ds.aopen_raster(path, async_=True).close as r:
        # `async_=True` is equivalent to
        # `async_={}`, and also equivalent to
        # `async_={io_pool='io', resample_pool='cpu', max_resampling_size=512, max_read_size=512}`
        test_raster(r)

    # `DataSource.close()` closes all rasters, the scheduler, and the pools.
    # If you let the garbage collector collect the `DataSource`, the rasters and
    # the scheduler will be correctly closed, but the pools will leak memory.
    ds.close()

    os.remove(path)

def test_raster(r):
    """Basic testing functions. It will be reused throughout those tests"""
    print('| Print raster informations')
    fp = r.fp
    if r.get_keys():
        print(f'|   key: {r.get_keys()[0]}')
    print(f'|   type: {type(r).__name__}')
    print(f'|   dtype: {r.dtype}, band-count: {len(r)}')
    print(f'|   Footprint: center:{fp.c}, scale:{fp.scale}')
    print(f'|              size(m):{fp.size}, raster-size(px):{fp.rsize}')
    fp_lowres = fp.intersection(fp, scale=fp.scale * 2)

    # *********************************************************************** **
    print('| Test 2 - Getting the full raster')
    with example_tools.Timer() as t:
        arr = r.get_data(band=-1)
    print(f'|   took {t}, {fp.rarea / float(t):_.0f} pixel/sec')

    # *********************************************************************** **
    print('| Test 3 - Getting and downsampling the full raster')
    with example_tools.Timer() as t:
        arr = r.get_data(fp=fp_lowres, band=-1)
    print(f'|   took {t}, {fp_lowres.rarea / float(t):_.0f} pixel/sec')

    # *********************************************************************** **
    print('| Test 4 - Getting the full raster in 9 tiles with a slow main'
          'thread')
    tiles = fp.tile_count(3, 3, boundary_effect='shrink').flatten()
    if hasattr(r, 'iter_data'):
        # Using `iter_data` of async rasters
        arr_iterator = r.iter_data(tiles, band=-1)
    else:
        # Making up an `iter_data` for classic rasters
        arr_iterator = (
            r.get_data(fp=tile, band=-1)
            for tile in tiles
        )
    with example_tools.Timer() as t:
        for tile, arr in zip(tiles, arr_iterator):
            time.sleep(1 / 9)
    print(f'|   took {t}, {r.fp.rarea / float(t):_.0f} pixel/sec')

if __name__ == '__main__':
    main()
