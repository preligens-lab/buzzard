import os

import buzzard as buzz
import numpy as np
import shapely.geometry

import example_tools
from part3 import mandelbrot_of_footprint

CACHE_DIR = 'mandelbrot_100mpx_tiles'

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
    ds = buzz.Dataset()

    # Create a Footprint that ranges from -2 to 2 on both x and y axes
    fp = buzz.Footprint(
        gt=(-2, 4 / 10000, 0, -2, 0, 4 / 10000),
        rsize=(10000, 10000),
    )
    cache_tiling = fp.tile((512, 512), boundary_effect='shrink')
    computation_tiling = fp.tile((128, 128), boundary_effect='shrink')
    cached_recipe_params = dict(
        key='mand_100mpx',

        fp=fp,
        dtype='float32',
        channel_count=1,
        compute_array=mandelbrot_of_footprint,

        cache_dir=CACHE_DIR,
        cache_tiles=cache_tiling,
        computation_tiles=computation_tiling,
    )
    ds.create_cached_raster_recipe(**cached_recipe_params)

    # Test 1 - Timings before and after caching ***************************** **
    print('Test 1 - Read mandelbrot 100mpx twice and compare timings')
    fp = ds.mand_100mpx.fp
    fp = fp & shapely.geometry.Point(-1.1172, -0.221103).buffer(fp.pxsizex * 300)
    print(f'Getting Footprint at {fp.c}...')
    with example_tools.Timer() as t:
        ds.mand_100mpx.get_data(fp=fp)
    print(f'  took {t}')

    print(f'Getting Footprint at {fp.c}...')
    with example_tools.Timer() as t:
        ds.mand_100mpx.get_data(fp=fp)
    print(f'  took {t}')
    print('Tiles in `{}` directory:\n- {}'.format(
        CACHE_DIR,
        '\n- '.join(example_tools.list_cache_files_path_in_dir(CACHE_DIR)),
    ))
    print()

    # Test 2 - Corrupt one cache file and try to reuse the file ************* **
    print('Test 2 - Corrupt one cache file and try to reuse the file')
    ds.mand_100mpx.close()

    # Pick one cache file and append one byte to it
    one_cache_tile_path = example_tools.list_cache_files_path_in_dir(CACHE_DIR)[0]
    print(f'Corrupting {one_cache_tile_path}...')
    with open(one_cache_tile_path, 'ba') as f:
        f.write(b'\0x42')
    ds.create_cached_raster_recipe(**cached_recipe_params)

    print(f'Getting Footprint at {fp.c}...')
    with example_tools.Timer() as t:
        arr = ds.mand_100mpx.get_data(fp=fp)
    print(f'  took {t}')

    example_tools.show_several_images((
        'part of mandelbrot 100 mega pixels',
        fp,
        arr,
    ))

    return # The NEXT features are not yet implemented

    # Test 4 - Colorize mandelbrot 100mpx ************************************ **
    ds.create_raster_recipe(
        'mand_red',
        fp=fp,
        dtype='uint8',
        channel_count=3,
        compute_array=colorize_mandelbrot,
        queue_data_per_primitive={'mand': ds.mand_100mpx.queue_data},
        computation_tiles=computation_tiling,
        automatic_remapping=False,
    )
    example_tools.show_several_images((
        'part of mandelbrot 10 mega pixels in red',
        fp,
        ds.mand_red.get_data(fp=fp),
    ))

if __name__ == '__main__':
    if os.path.isdir(CACHE_DIR):
        for path in example_tools.list_cache_files_path_in_dir(CACHE_DIR):
            os.remove(path)
    with buzz.Env(allow_complex_footprint=True):
        example()
