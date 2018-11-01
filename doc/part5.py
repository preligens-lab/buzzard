"""
TODO: Give credit to spacetelescope.org

"""

import functools
import os
import multiprocessing as mp
import multiprocessing.pool

import buzzard as buzz
import numpy as np
import skimage.io

import example_tools

from part1 import test_raster

ZOOMABLE_URLS = {
    'andromeda': 'https://cdn.spacetelescope.org/archives/images/zoomable/heic1502a/',
    # 'andromeda': 'https://cdn.spacetelescope.org/archives/images/zoomable/heic1501a/', # Shape problem
    'monocerotis': 'https://cdn.spacetelescope.org/archives/images/zoomable/heic0503a/',

}
DOWNLOAD_POOL = mp.pool.ThreadPool(5)

def main():
    ds = buzz.DataSource(allow_interpolation=True)
    open_zoomable_rasters(ds, 'andromeda')

    # Test 1 - Perform basic tests ****************************************** **
    test_raster(ds.andromeda_zoom0)
    example_tools.show_several_images((
        'andromeda_zoom0', ds.andromeda_zoom0.fp,
        ds.andromeda_zoom0.get_data(band=-1)
    ))

    test_raster(ds.andromeda_zoom1)
    example_tools.show_several_images((
        'andromeda_zoom1', ds.andromeda_zoom1.fp,
        ds.andromeda_zoom1.get_data(band=-1)
    ))

    test_raster(ds.andromeda_zoom2)
    example_tools.show_several_images((
        'andromeda_zoom2', ds.andromeda_zoom2.fp,
        ds.andromeda_zoom2.get_data(band=-1)
    ))

    # Test 2 - Test `get_data` timings ************************************** **
    with example_tools.Timer() as t:
        ds.andromeda_zoom5.get_data(band=-1)
    print(f'Getting andromeda_zoom5 took {t}, download was performed')

    with example_tools.Timer() as t:
        ds.andromeda_zoom5.get_data(band=-1)
    print(f'Getting andromeda_zoom5 took {t}, data was directly fetched from cache')

    print('Closing and opening andromeda rasters again...')
    # ds.close() # TODO: `uncomment` or `close/reopen only the one`
    ds = buzz.DataSource(allow_interpolation=True)
    open_zoomable_rasters(ds, 'andromeda')

    with example_tools.Timer() as t:
        ds.andromeda_zoom5.get_data(band=-1)
    print(f'Getting andromeda_zoom5 took {t}, cache files validity was checked')

    with example_tools.Timer() as t:
        ds.andromeda_zoom5.get_data(band=-1)
    print(f'Getting andromeda_zoom5 took {t}, data was directly fetched from cache')

    example_tools.show_several_images((
        'andromeda_zoom5', ds.andromeda_zoom5.fp,
        ds.andromeda_zoom5.get_data(band=-1)
    ))

    # Test 3 **************************************************************** **
    open_zoomable_rasters(ds, 'monocerotis')
    example_tools.show_several_images((
        'monocerotis_zoom3', ds.monocerotis_zoom3.fp,
        ds.monocerotis_zoom3.get_data(band=-1)
    ))


def open_zoomable_rasters(ds, name):
    infos = example_tools.infos_of_zoomable_url(
        ZOOMABLE_URLS[name], max_zoom=8, verbose=False,
    )
    for zoom_level, (fp, tiles, url_per_tile) in enumerate(zip(*infos)):
        print('  Opening {} at zoom {}, {}x{} pixels split between {} files'.format(
            name, zoom_level, *fp.rsize, tiles.size,
        ))
        ds.create_cached_raster_recipe(
            key=f'{name}_zoom{zoom_level}',

            fp=fp,
            dtype='uint8',
            band_count=3,
            compute_array=functools.partial(
                download_tile,
                url_per_tile=url_per_tile
            ),
            computation_pool=DOWNLOAD_POOL,

            cache_tiles=tiles,
            cache_dir=f'{name}_zoom{zoom_level}',
        )

def download_tile(fp, *_, url_per_tile):
    """A function to be fed to `compute_array` when constructing a recipe"""
    url = url_per_tile[fp]
    arr = skimage.io.imread(url)
    return arr

if __name__ == '__main__':
    with buzz.Env(allow_complex_footprint=True, warnings=False):
        main()
