import functools
import multiprocessing as mp
import multiprocessing.pool

import buzzard as buzz
import skimage.io

import example_tools

from part1 import test_raster

ZOOMABLE_URLS = {
    'andromeda': 'https://cdn.spacetelescope.org/archives/images/zoomable/heic1502a/',
    'monocerotis': 'https://cdn.spacetelescope.org/archives/images/zoomable/heic0503a/',

}
DOWNLOAD_POOL = mp.pool.ThreadPool(5)

def main():
    print("All images shown here belong to ESA/Hubble. See spacetelescope.org.\n")

    ds = buzz.Dataset(allow_interpolation=True)
    open_zoomable_rasters(ds, 'andromeda', overwrite=True)

    # Test 1 - Perform basic tests ****************************************** **
    print()
    print('Test 1 - Show andromeda with 3 resolutions')
    test_raster(ds.andromeda_zoom0)
    example_tools.show_several_images((
        'andromeda_zoom0', ds.andromeda_zoom0.fp,
        ds.andromeda_zoom0.get_data()
    ))
    print()

    test_raster(ds.andromeda_zoom1)
    example_tools.show_several_images((
        'andromeda_zoom1', ds.andromeda_zoom1.fp,
        ds.andromeda_zoom1.get_data()
    ))
    print()

    test_raster(ds.andromeda_zoom2)
    example_tools.show_several_images((
        'andromeda_zoom2', ds.andromeda_zoom2.fp,
        ds.andromeda_zoom2.get_data()
    ))
    print()

    # Test 2 - Test `get_data` timings ************************************** **
    print()
    print('Test 2 - Read andromeda 4 times and compare timings')
    with example_tools.Timer() as t:
        ds.andromeda_zoom5.get_data()
    print(f'Getting andromeda_zoom5 took {t}, download was performed')

    with example_tools.Timer() as t:
        ds.andromeda_zoom5.get_data()
    print(f'Getting andromeda_zoom5 took {t}, data was directly fetched from cache')

    print('Closing and opening andromeda rasters again...')
    ds.close()
    ds = buzz.Dataset(allow_interpolation=True)
    open_zoomable_rasters(ds, 'andromeda', overwrite=False)

    with example_tools.Timer() as t:
        ds.andromeda_zoom5.get_data()
    print(f'Getting andromeda_zoom5 took {t}, cache files validity was checked'
          ' and data was fetched from cache')

    with example_tools.Timer() as t:
        ds.andromeda_zoom5.get_data()
    print(f'Getting andromeda_zoom5 took {t}, data was directly fetched from cache')

    example_tools.show_several_images((
        'andromeda_zoom5', ds.andromeda_zoom5.fp,
        ds.andromeda_zoom5.get_data()
    ))

    # Test 3 **************************************************************** **
    print()
    print('Test 2 - Show monocerotis')
    open_zoomable_rasters(ds, 'monocerotis', overwrite=False)
    example_tools.show_several_images((
        'monocerotis_zoom3', ds.monocerotis_zoom3.fp,
        ds.monocerotis_zoom3.get_data()
    ))

def open_zoomable_rasters(ds, name, overwrite):
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
            channel_count=3,
            compute_array=functools.partial(
                download_tile,
                url_per_tile=url_per_tile
            ),
            computation_pool=DOWNLOAD_POOL,

            cache_tiles=tiles,
            cache_dir=f'{name}_zoom{zoom_level}',
            ow=overwrite,
        )

def download_tile(fp, *_, url_per_tile):
    """A function to be fed to `compute_array` when constructing a recipe"""
    url = url_per_tile[fp]
    arr = skimage.io.imread(url)
    return arr

if __name__ == '__main__':
    with buzz.Env(allow_complex_footprint=True):
        main()
