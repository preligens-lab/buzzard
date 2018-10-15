import functools
import os

import buzzard as buzz
import numpy as np

import example_tools

from part1 import test_raster

ZOOMABLE_URLS = {
    'andromeda': 'https://cdn.spacetelescope.org/archives/images/zoomable/heic1502a/',
}

def download_tile(fp, *_, url_per_tile):
    url = url_per_tile[fp]


def example():
    ds = buzz.DataSource()

    andromeda_infos = example_tools.infos_of_zoomable_url(
        ZOOMABLE_URLS['andromeda'], max_zoom=9, verbose=True
    )
    for zoom_level, (fp, tiles, url_per_tile) in enumerate(zip(*andromeda_infos)):
        print('Opening andromeda at zoom {}, {}x{} pixels for {} tiles total.'.format(
            zoom_level, *fp.rsize, tiles.size,
        ))
        ds.create_cached_raster_recipe(
            key=f'andromeda_zoom{zoom_level}',

            fp=fp,
            dtype='uint8',
            band_count=3,
            compute_array=functools.partial(
                download_tile,
                url_per_tile=url_per_tile
            ),

            cache_tiles=tiles,
            cache_dir=f'andromeda_zoom{zoom_level}',
        )
        print('d')



if __name__ == '__main__':
    for cache_dir in ZOOMABLE_URLS.keys():
        if os.path.isdir(cache_dir):
            for path in example_tools.list_cache_files_path_in_dir(cache_dir):
                os.remove(path)
    with buzz.Env(allow_complex_footprint=True, warnings=False):
        example()
