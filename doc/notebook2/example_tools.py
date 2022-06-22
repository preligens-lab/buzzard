import datetime
import glob
import time
import urllib
import urllib.parse
import urllib.request
from concurrent.futures import ProcessPoolExecutor
import http.client
import os
import gc

import matplotlib.pyplot as plt
import buzzard as buzz
import xmltodict
import buzzard as buzz
import numpy as np
from tqdm import tqdm



class Timer():

    def __enter__(self):
        self._start = datetime.datetime.now()
        return self

    def __exit__(self, *_):
        self._stop = datetime.datetime.now()

    def __float__(self):
        dt = self._stop - self._start
        dt = dt.total_seconds()
        return dt

    def __str__(self):
        return f'\033[33m{float(self):.4f} sec\033[0m'

def list_cache_files_path_in_dir(cache_dir):
    s = os.path.join(cache_dir, '*_[0123456789abcdef]*.tif')
    return glob.glob(s)

def show_several_images(*args):
    for title, fp, arr in args:
        # fig = plt.figure()
        fig = plt.figure(figsize=(8 / fp.height * fp.width, 8))
        ax = fig.add_subplot(111)
        # plt.imshow(arr, extent=fp.extent)
        ax.imshow(arr, extent=fp.extent)
        plt.show()
    plt.close('all')
    gc.collect() # Collect to avoid some rare problem


"""
# spacetelescope.org
- https://www.spacetelescope.org/images/viewall/
- https://www.spacetelescope.org/images/json/
- https://www.spacetelescope.org/press/image_formats/

### Sharpest ever view of the Andromeda Galaxy (heic15022a)
- https://www.spacetelescope.org/images/heic1502a/
- https://www.spacetelescope.org/images/heic1502a/zoomable/
- http://cdn.spacetelescope.org/archives/images/zoomable/heic1502a/ImageProperties.xml

# wikimedia
- https://commons.wikimedia.org/wiki/Help:Zoomable_images/dezoomify.py

"""

def infos_of_zoomable_url(img_dir, images_center=np.asarray([0., 0.]),
                          max_zoom=None,
                          check_all_urls=False, verbose=False):
    """Extract footprints and tiles' url from the url of a zoomable image"""

    # Read ImagesProperties.xml
    xml_url = urllib.parse.urljoin(img_dir, 'ImageProperties.xml')
    content = get_url(xml_url)
    if verbose:
        print(f"""content of {xml_url}:
        {content}""")
    content = xmltodict.parse(content)
    full_width = np.int_(content['IMAGE_PROPERTIES']['@WIDTH'])
    full_height = np.int_(content['IMAGE_PROPERTIES']['@HEIGHT'])
    full_rsize = np.asarray([full_width, full_height])
    max_tile_size = np.int_(content['IMAGE_PROPERTIES']['@TILESIZE'])

    assert full_width > 0
    assert full_height > 0
    assert max_tile_size > 0

    # Calculate zoom count
    def _zoom_count():
        tile_count = np.ceil(full_rsize / max_tile_size).astype(int)
        zoom_count = 0
        while True:
            zoom_count += 1
            if sum(tile_count) == 2:
                break
            tile_count = np.ceil(tile_count / 2).astype(int)
        return zoom_count
    zoom_count = _zoom_count()
    # if max_zoom is not None:
        # zoom_count = min(zoom_count, max_zoom)
    if verbose:
        print(f'{zoom_count} zoom levels')

    # Build zoom's Footprint
    def _footprint_per_zoom():
        for zoom in range(zoom_count):
            pxsizex = 2 ** (zoom_count - zoom - 1)
            scale = np.asarray([pxsizex, -pxsizex])
            pxvec = scale
            rsize = np.floor(full_rsize / pxsizex).astype(int)
            size = rsize * pxsizex

            tl = images_center - (rsize / 2 * pxvec)
            yield buzz.Footprint(tl=tl, size=size, rsize=rsize)

    fp_per_zoom = list(_footprint_per_zoom())

    # Check tile count found against attributes
    expected_tile_count = int(content['IMAGE_PROPERTIES']['@NUMTILES'])
    tile_count = sum(
        np.ceil(fp.rw / max_tile_size) * np.ceil(fp.rh / max_tile_size)
        for fp in fp_per_zoom
    )
    assert tile_count == expected_tile_count, f"""
    ImageProperties.xml states NUMTILES="{expected_tile_count}"
    but tile_count={tile_count} was calculated
    """

    # Apply the max_zoom parameter
    if max_zoom is not None:
        fp_per_zoom = fp_per_zoom[:max_zoom]

    # Create the tile's Footprint
    tile_matrix_per_zoom = [
        fp.tile((max_tile_size, max_tile_size), boundary_effect='shrink')
        for fp in fp_per_zoom
    ]

    if verbose:
        for zoom, (fp, tiles) in enumerate(zip(fp_per_zoom, tile_matrix_per_zoom)):
            print(f'''zoom {zoom}
                  fp.tl: {fp.tl}, fp.c: {fp.c}, fp.br: {fp.br}
                fp.size: {fp.size}, fp.rsize: {fp.rsize}
        size of a pixel: {fp.pxsize}
            pixel count: {fp.rarea:,}, byte count: {fp.rarea * 3:,}
        number of tiles: {tiles.size}, tiles.shape: {tiles.shape}''')

    # Build all paths
    def _tile_url(zoom, x, y):
        pxsizex = pow(2, zoom_count - zoom - 1)
        index = x + y * int(np.ceil(np.floor(full_width / pxsizex) / max_tile_size))
        for i in range(1, zoom + 1):
            index += int(np.ceil(np.floor(full_width / pow(2, zoom_count - i)) / max_tile_size)) * \
                     int(np.ceil(np.floor(full_height / pow(2, zoom_count - i)) / max_tile_size))
        group = index // 256
        url = urllib.parse.urljoin(img_dir, f'TileGroup{group}/{zoom}-{x}-{y}.jpg')
        return url

    url_dict_per_zoom = []
    for zoom, tiles in enumerate(tile_matrix_per_zoom):
        path_dict = {
            tiles[y, x]: _tile_url(zoom, x, y)
            for y, x in np.ndindex(*tiles.shape)
        }
        url_dict_per_zoom.append(path_dict)
        if check_all_urls:
             # Checking ~32000 tiles takes ~45min
            _assert_urls_exists(path_dict.values())

    return fp_per_zoom, tile_matrix_per_zoom, url_dict_per_zoom

def _url_status(url):
    parse_obj = urllib.parse.urlparse(url)

    timer = 1
    for i in range(6):
        try:
            connection = http.client.HTTPConnection(parse_obj.netloc)
            connection.request('HEAD', parse_obj.path)
            break
        except Exception as e:
            print(url, e, 'sleep', timer)
            time.sleep(timer)
            timer *= 2
    else:
        return e

    response = connection.getresponse()
    connection.close()
    return response.status

def _assert_urls_exists(urls):

    with ProcessPoolExecutor(20) as ex:
        it = ex.map(_url_status, urls, chunksize=10)
        it = zip(
            urls,
            tqdm(it, total=len(urls), desc='checking urls'),
        )
        for i, (url, code) in enumerate(it):
            assert code == 200, (url, code)
        assert i == (len(urls) - 1)

def get_url(url):
    req_headers = {
        'User-Agent': 'Mozilla/5.0 (Windows; U; Windows NT 5.1; en-US) AppleWebKit/525.13 (KHTML, like Gecko) Chrome/0.A.B.C Safari/525.13',
        'Referer': url
    }

    request = urllib.request.Request(url, headers=req_headers)
    opener = urllib.request.build_opener()
    timer = 1
    for i in range(1):
        try:
            return opener.open(request).read()
        except:
            time.sleep(timer)
            timer *= 2
    raise OSError("Unable to download `%s`."%url)
