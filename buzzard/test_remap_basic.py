import numpy as np
from pprint import pprint

from _sequential_gdal_file_raster import *
import buzzard as buzz
print('Hello')
from pytilia.visorbearer import *

C = BackSequentialGDALFileRaster
fp = buzz.Footprint(
    tl=(0, 100),
    rsize=(10, 10),
    size=(1000, 1000),
)
print(fp)

buzz.Env(allow_complex_footprint=1, warnings=0).__enter__()

xs, ys = np.meshgrid(range(fp.rw), range(fp.rh))

src = (xs + ys + 0.5).astype('float32')
src = np.repeat(src, 1, axis=-1)

# src = (xs + ys).astype('uint8') * 10 + 1
# src = np.repeat(src[..., None], 3, axis=2)

src = np.squeeze(src)
print(src.shape)




tests = [
    # ('same,samegrid', fp),
    # ('within,samegrid', fp.erode(3)),
    # ('overlap,samegrid', fp.move((-30, 130))),
    # ('exterior,samegrid', fp.move((-130, 100))),
    # ('within,shiftgrid', fp.erode(3).move((5, 95))),
    ('full,shiftgrid', (fp).dilate(2).move(fp.tl + fp.pxvec * [0.5, 0])),
    ('full,shiftgrid', (fp).dilate(2).move(fp.tl + fp.pxvec * [0, 0.5])),
    ('full,shiftgrid', (fp).dilate(2).move(fp.tl + fp.pxvec * [0.5, 0.5])),
    # ('rot', fp.intersection(fp, rotation=25)),
]
# pprint(tests)

imgs = []
subtitles = []
extents = []

imgs.append(src)
subtitles.append('src')
extents.append(fp.extent)

for s, dstfp in tests:
    print(s, dstfp)

    a = C._remap(
        fp, dstfp,
        src, None,
        None, 42,
        # None, 42,
        'erode', 'cv_area',
        # 'erode', 'cv_linear',
        # 'erode', 'cv_cubic',
    )
    print(a.shape)
    print()
    imgs.append(a)
    subtitles.append(s)
    extents.append(dstfp.extent)

for s, dstfp in tests:
    print(s, dstfp)

    a = C._remap(
        fp, dstfp,
        src, None,
        # None, 42,
        999, 42,
        'erode', 'cv_area',
    )
    print(a.shape)
    print()
    imgs.append(a)
    subtitles.append(s)
    extents.append(dstfp.extent)


show_many_images(
    imgs=imgs,
    subtitles=subtitles,
    extents=extents,
)
print('Bye')
