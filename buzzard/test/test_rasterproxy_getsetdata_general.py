"""
Tests for RasterProxy.get_data() function
Only testing the `fp` parameter

Generating a tif of size=(10, 10), rsize=(10, 10), tl=(0, 10), bl=(0, 0), reso=(1, -1),
with a nodata border of size 2,
and values equals to x+y coordinates.
N N N N N N N N N N
N N N N N N N N N N
N N 4 5 6 7 8 9 N N
N N 5 6 7 8 910 N N
N N 6 7 8 91011 N N
N N 7 8 9101112 N N
N N 8 910111213 N N
N N 91011121314 N N
N N N N N N N N N N
N N N N N N N N N N
The values of this raster means that for all resolutions,
adjacent difference in both x and y is ~= equal
to queryfp.resox (TOL constant defines the maximum drift of differences)

Testing a massing amount of combinations:
- At different zones of interest:
  - Far out of tif
  - In tif, in nodata
  - In tif, in data
- Of different sizes:
  - Sufficently small to fit between all zones
  - Sufficently big to cover all combinations of zones
- Different resolutions:
  - Equal to tif resolution
  - Multiple of tif resolution
  - Submultiple of tif resolution
  - Lesser than tif resolution and not submultiple
  - Higher than tif resolution and not multiple
"""

# pylint: disable=redefined-outer-name

from __future__ import division, print_function
import logging
import collections
import itertools
import tempfile
import os
import uuid

import numpy as np
import pytest

from buzzard import Footprint, DataSource
from buzzard.test import tools

LOGGER = logging.getLogger('buzzard')

# CONSTANTS - TEST ********************************************************** **
READ_TOL = 0.032 # Tolerance in adjacent differences differences
WRITE_TOL = 0.7 # Tolerance in adjacent differences differences
PRECISION = 6

# CONSTANTS - TIF GENERATION ************************************************ **
TIF_NODATA = -99
TIF_SIDE = 10
TIF_NODATA_BORDER_SIZE = (2, 0, 1, 0)

TIF_FP = (
    Footprint(tl=(100 + 0, 100 + TIF_SIDE), size=(TIF_SIDE, TIF_SIDE), rsize=(TIF_SIDE, TIF_SIDE))
)
TIF_DATA_START_LEFT = np.around(TIF_FP.lx + TIF_NODATA_BORDER_SIZE[0] * TIF_FP.pxvec[0], PRECISION)
TIF_DATA_LAST_RIGHT = (
    np.around(TIF_FP.rx - (TIF_NODATA_BORDER_SIZE[1] + 1.) * TIF_FP.pxvec[0], PRECISION)
)
TIF_DATA_START_TOP = (
    np.around(TIF_FP.ty + TIF_NODATA_BORDER_SIZE[2] * TIF_FP.pxvec[1], PRECISION)
)
TIF_DATA_LAST_BOTTOM = (
    np.around(TIF_FP.by - (TIF_NODATA_BORDER_SIZE[3] + 1.) * TIF_FP.pxvec[1], PRECISION)
)


# CONSTANTS - CORNERS DEFINITION ******************************************** **
Corner = collections.namedtuple('Corner', ['name', 'x', 'y', 'offset_sign'])
TL_CORNER = Corner('tl', TIF_FP.tlx, TIF_FP.tly, np.array([1., -1.]))
ALL_CORNERS = [
    TL_CORNER,
    Corner('bl', TIF_FP.blx, TIF_FP.bly, np.array([1., 1.])),
    Corner('br', TIF_FP.brx, TIF_FP.bry, np.array([-1., 1.])),
    Corner('tr', TIF_FP.trx, TIF_FP.try_, np.array([-1., -1.])),
]

# CONSTANTS - SCENARIOS ***************************************************** **
# All tested sizes should be multiples of all resolutions tested ************ **
DATA_FULL_LOADING_RESOLUTIONS = {
    'size': [12],
    'reso': [3/1, 3/2, 3/3, 3/4, 3/5, 3/6, 3/7, 3/8, 3/9, 3/10],
    'corner': [TL_CORNER],
    'offset': [0., 0.5, np.sqrt(2), 2., np.sqrt(2) + 1],
}

ZONES_EDGES_TESTS = {
    'size': [1, 9, 10, 14],
    'reso': [1.],
    'corner': ALL_CORNERS,
    'offset': [-15, -4, -0.5, 0., 1.5, 2., 2.5],
}

SCENARIOS = [DATA_FULL_LOADING_RESOLUTIONS, ZONES_EDGES_TESTS]


# FIXTURES - INTERACTION WITH DATASOURCE *************************************** **
@pytest.fixture(scope='module')
def gb():
    """Fixture for the datasource creation"""
    path = '{}/{}.tif'.format(tempfile.gettempdir(), uuid.uuid4())
    tools.make_tif2(path=path, nodata=TIF_NODATA,
                    nodata_border_size=TIF_NODATA_BORDER_SIZE, reso=TIF_FP.scale,
                    rsize=TIF_FP.rsize, tl=TIF_FP.tl)
    gb = DataSource(allow_interpolation=True)
    gb.open_raster('dsm', path)

    path2 = '{}/{}.tif'.format(tempfile.gettempdir(), uuid.uuid4())
    gb.create_raster(
        'out', path2, TIF_FP, 'float32', 1, {'nodata': TIF_NODATA}, sr=gb.dsm.wkt_origin
    )
    yield gb
    gb.dsm.close()
    os.remove(path)
    gb.out.close()
    os.remove(path2)


@pytest.fixture()
def querydat(gb, queryfp):
    dat = gb.dsm.get_data(fp=queryfp)
    return dat


# FIXTURES - TESTS EMISSION ************************************************* **
def pytest_generate_tests(metafunc):
    values = []
    for scenar in SCENARIOS:
        it = itertools.product(
            scenar['size'], scenar['size'], scenar['reso'], scenar['corner'],
            scenar['offset'], scenar['offset']
        )
        values += it
    metafunc.parametrize(
        argnames='sizex,sizey,reso,corner,offsetx,offsety',
        argvalues=values
    )


# FIXTURES - QUERY FOOTPRINT CREATION *************************************** **
@pytest.fixture()
def tl(offsetx, offsety, corner):
    offset = corner.offset_sign * [offsetx, offsety]
    return offset + [corner.x, corner.y]


@pytest.fixture()
def size(sizex, sizey):
    return np.array([sizex, sizey], dtype='float32')


@pytest.fixture()
def queryfp(tl, size, reso):
    rsize = (size / reso).astype(int)
    assert (size == rsize * reso).all()
    fp = Footprint(tl=tl, size=size, rsize=np.abs(rsize))
    return fp


# FIXTURES - TIF_NODATA INDICES CALCULATION ********************************* **
@pytest.fixture()
def classify_horizontal(queryfp):
    nodat_left = 0
    dat_mid = 0
    nodat_right = 0
    for i in range(queryfp.rsizex):
        x = np.around(queryfp.lx + queryfp.pxvec[0] * i, PRECISION)
        if x < TIF_DATA_START_LEFT:
            nodat_left += 1
        elif x <= TIF_DATA_LAST_RIGHT:
            dat_mid += 1
        else:
            nodat_right += 1
    return nodat_left, dat_mid, nodat_right


@pytest.fixture()
def nd_left(classify_horizontal):
    return classify_horizontal[0]


@pytest.fixture()
def nd_right(classify_horizontal):
    return classify_horizontal[2]


@pytest.fixture()
def classify_vertical(queryfp):
    nodat_top = 0
    dat_mid = 0
    nodat_bottom = 0
    for i in range(queryfp.rsizey):
        y = np.around(queryfp.ty + queryfp.pxvec[1] * i, PRECISION)
        if y > TIF_DATA_START_TOP:
            nodat_top += 1
        elif y >= TIF_DATA_LAST_BOTTOM:
            dat_mid += 1
        else:
            nodat_bottom += 1
    return nodat_top, dat_mid, nodat_bottom


@pytest.fixture()
def nd_top(classify_vertical):
    return classify_vertical[0]


@pytest.fixture()
def nd_bottom(classify_vertical):
    return classify_vertical[2]


@pytest.fixture()
def index_left(nd_left):
    return nd_left


@pytest.fixture()
def index_right(nd_right, queryfp):
    return queryfp.rsizex - nd_right


@pytest.fixture()
def index_top(nd_top):
    return nd_top


@pytest.fixture()
def index_bottom(nd_bottom, queryfp):
    return queryfp.rsizey - nd_bottom


@pytest.fixture()
def centerfp(queryfp, index_left, index_right, index_top, index_bottom):
    if index_left < index_right and index_top < index_bottom:
        return queryfp.clip(index_left, index_top, index_right, index_bottom)
    else:
        return None


@pytest.fixture()
def ref_centerdat(queryfp, centerfp):
    if centerfp is not None:
        mx, my = queryfp.meshgrid_spatial
        mx = mx - TIF_FP.tlx
        my = TIF_FP.tly - my
        sl = centerfp.slice_in(queryfp)
        a = mx[sl] + my[sl]
        return a
    else:
        return None

@pytest.fixture()
def query_centerdat(queryfp, centerfp, querydat):
    if centerfp is not None:
        sl = centerfp.slice_in(queryfp)
        return querydat[sl]
    else:
        return None


# TESTS ********************************************************************* **

def test_getdata(querydat, ref_centerdat, query_centerdat,
                 index_left, index_right, index_top, index_bottom):
    """
    Assert nodata and ~nodata positions in queried rectangle
    Assert ~nodata values adjacent differences are all contained in TOL
    """
    border_approx = 1
    band = querydat[None:None, 0:max(0, index_left - border_approx)]
    assert (band == TIF_NODATA).all()
    band = querydat[None:None, max(0, index_right + border_approx):None]
    assert (band == TIF_NODATA).all()
    band = querydat[0:max(0, index_top - border_approx), None:None]
    assert (band == TIF_NODATA).all()
    band = querydat[max(0, index_bottom + border_approx):None, None:None]
    assert (band == TIF_NODATA).all()

    if index_left < index_right and index_top < index_bottom:
        assert not (query_centerdat == TIF_NODATA).any()
        assert (np.abs(query_centerdat - ref_centerdat) <= READ_TOL).all()


def test_setdata(gb, queryfp, centerfp, ref_centerdat):
    reset = np.full(gb.out.fp.shape, 42, dtype='float32')
    gb.out.set_data(reset)
    dat = np.full(queryfp.shape, TIF_NODATA, dtype='float32')
    if centerfp is not None:
        dat[centerfp.slice_in(queryfp)] = ref_centerdat
    gb.out.set_data(dat, fp=queryfp)
    dst_full = gb.out.get_data()
    dst = gb.out.get_data(fp=queryfp)
    if (dat != TIF_NODATA).all():
        assert (dst_full != TIF_NODATA).all()
    undertainty_border = np.ceil(1 / queryfp.pxsizex) * 2
    if centerfp is None and (queryfp.rsemiminoraxis > undertainty_border):
        assert (dst == dat).all()
    elif centerfp is not None and (centerfp.rsize > undertainty_border * 2).all():
        testfp = centerfp.erode(undertainty_border)
        sl = testfp.slice_in(queryfp)
        assert (np.abs(dst[sl] - dat[sl]) < WRITE_TOL).all()
