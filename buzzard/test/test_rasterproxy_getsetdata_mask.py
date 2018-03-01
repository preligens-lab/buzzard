"""
Tests for RasterProxy.get_data() function
Only testing the `mask` parameter

Generating a tif of size=(10, 10), rsize=(10, 10), tl=(0, 10), bl=(0, 0), reso=(1, -1),
and values equals to x+y coordinates.
0  1  2  3  4  5  6  7  8  9
1  2  3  4  5  6  7  8  9 10
2  3  4  5  6  7  8  9 10 11
3  4  5  6  7  8  9 10 11 12
4  5  6  7  8  9 10 11 12 13
5  6  7  8  9 10 11 12 13 14
6  7  8  9 10 11 12 13 14 15
7  8  9 10 11 12 13 14 15 16
8  9 10 11 12 13 14 15 16 17
9 10 11 12 13 14 15 16 17 18
The values of this raster means that adjacent difference
in both x and y is ~= equal to footprint resolution.
"""

import pytest
import numpy.testing

import tempfile
import os
import uuid
import numpy as np

from buzzard import Footprint, DataSource
from buzzard.test import tools


# CONSTANTS - TIF GENERATION ************************************************ **
TIF_NODATA = -99
TIF_SIDE = 10
TIF_NODATA_BORDER_SIZE = (0, 0, 0, 0)

TIF_FP = (
    Footprint(tl=(100 + 0, 100 + TIF_SIDE), size=(TIF_SIDE, TIF_SIDE), rsize=(TIF_SIDE, TIF_SIDE))
)


@pytest.fixture
def ds():
    """Fixture for the datasource creation"""
    path = '{}/{}.tif'.format(tempfile.gettempdir(), uuid.uuid4())
    tools.make_tif2(path=path, nodata=TIF_NODATA,
                    nodata_border_size=TIF_NODATA_BORDER_SIZE, reso=TIF_FP.scale,
                    rsize=TIF_FP.rsize, tl=TIF_FP.tl)
    ds = DataSource()
    ds.open_raster('dsm', path)

    path2 = '{}/{}.tif'.format(tempfile.gettempdir(), uuid.uuid4())
    ds.create_raster(
        'out', path2, TIF_FP, 'float32', 1, {'nodata': TIF_NODATA}, sr=ds.dsm.wkt_origin
    )
    yield ds
    ds.dsm.close()
    os.remove(path)
    ds.out.close()
    os.remove(path2)


def test_getdata_without_mask(ds):
    """If the mask is None, the output contains all raster values"""
    dat = ds.dsm.get_data(mask=None)

    x, y = np.meshgrid(np.arange(10), np.arange(10))
    res = x + y

    numpy.testing.assert_array_equal(dat, res)


def test_getdata_with_mask(ds):
    """If a mask is passed, the output contains only raster values where the mask isn't null"""
    mask = np.ones((10, 10))
    mask[:2, 2:6] = 0
    dat = ds.dsm.get_data(mask=mask)

    x, y = np.meshgrid(np.arange(10), np.arange(10))
    res = x + y
    res[mask == 0] = TIF_NODATA

    numpy.testing.assert_array_equal(dat, res)
