"""Test raster recipes"""

# pylint: disable=redefined-outer-name

from __future__ import division, print_function

import numpy as np
import pytest

import buzzard as buzz
from buzzard.test import make_tile_set
from .tools import SRS

@pytest.fixture(scope='module')
def fps():
    """
    See make_tile_set
    A B C
    D E F
    G H I
    """
    return make_tile_set.make_tile_set(3, [0.1, -0.1])

def test_basic(fps):
    ds = buzz.DataSource()

    ones = ds.create_araster('', fps.AI, 'float32', 1, driver='MEM')
    ones.fill(1)

    def pxfun(fp):
        return ones.get_data(fp=fp) * 2

    # araster
    twos = ds.create_recipe_araster(pxfun, fps.AI, 'float32')
    assert ones.fp == twos.fp
    assert ones.dtype == twos.dtype
    for fp in fps.values():
        assert (twos.get_data(fp=fp) == 2).all()
    twos.close()

    # raster
    twos = ds.create_recipe_raster('hello', pxfun, fps.AI, 'float32')
    assert ones.fp == twos.fp
    assert ones.dtype == twos.dtype
    for fp in fps.values():
        assert (twos.get_data(fp=fp) == 2).all()
    twos.close()

    ones.close()

def test_reproj():
    sr0 = SRS[0]
    sr1 = SRS[3]

    with buzz.Env(significant=8, allow_complex_footprint=1, warnings=0):

        # Create `twos`, a recipe from `sr1` to `sr0`
        # `fp` in sr0
        # `ds.wkt` in sr0
        # `twos.fp` in sr0
        # `twos.fp_origin` in sr1
        # `pxfun@fp` in sr1
        fp = buzz.Footprint(
            tl=(sr0['cx'], sr0['cy']),
            size=(2, 2),
            rsize=(20, 20),
        )
        ds = buzz.DataSource(sr0['wkt'])
        def pxfun(fp):
            assert (twos.fp_origin & fp) == fp, 'fp should be aligned and within twos.fp'
            return ones.get_data(fp=fp) * 2
        twos = ds.create_recipe_araster(pxfun, fp, 'float32', sr=sr1['wkt'], band_schema={'nodata': 42})

        # Create `ones`, a raster from `sr1` to `sr1`
        # `ds2.wkt` in sr1
        # `ones.fp` in sr1
        # `ones.fp_origin` in sr1
        ds2 = buzz.DataSource(sr1['wkt'])
        ones = ds2.create_araster('', twos.fp_origin, 'float32', 1, driver='MEM', sr=sr1['wkt'], band_schema={'nodata': 42})
        ones.fill(1)

        # Test that `sr1` performs reprojection of `fp` before sending it to `pxfun`
        assert ones.fp == ones.fp_origin, 'ones has no reproj'
        assert twos.fp_origin == ones.fp_origin
        assert ones.dtype == twos.dtype
        tiles = fp.tile_count(3, 3, boundary_effect='shrink').flatten().tolist() + [fp]
        for tile in tiles:
            assert (twos.get_data(fp=tile) == 2).all()

        twos.close()
        ones.close()
