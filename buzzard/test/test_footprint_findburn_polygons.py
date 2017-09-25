
# pylint: disable=redefined-outer-name

import numpy as np
import numpy.random as npr
import pytest
import shapely.geometry as sg

import buzzard as buzz

_RANDOM_ORDERING_COUNT = 50

_GRID = """---------------------
---------a-----------
--000000000000000----
--0f--b--------g0----
--0-11111-22222-03---
--0-1---1-2-i-2-0-44-
-90-1-j-1-2-k-2-0-44-
--0-1l--1-2---2c0----
--0-11111d22222-05---
--0e-----------h0----
--000000000000000mmm-
--nnnn--88--6----m-m-
--n--n--88---7---mmm-
--n--n--88-----------
--nnnn------oooo-----
------------o--o-----
------------oooo-----
---------------------"""

def _dump_mask(mask):
    print('\n'.join(
        ' '.join(
            'X' if val else '-'
            for val in line
        )
        for line in mask
    ))

@pytest.fixture(scope='module')
def chars_grid():
    return np.asarray([list(line) for line in _GRID.split('\n')])

@pytest.fixture(scope='module')
def geometries(chars_grid):
    print(chars_grid)
    chars_set = {c for c in _GRID if c not in ['\n', '-']}

    def _foorprint_of_char(c):
        ys, xs = np.where(chars_grid == c)
        minx = xs.min()
        maxx = xs.max()
        miny = ys.min()
        maxy = ys.max()
        rsize = maxx - minx + 1, maxy - miny + 1
        fp = buzz.Footprint(tl=(minx, -miny), rsize=rsize, size=rsize)
        return fp

    def _geometry_of_footprint(fp):
        if fp.rarea != fp.rlength:
            return fp.poly - fp.erode(1).poly
        else:
            return fp.poly

    return list(map(_geometry_of_footprint, map(_foorprint_of_char, chars_set)))

@pytest.fixture(scope='module')
def truth(chars_grid):
    return chars_grid != '-'

@pytest.fixture(scope='module')
def fullfp(truth):
    rsize = np.flipud(truth.shape)
    return buzz.Footprint(tl=(0, 0), rsize=rsize, size=rsize)

def seeds():
    rng = npr.RandomState(42)
    return list(rng.randint(0, 10000, _RANDOM_ORDERING_COUNT))

@pytest.fixture(params=seeds())
def shuffled_geometries(request, geometries):
    seed = request.param
    geometries = list(geometries)
    npr.RandomState(seed).shuffle(geometries)
    return geometries

def test_burn(fullfp, shuffled_geometries, truth):
    geoms = shuffled_geometries
    res = fullfp.burn_polygons(geoms)
    if (res != truth).any():
        print('REF **************************************************')
        _dump_mask(truth)
        print('RES **************************************************')
        _dump_mask(res)
        assert False

def test_find(fullfp, geometries, truth):
    multipoly_ref = sg.MultiPolygon(geometries)
    geometries_test = fullfp.find_polygons(truth)
    multipoly_test = sg.MultiPolygon(geometries_test)
    assert (multipoly_ref ^ multipoly_test).is_empty
