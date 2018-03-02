
# pylint: disable=redefined-outer-name

from __future__ import division, print_function
import itertools

import shapely.geometry as sg
import numpy as np
import pytest

import buzzard as buzz

LESS_ERROR = 1 / 3
MORE_ERROR = 2

FP1 = buzz.Footprint(
    tl=(672939.369686, 6876118.107215),
    size=(24.020000, 24.020000),
    rsize=(1201, 1201),
)

def pytest_generate_tests(metafunc):
    with buzz.Env(significant=15):
        metafunc.parametrize(
            argnames='fp',
            argvalues=[
                FP1,
                FP1.move((-FP1.diagvec / 2)),
                FP1.move((1e8, 1e8)),
                FP1.move((1e8, 1e8), (1e8 + 1, 1e8), (1e8 + 1, 1e8 - 1)),
            ],
        )

@pytest.fixture(params=[6, 8, 10])
def env(request):
    with buzz.Env(significant=request.param):
        yield request.param

def test_same_grid_and_move(fp, env):
    ofp = fp
    if env < ofp._significant_min + 1:
        pytest.skip()

    for factx, facty in itertools.combinations_with_replacement([-1, 0, +1], 2):
        tl = ofp.tl % ofp.pxsize + 1e9 // ofp.pxsize * ofp.pxsize * [factx, facty]
        fp = ofp.move(tl)
        if env < fp._significant_min:
            continue
        eps = np.abs(np.r_[fp.coords, ofp.coords]).max() * 10 ** -buzz.env.significant

        for factx, facty in itertools.combinations_with_replacement([-1, 0, +1], 2):
            fact = np.asarray([factx, facty])

            fp = ofp.move(tl + eps * LESS_ERROR * fact)
            assert ofp.same_grid(fp)

            if (fact != 0).any():
                fp = ofp.move(tl + eps * MORE_ERROR * fact)
                assert not ofp.same_grid(fp)

def test_intersection_and_equals_and_of_extent(fp, env):
    if env < fp._significant_min:
        pytest.skip()
    eps = np.abs(fp.coords).max() * 10 ** -buzz.env.significant
    cwr = itertools.combinations_with_replacement

    for ax, ay, bx, by in cwr([-eps * LESS_ERROR, 0, +eps * LESS_ERROR], 4):
        deltas = np.asarray([ax, ay, bx, by])

        assert fp == fp & sg.LineString([fp.tl + [ax, ay], fp.br + [bx, by]])
        assert fp == fp.of_extent(fp.extent + deltas, fp.scale)

        if (np.asarray([ax, ay, bx, by]) != 0).any():
            assert fp != fp.of_extent(fp.extent + deltas / LESS_ERROR * MORE_ERROR, fp.scale)

    for slacka, slackb in itertools.product(
            [0,
             -fp.pxvec / np.linalg.norm(fp.pxvec) * eps * MORE_ERROR,
             -fp.pxlrvec / np.linalg.norm(fp.pxlrvec) * eps * MORE_ERROR,
             -fp.pxtbvec / np.linalg.norm(fp.pxtbvec) * eps * MORE_ERROR],
            [0,
             fp.pxvec / np.linalg.norm(fp.pxvec) * eps * MORE_ERROR,
             fp.pxlrvec / np.linalg.norm(fp.pxlrvec) * eps * MORE_ERROR,
             fp.pxtbvec / np.linalg.norm(fp.pxtbvec) * eps * MORE_ERROR],
    ):
        if np.isscalar(slacka) and np.isscalar(slackb):
            continue
        assert fp != fp.dilate(2) & sg.LineString([fp.tl + slacka, fp.br + slackb])


def test_spatial_to_raster(fp, env):
    if env < fp._significant_min:
        pytest.skip()
    rng = np.random.RandomState(42)
    eps = np.abs(fp.coords).max() * 10 ** -buzz.env.significant

    xy = np.dstack(fp.meshgrid_spatial)
    rxy = np.dstack(fp.meshgrid_raster)
    res = np.equal(
        rxy, fp.spatial_to_raster(xy),
    )
    assert np.all(res)
    res = np.equal(
        rxy, fp.spatial_to_raster(xy + (rng.rand(*xy.shape) * 2 - 1) * eps * LESS_ERROR),
    )
    assert np.all(res)
    res = np.equal(
        rxy, fp.spatial_to_raster(xy + (rng.rand(*xy.shape) * 2 - 1) * eps * MORE_ERROR),
    )
    assert not np.all(res)
