# pylint: disable=redefined-outer-name, unused-argument

import itertools
import operator

import numpy as np
import pytest

import buzzard as buzz
from buzzard.test.tools import assert_tiles_eq
from buzzard.test import make_tile_set

def is_invalid(tup):
    w, h, pox, poy = tup
    return (
        pox == 0 or poy == 0 or w == 0 or h == 0 or
        (pox > w) or
        (poy > h) or
        (w % pox != 0) or
        (h % poy != 0)
    )

PARAMS2 = ['br', 'tr', 'tl', 'bl']
COMBOS = {  # len = 1600
    (w, h, pox, poy)
    for w, h, pox, poy in itertools.product(range(4), range(4), range(10), range(10))
}
FAIL_COMBOS = {  # len = 1575
    tup for tup in COMBOS if is_invalid(tup)
}
VALID_COMBOS = COMBOS - FAIL_COMBOS  # len = 25

RANDOM_SIZES = [796, 795]
RANDOM_OCCURRENCES = [2, 3, 4]
RANDOM_SRC_SIZE = [2000, 794]
RANDOM_COMBOS = {
    tup
    for tup in itertools.product(
        RANDOM_SRC_SIZE, RANDOM_SRC_SIZE,
        RANDOM_SIZES, RANDOM_SIZES,
        RANDOM_OCCURRENCES, RANDOM_OCCURRENCES,
        PARAMS2,
    )
}
RANDOM_FAIL_COMBOS = {
    tup for tup in RANDOM_COMBOS if is_invalid(tup[2:6])
}
RANDOM_VALID_COMBOS = RANDOM_COMBOS - RANDOM_FAIL_COMBOS

# *************************************************************************** **
# FIXTURES ****************************************************************** **
# *************************************************************************** **

@pytest.fixture(scope='module')
def fps():
    """
    See make_tile_set
    A B C D E F G
    H I J K L M N
    O P Q.R.S T U
    V W X.Y.Z a b
    c d e.f.g h i
    j k l m n o p
    q r s t u v w
    """
    return make_tile_set.make_tile_set(7, [1, -1], [1, -1])


def pytest_generate_tests(metafunc):
    if metafunc.function == test_fail:
        metafunc.parametrize(
            argnames='w, h, pox, poy',
            argvalues=FAIL_COMBOS
        )
    if metafunc.function == test_success:
        metafunc.parametrize(
            argnames='w, h, pox, poy',
            argvalues=VALID_COMBOS,
        )
    if metafunc.function == test_random_fail:
        metafunc.parametrize(
            argnames='srcw, srch, w, h, pox, poy, boundary_effect_locus',
            argvalues=RANDOM_FAIL_COMBOS,
        )
    if metafunc.function == test_random_success:
        metafunc.parametrize(
            argnames='srcw, srch, w, h, pox, poy, boundary_effect_locus',
            argvalues=RANDOM_VALID_COMBOS,
        )

# *************************************************************************** **
# TESTS  ******************************************************************** **
# *************************************************************************** **

def test_random_success(srcw, srch, w, h, pox, poy, boundary_effect_locus):
    fp = buzz.Footprint(tl=(0, 0), rsize=(srcw, srch), size=(srcw, srch))
    tiles = fp.tile_occurrence((w, h), pox, poy,
                               boundary_effect_locus=boundary_effect_locus)
    for f in SUCCESS_ASSERTS:
        f(fp, tiles, (w, h), pox, poy, boundary_effect_locus)

def test_random_fail(srcw, srch, w, h, pox, poy, boundary_effect_locus):
    fp = buzz.Footprint(tl=(0, 0), rsize=(srcw, srch), size=(srcw, srch))
    with pytest.raises(ValueError):
        fp.tile_occurrence((w, h), pox, poy, boundary_effect_locus=boundary_effect_locus)


def test_fail(fps, w, h, pox, poy):
    with pytest.raises(ValueError):
        fps.Qg.tile_occurrence((w, h), pox, poy)


def test_success(fps, w, h, pox, poy):
    # Tiles of area 1
    if (1, 1, 1, 1) == (w, h, pox, poy):
        truth = [
            [fps.Q, fps.R, fps.S],
            [fps.X, fps.Y, fps.Z],
            [fps.e, fps.f, fps.g],
        ]

    # Tiles of area 2
    elif (2, 1, 1, 1) == (w, h, pox, poy):
        truth = [
            [fps.QR, fps.ST],
            [fps.XY, fps.Za],
            [fps.ef, fps.gh],
        ]
    elif (2, 1, 2, 1) == (w, h, pox, poy):
        truth = [
            [fps.PQ, fps.QR, fps.RS, fps.ST],
            [fps.WX, fps.XY, fps.YZ, fps.Za],
            [fps.de, fps.ef, fps.fg, fps.gh],
        ]
    elif (1, 2, 1, 1) == (w, h, pox, poy):
        truth = [
            [fps.QX, fps.RY, fps.SZ],
            [fps.el, fps.fm, fps.gn],
        ]
    elif (1, 2, 1, 2) == (w, h, pox, poy):
        truth = [
            [fps.JQ, fps.KR, fps.LS],
            [fps.QX, fps.RY, fps.SZ],
            [fps.Xe, fps.Yf, fps.Zg],
            [fps.el, fps.fm, fps.gn],
        ]

    # Tiles of area 3
    elif (1, 3, 1, 1) == (w, h, pox, poy):
        truth = [
            [fps.Qe, fps.Rf, fps.Sg, ],
        ]
    elif (1, 3, 1, 3) == (w, h, pox, poy):
        truth = [
            [fps.CQ, fps.DR, fps.ES, ],
            [fps.JX, fps.KY, fps.LZ, ],
            [fps.Qe, fps.Rf, fps.Sg, ],
            [fps.Xl, fps.Ym, fps.Zn, ],
            [fps.es, fps.ft, fps.gu, ],
        ]
    elif (3, 1, 1, 1) == (w, h, pox, poy):
        truth = [
            [fps.QS, ],
            [fps.XZ, ],
            [fps.eg, ],
        ]
    elif (3, 1, 3, 1) == (w, h, pox, poy):
        truth = [
            [fps.OQ, fps.PR, fps.QS, fps.RT, fps.SU, ],
            [fps.VX, fps.WY, fps.XZ, fps.Ya, fps.Zb, ],
            [fps.ce, fps.df, fps.eg, fps.fh, fps.gi, ],
        ]

    # Tiles of area 4
    elif (2, 2, 1, 1) == (w, h, pox, poy):
        truth = [
            [fps.QY, fps.Sa],
            [fps.em, fps.go],
        ]
    elif (2, 2, 2, 1) == (w, h, pox, poy):
        truth = [
            [fps.PX, fps.QY, fps.RZ, fps.Sa],
            [fps.dl, fps.em, fps.fn, fps.go],
        ]
    elif (2, 2, 1, 2) == (w, h, pox, poy):
        truth = [
            [fps.JR, fps.LT],
            [fps.QY, fps.Sa],
            [fps.Xf, fps.Zh],
            [fps.em, fps.go],
        ]
    elif (2, 2, 2, 2) == (w, h, pox, poy):
        truth = [
            [fps.IQ, fps.JR, fps.KS, fps.LT],
            [fps.PX, fps.QY, fps.RZ, fps.Sa],
            [fps.We, fps.Xf, fps.Yg, fps.Zh],
            [fps.dl, fps.em, fps.fn, fps.go],
        ]

    # Tiles of area 6
    elif (2, 3, 1, 1) == (w, h, pox, poy):
        truth = [
            [fps.Qf, fps.Sh],
        ]
    elif (2, 3, 2, 1) == (w, h, pox, poy):
        truth = [
            [fps.Pe, fps.Qf, fps.Rg, fps.Sh, ],
        ]
    elif (2, 3, 1, 3) == (w, h, pox, poy):
        truth = [
            [fps.CR, fps.ET, ],
            [fps.JY, fps.La, ],
            [fps.Qf, fps.Sh, ],
            [fps.Xm, fps.Zo, ],
            [fps.et, fps.gv, ],
        ]
    elif (2, 3, 2, 3) == (w, h, pox, poy):
        truth = [
            [fps.BQ, fps.CR, fps.DS, fps.ET, ],
            [fps.IX, fps.JY, fps.KZ, fps.La, ],
            [fps.Pe, fps.Qf, fps.Rg, fps.Sh, ],
            [fps.Wl, fps.Xm, fps.Yn, fps.Zo, ],
            [fps.ds, fps.et, fps.fu, fps.gv, ],
        ]
    elif (3, 2, 1, 1) == (w, h, pox, poy):
        truth = [
            [fps.QZ],
            [fps.en],
        ]
    elif (3, 2, 1, 2) == (w, h, pox, poy):
        truth = [
            [fps.JS, ],
            [fps.QZ, ],
            [fps.Xg, ],
            [fps.en, ],
        ]
    elif (3, 2, 3, 1) == (w, h, pox, poy):
        truth = [
            [fps.OX, fps.PY, fps.QZ, fps.Ra, fps.Sb, ],
            [fps.cl, fps.dm, fps.en, fps.fo, fps.gp, ],
        ]
    elif (3, 2, 3, 2) == (w, h, pox, poy):
        truth = [
            [fps.HQ, fps.IR, fps.JS, fps.KT, fps.LU, ],
            [fps.OX, fps.PY, fps.QZ, fps.Ra, fps.Sb, ],
            [fps.Ve, fps.Wf, fps.Xg, fps.Yh, fps.Zi, ],
            [fps.cl, fps.dm, fps.en, fps.fo, fps.gp, ],
        ]

    # Tiles of area 9
    elif (3, 3, 1, 1) == (w, h, pox, poy):
        truth = [
            [fps.Qg],
        ]
    elif (3, 3, 1, 3) == (w, h, pox, poy):
        truth = [
            [fps.CS, ],
            [fps.JZ, ],
            [fps.Qg, ],
            [fps.Xn, ],
            [fps.eu, ],
        ]
    elif (3, 3, 3, 1) == (w, h, pox, poy):
        truth = [
            [fps.Oe, fps.Pf, fps.Qg, fps.Rh, fps.Si, ],
        ]
    elif (3, 3, 3, 3) == (w, h, pox, poy):
        truth = [
            [fps.AQ, fps.BR, fps.CS, fps.DT, fps.EU],
            [fps.HX, fps.IY, fps.JZ, fps.Ka, fps.Lb],
            [fps.Oe, fps.Pf, fps.Qg, fps.Rh, fps.Si],
            [fps.Vl, fps.Wm, fps.Xn, fps.Yo, fps.Zp],
            [fps.cs, fps.dt, fps.eu, fps.fv, fps.gw],
        ]

    else:
        raise Exception('Test %s not implemented' % str((w, h, pox, poy)))
    tiles = fps.Qg.tile_occurrence((w, h), pox, poy)
    assert_tiles_eq(tiles, truth)
    for f in SUCCESS_ASSERTS:
        f(fps.Qg, tiles, (w, h), pox, poy, 'tl')


def assert_property_tile_size(src, tiles, size, occx, occy, boundary_effect_locus):
    w = np.vectorize(operator.attrgetter('w'))(tiles.flatten())
    assert np.unique(w).size == 1
    h = np.vectorize(operator.attrgetter('h'))(tiles.flatten())
    assert np.unique(h).size == 1

def assert_property_pixel_coverage(src, tiles, size, occx, occy, boundary_effect_locus):
    mask = np.zeros(src.shape, dtype='int')
    tiles = tiles.flatten()
    for t in tiles:
        mask[t.slice_in(src, clip=True)] += 1
    assert (mask == occx * occy).all()

def assert_property_share_area(src, tiles, size, occx, occy, boundary_effect_locus):
    border_tiles = np.r_[tiles[-1, 1:-1], tiles[0, 1:-1], tiles[:, 0], tiles[:, -1]]
    for t in border_tiles:
        assert t.share_area(src)

def assert_property_unique(src, tiles, size, occx, occy, boundary_effect_locus):
    tls = np.vectorize(operator.attrgetter('tl'), signature='()->(2)')(tiles.flatten())
    assert np.unique(tls, axis=0).shape[0] == tiles.size


SUCCESS_ASSERTS = [
    assert_property_tile_size,
    assert_property_pixel_coverage,

    assert_property_share_area,
    assert_property_unique,
]

def test_value_error(fps):
    with pytest.raises(ValueError, match='shape'):
        fps.AI.tile_occurrence(1, 1, 1)
    with pytest.raises(ValueError, match='shape'):
        fps.AI.tile_occurrence([1, 1, 1], 1, 1)
    with pytest.raises(ValueError, match='effect'):
        fps.AI.tile_occurrence((1, 1), 1, 1, boundary_effect='')
    with pytest.raises(ValueError, match='effect_locus'):
        fps.AI.tile_occurrence((1, 1), 1, 1, boundary_effect_locus='')
