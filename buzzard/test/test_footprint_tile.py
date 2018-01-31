# pylint: disable=redefined-outer-name
# pylint: disable=too-many-lines


import itertools

import pytest

from buzzard.test.tools import assert_tiles_eq
from buzzard.test import make_tile_set

ANY = 42
PARAMS1 = {
    'extend',
    'overlap',
    'exclude',
    'exception',
    'shrink',
}
PARAMS2 = {'br', 'tr', 'tl', 'bl'}
COMBOS = {  # len = 625
    (w, h, ow, oh)
    for w, h, ow, oh in itertools.product(range(5), range(5), range(5), range(5))
}
FAIL_COMBOS = {  # len = 525
    (w, h, ow, oh)
    for w, h, ow, oh in COMBOS
    if w == 0 or h == 0
    or ow >= w or oh >= h
}
VALID_COMBOS = COMBOS - FAIL_COMBOS  # len = 100
FIT_XY_COMBOS = {  # len = 25
    (w, h, ow, oh)
    for w, h, ow, oh in VALID_COMBOS
    if ((w == 3) or (w == 2 and ow == 1) or (w == 1)) and
    ((h == 3) or (h == 2 and oh == 1) or (h == 1))
}
NOFIT_XY_COMBOS = VALID_COMBOS - FIT_XY_COMBOS  # len = 75
EXTRA_COMBO = [
    list(coords) + [be, bel]
    for (coords, be, bel) in itertools.product(
        [(2, 2, 0, 1)],
        PARAMS1 - {'exception'},
        PARAMS2 - {'br'},
    )
]

# *************************************************************************** **
# FIXTURES ****************************************************************** **
# *************************************************************************** **

@pytest.fixture(scope='module')
def fps():
    """
    See make_tile_set
    A B C D E
    F G H I J
    K L M N O
    P Q R S T
    U V W X Y
    """
    return make_tile_set.make_tile_set(5, [1, -1], [1, -1])


def pytest_generate_tests(metafunc):
    """
    Testing all 625 combinations of parameters for a 3x3 footprint and up to 4x4 tile
    - Assert that exceptions are raised
    - Assert that return values are valid
    """
    if metafunc.function == test_fail:
        metafunc.parametrize(
            argnames='w, h, ow, oh',
            argvalues=FAIL_COMBOS,
        )
    if metafunc.function == test_fit_xy:
        metafunc.parametrize(
            argnames='w, h, ow, oh',
            argvalues=FIT_XY_COMBOS,
        )
    if metafunc.function in [
            test_nofit_xy_br_extend,
            test_nofit_xy_br_overlap,
            test_nofit_xy_br_exclude,
            test_nofit_xy_br_shrink,
            test_nofit_xy_exception,
    ]:
        metafunc.parametrize(
            argnames='w, h, ow, oh',
            argvalues=NOFIT_XY_COMBOS,
        )


@pytest.fixture(params=PARAMS2)
def boundary_effect_locus(request):
    return request.param


@pytest.fixture(params=PARAMS1)
def boundary_effect(request):
    return request.param

# *************************************************************************** **
# TESTS  ******************************************************************** **
# *************************************************************************** **

def test_fail(fps, w, h, ow, oh):
    with pytest.raises(ValueError):
        fps.GS.tile((w, h), ow, oh, boundary_effect='extend')


def test_nofit_xy_exception(fps, w, h, ow, oh, boundary_effect_locus):
    with pytest.raises(ValueError, match='There is a gap'): # TODO MOVE!!
        fps.GS.tile(
            (w, h), ow, oh,
            boundary_effect='exception', boundary_effect_locus=boundary_effect_locus
        )


def test_fit_xy(fps, w, h, ow, oh, boundary_effect, boundary_effect_locus):
    """
    Compares tiling versus truth that is manually inputed
    Handles combinations of parameters where all tiles fit inside origin
    """
    if (1, 1, 0, 0) == (w, h, ow, oh):
        truth = [
            [fps.G, fps.H, fps.I, ],
            [fps.L, fps.M, fps.N, ],
            [fps.Q, fps.R, fps.S, ],
        ]
    elif (1, 2, 0, 1) == (w, h, ow, oh):
        truth = [
            [fps.GL, fps.HM, fps.IN],
            [fps.LQ, fps.MR, fps.NS],
        ]
    elif (1, 3, 0, ANY) == (w, h, ow, ANY):
        truth = [
            [fps.GQ, fps.HR, fps.IS, ],
        ]
    elif (2, 1, 1, 0) == (w, h, ow, oh):
        truth = [
            [fps.GH, fps.HI],
            [fps.LM, fps.MN],
            [fps.QR, fps.RS],
        ]
    elif (2, 2, 1, 1) == (w, h, ow, oh):
        truth = [
            [fps.GM, fps.HN],
            [fps.LR, fps.MS],
        ]
    elif (2, 3, 1, ANY) == (w, h, ow, ANY):
        truth = [
            [fps.GR, fps.HS],
        ]
    elif (3, 1, ANY, 0) == (w, h, ANY, oh):
        truth = [
            [fps.GI, ],
            [fps.LN, ],
            [fps.QS, ],
        ]
    elif (3, 2, ANY, 1) == (w, h, ANY, oh):
        truth = [
            [fps.GN],
            [fps.LS],
        ]
    elif (3, 3, ANY, ANY) == (w, h, ANY, ANY):
        truth = [
            [fps.GS, ],
        ]
    else:
        raise Exception('Test %s not implemented' % str((w, h, ow, oh)))
    tiles = fps.GS.tile(
        (w, h), ow, oh, boundary_effect=boundary_effect, boundary_effect_locus=boundary_effect_locus
    )
    assert_tiles_eq(tiles, truth)


def test_nofit_xy_br_extend(fps, w, h, ow, oh):
    """
    Compares tiling versus truth that is manually inputed
    Handles combinations of parameters where all tiles DO NOT fit inside origin
    for 'extend' parameter
    """
    if (1, 2, 0, 0) == (w, h, ow, oh):
        truth = [
            [fps.GL, fps.HM, fps.IN, ],
            [fps.QV, fps.RW, fps.SX, ],
        ]
    elif (2, 1, 0, 0) == (w, h, ow, oh):
        truth = [
            [fps.GH, fps.IJ, ],
            [fps.LM, fps.NO, ],
            [fps.QR, fps.ST, ],
        ]
    elif (2, 2, 0, 0) == (w, h, ow, oh):
        truth = [
            [fps.GM, fps.IO, ],
            [fps.QW, fps.SY, ],
        ]
    elif (2, 2, 0, 1) == (w, h, ow, oh):
        truth = [
            [fps.GM, fps.IO],
            [fps.LR, fps.NT],
        ]
    elif (2, 2, 1, 0) == (w, h, ow, oh):
        truth = [
            [fps.GM, fps.HN],
            [fps.QW, fps.RX],
        ]
    elif (2, 3, 0, ANY) == (w, h, ow, ANY):
        truth = [
            [fps.GR, fps.IT, ],
        ]
    elif (3, 2, ANY, 0) == (w, h, ANY, oh):
        truth = [
            [fps.GN],
            [fps.QX],
        ]
    elif (4, 1, ANY, 0) == (w, h, ANY, oh):
        truth = [
            [fps.GJ],
            [fps.LO],
            [fps.QT],
        ]
    elif (4, 2, ANY, 0) == (w, h, ANY, oh):
        truth = [
            [fps.GO],
            [fps.QY],
        ]
    elif (4, 2, ANY, 1) == (w, h, ANY, oh):
        truth = [
            [fps.GO],
            [fps.LT],
        ]
    elif (4, 3, ANY, ANY) == (w, h, ANY, ANY):
        truth = [
            [fps.GT],
        ]
    elif (4, 4, ANY, ANY) == (w, h, ANY, ANY):
        truth = [
            [fps.GY],
        ]
    elif (1, 4, 0, ANY) == (w, h, ow, ANY):
        truth = [
            [fps.GV, fps.HW, fps.IX],
        ]
    elif (2, 4, 0, ANY) == (w, h, ow, ANY):
        truth = [
            [fps.GW, fps.IY],
        ]
    elif (2, 4, 1, ANY) == (w, h, ow, ANY):
        truth = [
            [fps.GW, fps.HX],
        ]
    elif (3, 4, ANY, ANY) == (w, h, ANY, ANY):
        truth = [
            [fps.GX],
        ]
    else:
        raise Exception('Test %s not implemented' % str((w, h, ow, oh)))
    tiles = fps.GS.tile((w, h), ow, oh, boundary_effect='extend')
    assert_tiles_eq(tiles, truth)


def test_nofit_xy_br_overlap(fps, w, h, ow, oh):
    """
    Compares tiling versus truth that is manually inputed
    Handles combinations of parameters where all tiles DO NOT fit inside origin
    for 'overlap' parameter
    """
    if (1, 2, 0, 0) == (w, h, ow, oh):
        truth = [
            [fps.GL, fps.HM, fps.IN, ],
            [fps.LQ, fps.MR, fps.NS, ],
        ]
    elif (2, 1, 0, 0) == (w, h, ow, oh):
        truth = [
            [fps.GH, fps.HI, ],
            [fps.LM, fps.MN, ],
            [fps.QR, fps.RS, ],
        ]
    elif (2, 2, ANY, ANY) == (w, h, ANY, ANY):
        truth = [
            [fps.GM, fps.HN, ],
            [fps.LR, fps.MS, ],
        ]
    elif (2, 3, 0, ANY) == (w, h, ow, ANY):
        truth = [
            [fps.GR, fps.HS, ],
        ]
    elif (3, 2, ANY, 0) == (w, h, ANY, oh):
        truth = [
            [fps.GN],
            [fps.LS],
        ]
    elif ((4, ANY, ANY, ANY) == (w, ANY, ANY, ANY) or
          (ANY, 4, ANY, ANY) == (ANY, h, ANY, ANY)):
        with pytest.raises(ValueError, match='overlap'):
            _ = fps.GS.tile((w, h), ow, oh, boundary_effect='overlap')
        return
    else:
        raise Exception('Test %s not implemented' % str((w, h, ow, oh)))
    tiles = fps.GS.tile((w, h), ow, oh, boundary_effect='overlap')
    assert_tiles_eq(tiles, truth)


def test_nofit_xy_br_exclude(fps, w, h, ow, oh):
    """
    Compares tiling versus truth that is manually inputed
    Handles combinations of parameters where all tiles DO NOT fit inside origin
    for 'exclude' parameter
    """
    if (1, 2, 0, 0) == (w, h, ow, oh):
        truth = [
            [fps.GL, fps.HM, fps.IN],
        ]
    elif (2, 1, 0, 0) == (w, h, ow, oh):
        truth = [
            [fps.GH, ],
            [fps.LM, ],
            [fps.QR, ],
        ]
    elif (2, 2, 0, 0) == (w, h, ow, oh):
        truth = [
            [fps.GM, ],
        ]
    elif (2, 2, 0, 1) == (w, h, ow, oh):
        truth = [
            [fps.GM, ],
            [fps.LR, ],
        ]
    elif (2, 2, 1, 0) == (w, h, ow, oh):
        truth = [
            [fps.GM, fps.HN],
        ]
    elif (2, 3, 0, ANY) == (w, h, ow, ANY):
        truth = [
            [fps.GR, ],
        ]
    elif (3, 2, ANY, 0) == (w, h, ANY, oh):
        truth = [
            [fps.GN],
        ]
    elif (4, ANY, ANY, ANY) == (w, ANY, ANY, ANY):
        truth = []
    elif (ANY, 4, ANY, ANY) == (ANY, h, ANY, ANY):
        truth = []
    else:
        raise Exception('Test %s not implemented' % str((w, h, ow, oh)))
    tiles = fps.GS.tile((w, h), ow, oh, boundary_effect='exclude')
    assert_tiles_eq(tiles, truth)


def test_nofit_xy_br_shrink(fps, w, h, ow, oh):
    """
    Compares tiling versus truth that is manually inputed
    Handles combinations of parameters where all tiles DO NOT fit inside origin
    for 'shrink' parameter
    """
    if (1, 2, 0, 0) == (w, h, ow, oh):
        truth = [
            [fps.GL, fps.HM, fps.IN, ],
            [fps.Q, fps.R, fps.S, ],
        ]
    elif (2, 1, 0, 0) == (w, h, ow, oh):
        truth = [
            [fps.GH, fps.I, ],
            [fps.LM, fps.N, ],
            [fps.QR, fps.S, ],
        ]
    elif (2, 2, 0, 0) == (w, h, ow, oh):
        truth = [
            [fps.GM, fps.IN, ],
            [fps.QR, fps.S, ],
        ]
    elif (2, 2, 0, 1) == (w, h, ow, oh):
        truth = [
            [fps.GM, fps.IN],
            [fps.LR, fps.NS],
        ]
    elif (2, 2, 1, 0) == (w, h, ow, oh):
        truth = [
            [fps.GM, fps.HN],
            [fps.QR, fps.RS],
        ]
    elif ((2, 3, 0, ANY) == (w, h, ow, ANY) or
          (2, 4, 0, ANY) == (w, h, ow, ANY)):
        truth = [
            [fps.GR, fps.IS, ],
        ]
    elif ((3, 2, ANY, 0) == (w, h, ANY, oh) or
          (4, 2, ANY, 0) == (w, h, ANY, oh)):
        truth = [
            [fps.GN],
            [fps.QS],
        ]
    elif ((3, 4, ANY, ANY) == (w, h, ANY, ANY) or
          (4, 3, ANY, ANY) == (w, h, ANY, ANY) or
          (4, 4, ANY, ANY) == (w, h, ANY, ANY)):
        truth = [
            [fps.GS],
        ]
    elif (1, 4, 0, ANY) == (w, h, ow, ANY):
        truth = [
            [fps.GQ, fps.HR, fps.IS],
        ]
    elif (4, 1, ANY, 0) == (w, h, ANY, oh):
        truth = [
            [fps.GI],
            [fps.LN],
            [fps.QS],
        ]
    elif (4, 2, ANY, 1) == (w, h, ANY, oh):
        truth = [
            [fps.GN],
            [fps.LS],
        ]
    elif (2, 4, 1, ANY) == (w, h, ow, ANY):
        truth = [
            [fps.GR, fps.HS],
        ]
    else:
        raise Exception('Test %s not implemented' % str((w, h, ow, oh)))
    tiles = fps.GS.tile((w, h), ow, oh, boundary_effect='shrink')
    assert_tiles_eq(tiles, truth)


@pytest.mark.parametrize(
    "w, h, ow, oh, boundary_effect, boundary_effect_locus", EXTRA_COMBO
)
def test_extra(fps, w, h, ow, oh, boundary_effect, boundary_effect_locus):
    if (2, 2, 0, 1) == (w, h, ow, oh):
        if boundary_effect_locus == 'tr':
            if boundary_effect == 'extend':
                truth = [
                    [fps.GM, fps.IO],
                    [fps.LR, fps.NT],
                ]
            elif boundary_effect == 'overlap':
                truth = [
                    [fps.GM, fps.HN],
                    [fps.LR, fps.MS],
                ]
            elif boundary_effect == 'exclude':
                truth = [
                    [fps.GM],
                    [fps.LR],
                ]
            elif boundary_effect == 'shrink':
                truth = [
                    [fps.GM, fps.IN],
                    [fps.LR, fps.NS],
                ]
            else:
                assert False
        elif boundary_effect_locus == 'tl' or boundary_effect_locus == 'bl':
            if boundary_effect == 'extend':
                truth = [
                    [fps.FL, fps.HN],
                    [fps.KQ, fps.MS],
                ]
            elif boundary_effect == 'overlap':
                truth = [
                    [fps.GM, fps.HN],
                    [fps.LR, fps.MS],
                ]
            elif boundary_effect == 'exclude':
                truth = [
                    [fps.HN],
                    [fps.MS],
                ]
            elif boundary_effect == 'shrink':
                truth = [
                    [fps.GL, fps.HN],
                    [fps.LQ, fps.MS],
                ]
            else:
                assert False
        else:
            assert False
    tiles = fps.GS.tile(
        (w, h), ow, oh,
        boundary_effect=boundary_effect, boundary_effect_locus=boundary_effect_locus
    )
    assert_tiles_eq(tiles, truth)

def test_value_error(fps):
    with pytest.raises(ValueError, match='shape'):
        fps.AI.tile(1)
    with pytest.raises(ValueError, match='shape'):
        fps.AI.tile([1, 1, 1])
    with pytest.raises(ValueError, match='effect'):
        fps.AI.tile((1, 1), boundary_effect='')
    with pytest.raises(ValueError, match='effect_locus'):
        fps.AI.tile((1, 1), boundary_effect_locus='')
