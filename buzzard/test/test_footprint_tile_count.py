# pylint: disable=redefined-outer-name, unused-argument

import itertools
import operator

import numpy as np
import pytest

from buzzard.test import make_tile_set

PARAMS1 = [
    'extend',
    'overlap',
    'exclude',
    'exception',
    'shrink',
]
PARAMS2 = ['br', 'tr', 'tl', 'bl']
COMBOS = {
    (tx, ty, ox, oy, be)
    for (tx, ty, ox, oy, be) in itertools.product(
        range(4), range(4), range(4), range(4), PARAMS1)
}
FAIL_COMBOS = {
    (tx, ty, ox, oy, be)
    for (tx, ty, ox, oy, be) in COMBOS
    if tx == 0 or ty == 0
    or tx >= 4 or ty >= 4
    or ox >= 3 or oy >= 3
    or (ox == 2 and tx != 1)
    or (oy == 2 and ty != 1)
    or (ox == 1 and tx >= 3)
    or (oy == 1 and ty >= 3)
    or (be == 'exception' and (
        (tx == 2 and ox != 1) or
        (ty == 2 and oy != 1)
    ))
}
VALID_COMBOS = COMBOS - FAIL_COMBOS

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


@pytest.fixture(params=PARAMS2)
def boundary_effect_locus(request):
    return request.param


def pytest_generate_tests(metafunc):
    """
    Implementation glossary
    -------
    tx: tile per row
    ty: tile per column
    ox: overlap horizontal
    oy: overlap vertical
    """
    if metafunc.function == test_fail:
        metafunc.parametrize(
            argnames='tx, ty, ox, oy, be',
            argvalues=FAIL_COMBOS,
        )
    if metafunc.function == test_success:
        metafunc.parametrize(
            argnames='tx, ty, ox, oy, be',
            argvalues=VALID_COMBOS,
        )

# *************************************************************************** **
# TESTS  ******************************************************************** **
# *************************************************************************** **

def test_fail(fps, tx, ty, ox, oy, be, boundary_effect_locus):
    with pytest.raises(ValueError):
        fps.GS.tile_count(
            tx, ty, ox, oy,
            boundary_effect=be, boundary_effect_locus=boundary_effect_locus
        )


def test_success(fps, tx, ty, ox, oy, be, boundary_effect_locus):
    tiles = fps.GS.tile_count(
        tx, ty, ox, oy,
        boundary_effect=be, boundary_effect_locus=boundary_effect_locus
    )
    for assert_prop in _BOUNDARY_EFFECT_PROPERTIES[be]:
        assert_prop(fps, tiles, tx, ty, ox, oy, boundary_effect_locus)


def assert_property_tile_size(fps, tiles, tx, ty, ox, oy, param2):
    w = np.vectorize(operator.attrgetter('w'))(tiles.flatten())
    assert np.unique(w).size == 1
    h = np.vectorize(operator.attrgetter('h'))(tiles.flatten())
    assert np.unique(h).size == 1


def assert_property_overlap(fps, tiles, tx, ty, ox, oy, param2):
    if param2 == 'br':
        stride = tiles[0, 0].rsize - (ox, oy)
        refptx, refpty = 'lx', 'ty'
    elif param2 == 'tl':
        stride = tiles[-1, -1].rsize - (ox, oy)
        refptx, refpty = 'rx', 'by'
    elif param2 == 'tr':
        stride = tiles[-1, 0].rsize - (ox, oy)
        refptx, refpty = 'lx', 'by'
    elif param2 == 'bl':
        stride = tiles[0, -1].rsize - (ox, oy)
        refptx, refpty = 'rx', 'ty'
    else:
        assert False
    refptx = operator.attrgetter(refptx)
    refpty = operator.attrgetter(refpty)
    if tiles.shape[0] > 1:
        diffs_vert = np.vectorize(
            lambda a, b: abs(refpty(a) - refpty(b)))(
                tiles[0:-1], tiles[1:])
        assert (diffs_vert == stride[1]).all()
    if tiles.shape[1] > 1:
        diffs_horiz = np.vectorize(
            lambda a, b: abs(refptx(a) - refptx(b)))(
                tiles[:, 0:-1], tiles[:, 1:])
        assert (diffs_horiz == stride[0]).all()


def assert_property_global_bounds(fps, tiles, tx, ty, ox, oy, param2):
    for t in tiles.flatten():
        assert t.poly.within(fps.GS.poly)


def assert_property_share_area(fps, tiles, tx, ty, ox, oy, param2):
    border_tiles = np.r_[tiles[-1, 1:-1], tiles[0, 1:-1], tiles[:, 0], tiles[:, -1]]
    for t in border_tiles:
        assert t.share_area(fps.GS)


def assert_property_full_pixel_coverage(fps, tiles, tx, ty, ox, oy, param2):
    mask = np.zeros(fps.GS.shape, dtype='int')
    for t in tiles.flatten():
        mask[t.slice_in(fps.GS, clip=True)] += 1
    assert (mask > 0).all()

def assert_property_shape(fps, tiles, tx, ty, ox, oy, param2):
    assert tiles.shape == (ty, tx)


def assert_property_unique(fps, tiles, tx, ty, ox, oy, param2):
    tls = np.vectorize(operator.attrgetter('tl'), signature='()->(2)')(tiles.flatten())
    assert np.unique(tls, axis=0).shape[0] == tiles.size


def assert_property_origin(fps, tiles, tx, ty, ox, oy, param2):
    if param2 == 'br':
        assert (tiles[0, 0].tl == fps.GS.tl).all()
    elif param2 == 'tl':
        assert (tiles[-1, -1].br == fps.GS.br).all()
    elif param2 == 'tr':
        assert (tiles[-1, 0].bl == fps.GS.bl).all()
    elif param2 == 'bl':
        assert (tiles[0, -1].tr == fps.GS.tr).all()
    else:
        assert False

_BOUNDARY_EFFECT_PROPERTIES = {
    'extend': [
        assert_property_tile_size,
        assert_property_overlap,
        assert_property_full_pixel_coverage,

        assert_property_shape,
        assert_property_origin,
        assert_property_share_area,
        assert_property_unique,
    ],
    'overlap': [
        assert_property_tile_size,
        assert_property_global_bounds,
        assert_property_full_pixel_coverage,

        assert_property_shape,
        assert_property_origin,
        assert_property_share_area,
        assert_property_unique,
    ],
    'exclude': [
        assert_property_tile_size,
        assert_property_overlap,
        assert_property_global_bounds,

        assert_property_shape,
        assert_property_origin,
        assert_property_share_area,
        assert_property_unique,
    ],
    'shrink': [
        assert_property_overlap,
        assert_property_global_bounds,
        assert_property_full_pixel_coverage,

        assert_property_shape,
        assert_property_origin,
        assert_property_share_area,
        assert_property_unique,
    ],
    'exception': [
        assert_property_tile_size,
        assert_property_overlap,
        assert_property_global_bounds,
        assert_property_full_pixel_coverage,

        assert_property_shape,
        assert_property_origin,
        assert_property_share_area,
        assert_property_unique,
    ],

}

def test_value_error(fps):
    with pytest.raises(ValueError, match='colcount'):
        fps.AI.tile_count(1, -1)
    with pytest.raises(ValueError, match='rowcount'):
        fps.AI.tile_count(-1, 1)
    with pytest.raises(ValueError, match='effect'):
        fps.AI.tile_count(1, 1, boundary_effect='')
    with pytest.raises(ValueError, match='effect_locus'):
        fps.AI.tile_count(1, 1, boundary_effect_locus='')
