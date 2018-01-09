
# pylint: disable=redefined-outer-name

from __future__ import division, print_function
import itertools

import numpy as np
import pytest
from affine import Affine

import buzzard as buzz
from buzzard.test.tools import eq, fpeq, eqall
from buzzard.test import make_tile_set

LETTERS = "ABCDEFGHI"


@pytest.fixture(scope='module')
def fps():
    """
    See make_tile_set
    A B C
    D E F
    G H I
    """
    return make_tile_set.make_tile_set(3, [0.1, -0.1])

@pytest.fixture(scope='module')
def fps1px():
    """
    See make_tile_set
    A B C
    D E F
    G H I
    """
    return make_tile_set.make_tile_set(3, [1, -1], (1, -1))

def test_size_accessors_spatial(fps):
    assert eq(fps.A.size, fps.B.size, fps.C.size, fps.D.size, fps.E.size,
              fps.F.size, fps.G.size, fps.H.size, fps.I.size,)
    assert eq(fps.AE.size, fps.BF.size, fps.DH.size, fps.EI.size)
    assert eq(fps.AH.size, fps.BI.size)
    assert eq(fps.AG.h, fps.BH.h, fps.CI.h, fps.CI.height,)
    assert eq(fps.AB.w, fps.AE.w, fps.AH.w, fps.HI.w, fps.HI.width, )


def test_size_accessors_raster(fps):
    assert eq(fps.A.rtl, [0, 0], [fps.A.rtlx, fps.A.rtly])
    assert eq(fps.A.rtl, [0, 0])
    assert eq(fps.A.rsize, fps.B.rsize, fps.C.rsize, fps.D.rsize, fps.E.rsize,
              fps.F.rsize, fps.G.rsize, fps.H.rsize, fps.I.rsize, )
    assert eq(fps.AE.rsize, fps.BF.rsize, fps.DH.rsize, fps.EI.rsize, )
    assert eq(fps.AH.rsize, fps.BI.rsize, )
    assert eq(fps.AG.rh, fps.BH.rh, fps.CI.rh, fps.CI.rheight, )
    assert eq(fps.AB.rw, fps.AE.rw, fps.AH.rw, fps.HI.rw, fps.HI.rwidth, )

    assert eq(fps.A.shape, fps.B.shape, fps.C.shape, fps.D.shape, fps.E.shape,
              fps.F.shape, fps.G.shape, fps.H.shape, fps.I.shape, )
    assert eq(fps.AE.shape, fps.BF.shape, fps.DH.shape, fps.EI.shape, )
    assert eq(fps.AH.shape, fps.BI.shape, )


def test_coordinates_accessors_spatial_corners(fps):
    buzz.Footprint(
        gt=fps.A.gt, rsize=fps.A.rsize
    )
    assert eq(fps.E.tl, fps.B.bl, fps.A.br, fps.D.tr, )
    assert eq(fps.E.bl, fps.D.br, fps.G.tr, fps.H.tl, )
    assert eq(fps.E.br, fps.H.tr, fps.I.tl, fps.F.bl, )
    assert eq(fps.E.tr, fps.F.tl, fps.C.bl, fps.B.br, )
    for letter in LETTERS:
        those_tl = [v.tl for k, v in fps.items() if k.startswith(letter)]
        assert eq(*those_tl)
        those_br = [v.br for k, v in fps.items() if k.endswith(letter)]
        assert eq(*those_br)


def test_coordinates_accessors_spatial_others(fps):
    assert eq(fps.B.b, fps.E.t, fps.EH.t, fps.AC.b, fps.DF.t, fps.DI.t, )
    assert eq(fps.D.r, fps.E.l, fps.EF.l, fps.AG.r, fps.BH.l, fps.BI.l, )
    assert eq(fps.H.t, fps.E.b, fps.BE.b, fps.GI.t, fps.DF.b, fps.AF.b, )
    assert eq(fps.F.l, fps.E.r, fps.DE.r, fps.CI.l, fps.BH.r, fps.AH.r, )
    assert eq(fps.E.c, fps.AI.c, fps.DF.c, fps.BH.c, )


def test_vector_accessors_spatial(fps):
    assert eq(fps.AC.lrvec, fps.AF.lrvec, fps.AI.lrvec, fps.DF.lrvec, fps.DI.lrvec, fps.GI.lrvec, )
    assert eq(fps.AG.tbvec, fps.AH.tbvec, fps.AI.tbvec, fps.BH.tbvec, fps.BI.tbvec, fps.CI.tbvec, )
    assert eq(fps.AE.diagvec, fps.BF.diagvec, fps.DH.diagvec, fps.EI.diagvec, )
    assert eqall([fp.pxvec for fp in fps.values()] +
                 [fp.diagvec / fp.rsize for fp in fps.values()],)


def test_coordinates_accessors_raster_corners(fps):
    assert eq(fps.AC.rtr, fps.AF.rtr, fps.AI.rtr, fps.DF.rtr, fps.DI.rtr, fps.GI.rtr, )
    assert eq(fps.AG.rbl, fps.AH.rbl, fps.AI.rbl, fps.BH.rbl, fps.BI.rbl, fps.CI.rbl, )
    assert eq(fps.AE.rbr, fps.BF.rbr, fps.DH.rbr, fps.EI.rbr, )


def test_coordinates_accessors_raster_others(fps):
    assert eq(fps.AC.rt, fps.AF.rt, fps.AI.rt, fps.DF.rt, fps.DI.rt, fps.GI.rt, )
    assert eq(fps.AG.rl, fps.AH.rl, fps.AI.rl, fps.BH.rl, fps.BI.rl, fps.CI.rl, )
    assert eq(fps.AE.rr, fps.BF.rr, fps.DH.rr, fps.EI.rr, )
    assert eq(fps.AE.rb, fps.BF.rb, fps.DH.rb, fps.EI.rb, )
    assert eq(fps.AE.rc, fps.BF.rc, fps.DH.rc, fps.EI.rc, )


def test_resolution_and_rotation_accessors(fps):

    c, a, b, f, d, e = fps.A.gt
    assert eq(
        (a, b, c, d, e, f),
        fps.A.aff6,
        fps.A.aff23.flatten(),
        fps.A.aff33.flatten()[:6],
        fps.A.affine[0:6],
    )


    def _of_all(fn):
        return [fn(fp) for fp in fps.values()]

    assert eqall(
        _of_all(lambda fp: fp.gt[1]) +
        _of_all(lambda fp: fp.scale[0]) +
        _of_all(lambda fp: fp.pxsize[0]) +
        _of_all(lambda fp: fp.pxsize[1]) +
        _of_all(lambda fp: fp.pxsizex) +
        _of_all(lambda fp: fp.pxsizey) +
        _of_all(lambda fp: fp.pxvec[0]) +
        _of_all(lambda fp: -fp.gt[5]) +
        _of_all(lambda fp: -fp.scale[1]) +
        _of_all(lambda fp: -fp.pxvec[1])
    )
    assert eqall(
        _of_all(lambda fp: fp.angle) +
        [0]
    )
    assert eqall(_of_all(lambda fp: fp.gt[2]))
    assert eqall(_of_all(lambda fp: fp.gt[4]))
    for fp in fps.values():
        assert eq(fp.gt[0], fp.tlx)
        assert eq(fp.gt[3], fp.tly)


def test_non_trivial_accessors(fps):
    assert eq(fps.AI.semimajoraxis, fps.AH.semimajoraxis, fps.AG.semimajoraxis, )
    assert eq(fps.BH.semiminoraxis, fps.BE.semiminoraxis, fps.B.semiminoraxis, )

    assert eq(
        fps.A.length + fps.B.length + fps.D.length + fps.E.length,
        fps.AE.length + fps.A.length * 2
    )

    assert eq(fps.AI.rsemimajoraxis, fps.AH.rsemimajoraxis, fps.AG.rsemimajoraxis, )
    assert eq(fps.BH.rsemiminoraxis, fps.BE.rsemiminoraxis, fps.B.rsemiminoraxis, )

    assert eq(fps.AI.rarea, np.prod(fps.AI.rsize), np.prod(fps.AI.size / fps.AI.pxsize))

    assert eq(fps.AI.rlength,
              # sum of sides minus 4
              fps.AI.rsizex * 2 + fps.AI.rsizey * 2 - 4,
              # sum of smaller rlength plus delta
              fps.AC.rlength + fps.AD.rsizey * 2,)
    fp = buzz.Footprint(gt=fps.AI.gt, rsize=(2, 10))
    assert eq(fp.rsemiminoraxis, 1)
    assert eq(fp.rlength, fp.rsemimajoraxis * 4)

    fp = buzz.Footprint(gt=fps.AI.gt, rsize=(1, 10))
    assert eq(fp.rsemiminoraxis, 1, tol=1)
    assert eq(fp.rlength, fp.rsemimajoraxis * 2)

    fp = buzz.Footprint(gt=fps.AI.gt, rsize=(1, 1))
    assert eq(fp.rsemiminoraxis, 1, fp.rsemimajoraxis, tol=1)
    assert eq(fp.rlength, 1)

def test_equal(fps):
    dfs = [
        fps.DF,
        fps.AF.intersection(fps.DI),
        fps.DF.intersection(fps.AI),
        fps.DF.intersection(fps.AF),
        fps.DF.intersection(fps.DI),
        fps.DF & fps.DI,
    ]
    for a in dfs:
        assert a == dfs[0]
    bhs = [
        fps.BH,
        fps.AH.intersection(fps.BI),
        fps.BH.intersection(fps.AI),
        fps.BH.intersection(fps.AH),
        fps.BH.intersection(fps.BI),
    ]
    for a in bhs:
        assert a == bhs[0]
    for a, b in itertools.combinations(fps.values(), 2):
        assert a != b


def test_morpho(fps):

    def create(rsizex, rsizey):
        return buzz.Footprint(gt=fps.AI.gt, rsize=(rsizex, rsizey))
    assert eq(create(3, 3).erode(1).rarea, 1 * 1)
    assert eq(create(4, 4).erode(1).rarea, 2 * 2)
    assert eq(create(5, 5).erode(1).rarea, 3 * 3)
    assert eq(create(5, 5).erode(2).rarea, 1 * 1)

    assert eq(create(3, 4).erode(1).rarea, 1 * 2)
    assert eq(create(4, 3).erode(1).rarea, 1 * 2)

    assert eq(create(2, 2).dilate(1).rarea, 4 * 4)
    assert eq(create(2, 3).dilate(1).rarea, 4 * 5)

    assert eq(create(1, 1).dilate(1).dilate(2).erode(3).rarea, 1 * 1)


def test_init_edge_cases(fps):
    # rotation
    aff = Affine.translation(42, 21) * Affine.rotation(12) * Affine.scale(0.1, -0.1)
    with buzz.Env(allow_complex_footprint=True):
        buzz.Footprint(gt=aff.to_gdal(), rsize=[1, 1])

    # missing parameters
    with pytest.raises(ValueError):
        buzz.Footprint(tl=fps.A.tl, size=fps.A.size)
    with pytest.raises(ValueError):
        buzz.Footprint(rsize=fps.A.rsize, size=fps.A.size)
    with pytest.raises(ValueError):
        buzz.Footprint(rsize=fps.A.rsize, tl=fps.A.tl)
    with pytest.raises(ValueError):
        buzz.Footprint(rsize=fps.A.rsize)

    # shapes
    with pytest.raises(ValueError, match='shape'):
        buzz.Footprint(rsize=[], tl=fps.A.tl, size=fps.A.size)
    with pytest.raises(ValueError, match='shape'):
        buzz.Footprint(rsize=fps.A.rsize, tl=[], size=fps.A.size)
    with pytest.raises(ValueError, match='shape'):
        buzz.Footprint(rsize=fps.A.rsize, tl=fps.A.tl, size=[])
    with pytest.raises(ValueError, match='shape'):
        buzz.Footprint(rsize=fps.A.rsize, gt=[])

    # finitude
    with pytest.raises(ValueError):
        buzz.Footprint(rsize=[-1] * 2, tl=fps.A.tl, size=fps.A.size)
    with pytest.raises(ValueError):
        buzz.Footprint(rsize=fps.A.rsize, tl=[np.inf] * 2, size=fps.A.size)
    with pytest.raises(ValueError):
        buzz.Footprint(rsize=fps.A.rsize, tl=fps.A.tl, size=[np.inf] * 2)
    with pytest.raises(ValueError):
        buzz.Footprint(rsize=fps.A.rsize, gt=[np.inf] * 6)

def test_clip(fps1px):
    fps = fps1px
    assert fpeq(
        fps.E,
        fps.E.clip(0, 0, 1, 1),
        fps.E.clip(-1, -1, 1, 1),
        fps.AI.clip(1, 1, 2, 2),
        fps.AI.clip(-2, -2, -1, -1),
        fps.BI.clip(0, 1, 1, 2),
        fps.BI.clip(0 - 2, 1 - 3, 1 - 2, 2 - 3),
    )


def test_move(fps1px):
    fps = fps1px

    with buzz.Env(warnings=False, allow_complex_footprint=True):
        assert fpeq(
            fps.B,
            fps.A.move(fps.B.tl),
            fps.B.move(fps.B.tl),
            fps.C.move(fps.B.tl),
            fps.A.move(fps.B.tl, fps.B.tr),
            fps.B.move(fps.B.tl, fps.B.tr),
            fps.C.move(fps.B.tl, fps.B.tr),
            fps.A.move(fps.B.tl, fps.B.tr, fps.B.br),
            fps.B.move(fps.B.tl, fps.B.tr, fps.B.br),
            fps.C.move(fps.B.tl, fps.B.tr, fps.B.br),
        )

        aff = (
            Affine.translation(*fps.A.bl) * Affine.rotation(45) * Affine.scale(2**0.5, 2**0.5 * -2)
        )
        assert fpeq(
            buzz.Footprint(gt=aff.to_gdal(), rsize=(1, 1)),
            fps.A.move(fps.A.bl, fps.A.tr, fps.I.tr),
            fps.B.move(fps.A.bl, fps.A.tr, fps.I.tr),
            fps.C.move(fps.A.bl, fps.A.tr, fps.I.tr),
        )
        with pytest.raises(ValueError, match='angle'):
            fps.C.move(fps.A.bl, fps.A.tr, fps.I.c)


def test_binary_predicates(fps):
    for fp in fps.values():
        assert fp.share_area(fps.AI)
        assert fp.same_grid(fps.AI)
        sq2 = 2 ** 0.5
        assert not fp.same_grid(fp.move([sq2, sq2]))
        with buzz.Env(allow_complex_footprint=True):
            assert not fp.same_grid(fp.move([sq2, sq2], [2 * sq2, 2 * sq2]))


def test_numpy_like_functions(fps, fps1px):

    assert eq(
        fps.A.meshgrid_raster,
        fps.B.meshgrid_raster,
        fps.C.meshgrid_raster,
        np.meshgrid(range(fps.A.rw), range(fps.A.rh)),
    )
    assert eq(
        fps.A.meshgrid_spatial,
        fps.C.meshgrid_spatial + fps.A.tl[:, None, None] - fps.C.tl[:, None, None],
        fps.I.meshgrid_spatial + fps.A.tl[:, None, None] - fps.I.tl[:, None, None],
    )

    assert eq(
        fps.A.meshgrid_raster,
        fps.A.meshgrid_raster_in(fps.AI),
        fps.A.meshgrid_raster_in(fps.A),
        fps.I.meshgrid_raster_in(fps.A) + ((fps.A.tl - fps.I.tl) / fps.A.scale)[:, None, None],
    )
    assert fps.A.meshgrid_raster_in(fps.A, dtype='uint8')[0].dtype == np.uint8
    assert fps.A.meshgrid_raster_in(fps.A, dtype='float64', op=42)[0].dtype == np.float64


    fps = fps1px

    dense = fps.AI.move(fps.A.tl, fps.A.tr)
    mesh = np.dstack(fps.EI.meshgrid_raster_in(fps.AI))
    mesh_dense = np.dstack(fps.EI.meshgrid_raster_in(dense))
    assert ((mesh_dense / mesh) == 3).all()

    op = lambda arr: (arr * 0 + 42)

    assert (np.asarray(fps.AI.meshgrid_raster_in(fps.AI, op=op)) == 42).all()

    mg_ai = np.asarray(fps.AI.meshgrid_spatial)
    for fp in fps.values():
        mg = np.asarray(fp.meshgrid_spatial)
        slices = (slice(0, 2),) + fp.slice_in(fps.AI)
        assert (mg == mg_ai[slices]).all()

    assert fps.A.meshgrid_spatial[0][fps.I.slice_in(fps.A, clip=True)].size == 0

def test_coord_conv(fps):
    ai = np.dstack(fps.AI.meshgrid_spatial)

    assert fps.AI.raster_to_spatial(ai).shape == ai.shape
    assert fps.AI.spatial_to_raster(ai).shape == ai.shape
    assert fps.AI.spatial_to_raster(ai, dtype='float16').dtype == np.float16
    assert fps.AI.spatial_to_raster(ai, dtype='float16', op=42).dtype == np.float16
