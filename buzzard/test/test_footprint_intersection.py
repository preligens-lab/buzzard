# pylint: disable=redefined-outer-name

from __future__ import division, print_function
import itertools

import numpy as np
import pytest
import shapely.geometry as sg

import buzzard as buzz
from buzzard.test.tools import fpeq
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


def test_rectangles(fps):
    for a, b in itertools.combinations_with_replacement(fps.values(), 2):
        if a.poly.overlaps(b.poly) or a.poly.covers(b.poly) or b.poly.covers(a.poly):
            assert fpeq(
                a.intersection(b),
                b.intersection(a)
            )
    dfs = [
        fps.DF,
        fps.AF.intersection(fps.DI),
        fps.DF.intersection(fps.AI),
        fps.DF.intersection(fps.AF),
        fps.DF.intersection(fps.DI),
    ]
    for a in dfs:
        assert fpeq(a, dfs[0])
    bhs = [
        fps.BH,
        fps.AH.intersection(fps.BI),
        fps.BH.intersection(fps.AI),
        fps.BH.intersection(fps.AH),
        fps.BH.intersection(fps.BI),
    ]
    for a in bhs:
        assert fpeq(a, bhs[0])

    assert fpeq(
        fps.E,
        fps.E.intersection(fps.E),
        fps.AI.intersection(
            fps.BH, fps.DF,
            fps.AH, fps.BI,
            fps.AF, fps.DI,
            fps.AI,
        ),
    )

def test_points(fps1px):
    """Test points at fps.E's points of interest"""
    fps = fps1px
    assert fpeq(
        fps.E,
        fps.AI.intersection(sg.Point(*fps.E.c)),
        fps.AI.intersection(sg.Point(*fps.E.t)),
        fps.AI.intersection(sg.Point(*fps.E.l)),
        fps.AI.intersection(sg.Point(*fps.E.tl)),
    )
    assert fpeq(
        fps.I,
        fps.AI.intersection(sg.Point(*fps.E.br)),
    )
    assert fpeq(
        fps.H,
        fps.AI.intersection(sg.Point(*fps.E.bl)),
        fps.AI.intersection(sg.Point(*fps.E.b)),
    )
    assert fpeq(
        fps.F,
        fps.AI.intersection(sg.Point(*fps.E.tr)),
        fps.AI.intersection(sg.Point(*fps.E.r)),
    )

def test_lines(fps1px):
    """Test small lines centered at fps.E's points of interest
    Test all 3 orientations
    """
    fps = fps1px

    def _f(coords, axes):
        """create small line around point"""
        axes = np.asarray(axes)
        assert 1 <= axes.sum() <= 2
        epsilon = 10 ** -(buzz.env.significant - 1)
        tl = coords - (epsilon, epsilon) * axes * (1, -1)
        br = coords + (epsilon, epsilon) * axes * (1, -1)
        return sg.LineString([tl, br])

    # Middle, size 1
    assert fpeq(
        fps.E,
        fps.AI.intersection(_f(fps.E.c, (1, 0))),
        fps.AI.intersection(_f(fps.E.c, (1, 1))),
        fps.AI.intersection(_f(fps.E.c, (0, 1))),
        fps.AI.intersection(_f(fps.E.l, (0, 1))),
        fps.AI.intersection(_f(fps.E.t, (1, 0))),
    )

    # Connectivity 4, size 1&2
    assert fpeq(
        fps.BE,
        fps.AI.intersection(_f(fps.E.t, (1, 1))),
        fps.AI.intersection(_f(fps.E.t, (0, 1))),
        fps.AI.intersection(_f(fps.E.tl, (0, 1))),
    )

    assert fpeq(
        fps.EF,
        fps.AI.intersection(_f(fps.E.r, (1, 0))),
        fps.AI.intersection(_f(fps.E.r, (1, 1))),
        fps.AI.intersection(_f(fps.E.tr, (1, 0))),
    )
    assert fpeq(
        fps.F,
        fps.AI.intersection(_f(fps.E.r, (0, 1))),
    )

    assert fpeq(
        fps.EH,
        fps.AI.intersection(_f(fps.E.b, (1, 1))),
        fps.AI.intersection(_f(fps.E.b, (0, 1))),
        fps.AI.intersection(_f(fps.E.bl, (0, 1))),
    )
    assert fpeq(
        fps.H,
        fps.AI.intersection(_f(fps.E.b, (1, 0))),
    )

    assert fpeq(
        fps.DE,
        fps.AI.intersection(_f(fps.E.l, (1, 0))),
        fps.AI.intersection(_f(fps.E.l, (1, 1))),
        fps.AI.intersection(_f(fps.E.tl, (1, 0))),
    )

    # Connectivity 8, size 4
    assert fpeq(
        fps.AE,
        fps.AI.intersection(_f(fps.E.tl, (1, 1))),
    )
    assert fpeq(
        fps.BF,
        fps.AI.intersection(_f(fps.E.tr, (1, 1))),
    )
    assert fpeq(
        fps.EI,
        fps.AI.intersection(_f(fps.E.br, (1, 1))),
    )
    assert fpeq(
        fps.DH,
        fps.AI.intersection(_f(fps.E.bl, (1, 1))),
    )

    # Connectivity 8, size 2
    assert fpeq(
        fps.CF,
        fps.AI.intersection(_f(fps.E.tr, (0, 1))),
    )
    assert fpeq(
        fps.FI,
        fps.AI.intersection(_f(fps.E.br, (0, 1))),
    )
    assert fpeq(
        fps.HI,
        fps.AI.intersection(_f(fps.E.br, (1, 0))),
    )
    assert fpeq(
        fps.GH,
        fps.AI.intersection(_f(fps.E.bl, (1, 0))),
    )

    # Bonus (diagonals)
    assert fpeq(
        fps.AI,
        fps.AI.intersection(sg.LineString([fps.AI.tl, fps.AI.br])),
        fps.AI.intersection(sg.LineString([fps.AI.bl, fps.AI.tr])),
        fps.AI.intersection(sg.LineString([fps.A.t, fps.I.b])),
        fps.AI.intersection(sg.LineString([fps.A.l, fps.I.r])),
        fps.AI.intersection(sg.LineString([fps.A.c, fps.I.c])),
    )

    # Bonus 2 (multi point line)
    assert fpeq(
        fps.AI,
        fps.AI.intersection(sg.LineString([
            fps.A.c, fps.F.c, fps.G.c,
        ])),
        fps.AI.intersection(sg.LineString([
            fps.A.c, fps.C.c, fps.G.r,
        ])),
    )


class _FtPoly(object):

    def __init__(self, data):
        self.__geo_interface__ = data


def test_corner_cases(fps1px):
    fps = fps1px

    with pytest.raises(ValueError):
        fps.A.intersection()
    with pytest.raises(ValueError):
        fps.A.intersection(fps.A, hello=True)
    with pytest.raises(TypeError):
        fps.A.intersection(42)
    assert fpeq(
        fps.BH,
        fps.AH.intersection(_FtPoly(fps.BI.__geo_interface__)),
    )
    with pytest.raises(ValueError):
        fps.A.intersection(fps.A, scale='hello')
    with pytest.raises(ValueError):
        fps.A.intersection(fps.A, rotation='hello')
    with pytest.raises(ValueError):
        fps.A.intersection(fps.A, alignment='hello')

    # reso
    assert fpeq(
        buzz.Footprint(rsize=[2, 6], size=fps.BH.size, tl=fps.BH.tl),
        fps.AH.intersection(fps.BI, scale=0.5),
        fps.AH.intersection(fps.BI, scale=[0.5, -0.5]),
        fps.AH.intersection(fps.BI, scale=[0.5]),
    )
    with pytest.raises(ValueError):
        fps.AH.intersection(fps.BI, scale=[])
    with pytest.raises(ValueError):
        fps.AH.intersection(fps.BI, scale=0)

    lowest = fps.BH.intersection(fps.BH, scale=0.5)
    highest = fps.BH.intersection(fps.BH, scale=1.0)
    assert fpeq(
        lowest,
        lowest.intersection(highest, scale='lowest'),
        highest.intersection(lowest, scale='lowest'),
    )
    assert fpeq(
        highest,
        highest.intersection(lowest, scale='highest'),
        lowest.intersection(highest, scale='highest'),
    )

    # rot / align
    assert fpeq(
        fps.BH,
        fps.AH.intersection(fps.BI, rotation=0),
        fps.AH.intersection(fps.BI, alignment=fps.BH.tl),
    )
    with pytest.raises(ValueError):
        fps.AH.intersection(fps.BI, alignment=[])
    assert fpeq(
        buzz.Footprint(
            rsize=[1 + 1, 3 + 1], size=fps.BH.size * [2/1, 4/3], tl=fps.BH.tl - [0.5, -0.5]
        ),
        fps.BH.intersection(fps.BH, alignment=[0.5, 0.5]),
    )
    assert fpeq(
        fps.BH,
        fps.BH.intersection(fps.BH, alignment='tl'),
    )
    with buzz.Env(warnings=False, allow_complex_footprint=True):
        for angle in np.r_[0:180:13j]:
            rotated = fps.E.intersection(fps.E, rotation=angle)
            nofit = angle % 90 != 0
            if nofit:
                assert tuple(rotated.rsize) == (2, 2)
            else:
                assert tuple(rotated.rsize) == (1, 1)
            assert all(np.around(rotated.scale, 3) == (1, -1))
            assert np.around(rotated.angle, 3) == angle
            diff = rotated.poly - fps.E.poly
            if nofit:
                assert np.around(diff.area, 3) == 3.0
            else:
                assert np.around(diff.area, 3) == 0.0
            dot = np.dot(fps.E.lrvec / fps.E.w, rotated.lrvec / rotated.w)
            angle_real = np.arccos(dot) / np.pi * 180
            assert np.around(angle_real) == angle

    # homo
    assert fpeq(
        fps.BH,
        fps.BH.intersection(fps.BH, homogeneous=True),
    )
    with pytest.raises(ValueError, match='grid'):
        fps.BH.intersection(
            fps.BH.intersection(fps.BH, scale=0.5),
            homogeneous=True,
        )
    with pytest.raises(ValueError, match='grid'):
        fps.BH.intersection(
            fps.BH.intersection(fps.BH, alignment=[0.5, 0.5]),
            homogeneous=True,
        )
    with buzz.Env(warnings=False, allow_complex_footprint=True):
        with pytest.raises(ValueError, match='grid'):
            fps.AH.intersection(
                fps.E.intersection(fps.E, rotation=42),
                homogeneous=True,
            )

    # fit
    stripe = sg.Polygon([fps.A.tr, fps.I.tr, fps.I.bl, fps.A.bl])
    assert fpeq(
        fps.AI,
        fps.AI.intersection(stripe),
        fps.AI.intersection(stripe, rotation=0),
    )
    with buzz.Env(warnings=False, allow_complex_footprint=True):
        assert fpeq(
            fps.AI.intersection(stripe, rotation='fit'),
            fps.AI.intersection(stripe, rotation=45),
        )

    # misc
    with pytest.raises(ValueError, match='touch'):
        fps.A.intersection(fps.B)
    with pytest.raises(ValueError, match='empty'):
        fps.A.intersection(fps.C)
    with pytest.raises(ValueError, match='touch'):
        fps.A.intersection(fps.D)
