""">>> help(IntersectionMixin)"""

from __future__ import division, print_function
import functools
import itertools

import numpy as np
from affine import Affine
import shapely.geometry as sg

from buzzard import _tools
from buzzard._env import env

class IntersectionMixin(object):
    """Private mixin for the Footprint class containing the `intersection` subroutines"""

    _INTERSECTION_RESOLUTIONS = {'self', 'highest', 'lowest'}
    _INTERSECTION_ROTATIONS = {'auto', 'fit'}
    _INTERSECTION_ALIGNMENTS = {'auto', 'tl'}

    def _intersection_expand_parameters(self, footprints, resolution, rotation, alignment):
        if isinstance(resolution, np.ndarray):
            resofp = None
            resolution = resolution
        elif resolution == 'self':
            resofp = self
            resolution = resofp.scale
        elif resolution == 'highest':
            resofp = max(footprints, key=lambda fp: np.product(fp.pxsize))
            resolution = resofp.scale
        elif resolution == 'lowest':
            resofp = min(footprints, key=lambda fp: np.product(fp.pxsize))
            resolution = resofp.scale
        else:
            assert False # pragma: no cover

        if isinstance(rotation, float):
            rotfp = None
            rotation = rotation
            fitrot = False
        elif rotation == 'auto' and resofp is None:
            rotfp = self
            rotation = rotfp.angle
            fitrot = False
        elif rotation == 'auto' and resofp is not None:
            rotfp = resofp
            rotation = rotfp.angle
            fitrot = False
        elif rotation == 'fit':
            rotfp = None
            rotation = None
            fitrot = True
        else:
            assert False # pragma: no cover

        if isinstance(alignment, np.ndarray):
            alignment = alignment
            fitalign = False
        elif alignment == 'auto' and resofp is not None and resofp is rotfp:
            alignment = resofp.tl
            fitalign = False
        elif alignment == 'auto':
            alignment = None
            fitalign = True
        elif alignment == 'tl':
            alignment = None
            fitalign = True
        else:
            assert False # pragma: no cover

        return resolution, rotation, fitrot, alignment, fitalign

    def _intersection_unsafe(self, footprints, geoms, resolution, rotation, alignment):
        geoms = [fp.poly for fp in footprints] + geoms
        for g1, g2 in itertools.combinations(geoms, 2):
            if g1.disjoint(g2):
                raise ValueError('Intersection is empty')
            elif g1.touches(g2):
                raise ValueError('Two geometries are only touching, intersection is empty')
        geom = functools.reduce(sg.Polygon.intersection, geoms)
        del geoms
        assert geom.is_valid
        assert not geom.is_empty
        resolution, rotation, fitrot, alignment, fitalign = self._intersection_expand_parameters(
            footprints, resolution, rotation, alignment
        )
        del footprints

        if fitrot:
            # TODO: Make this block work with non-polygon geom
            rect = geom.minimum_rotated_rectangle
            abovex, _, _, abovey = rect.bounds
            assert rect.exterior.is_ccw
            points = np.c_[rect.exterior.xy][0:4]

            # Get index of the point closest to (abovex, abovey) and  it as top left
            def _quadrance_to_above(pt):
                return (abovex - pt[0]) ** 2. + (abovey - pt[1]) ** 2.

            tli = np.array([_quadrance_to_above(pt) for pt in points]).argmin()
            rect = _tools.Rect(
                points[tli], points[(tli + 1) % 4],
                points[(tli + 2) % 4], points[(tli + 3) % 4],
            )
            rotation = rect.angle
        else:
            tmp_to_spatial = (
                Affine.translation(*geom.centroid.coords[0]) *
                Affine.rotation(rotation) *
                Affine.scale(*resolution)
            )
            spatial_to_tmp = ~tmp_to_spatial
            points = np.concatenate(list(_exterior_coords_iterator(geom)), axis=0)
            points = points[:, :2]
            spatial_to_tmp.itransform(points)

            rect = _tools.Rect(
                tl=tmp_to_spatial * points.min(axis=0),
                bl=tmp_to_spatial * [points[:, 0].min(), points[:, 1].max()],
                br=tmp_to_spatial * points.max(axis=0),
                tr=tmp_to_spatial * [points[:, 0].max(), points[:, 1].min()],
            )


        if env.significant <= rect.significant_min(np.abs(resolution).min()):
            raise RuntimeError('`env.significant` of value {} should be at least {}'.format(
                env.significant, rect.significant_min(np.abs(resolution).min()),
            ))

        if fitalign:
            alignment = rect.tl

        tmp_to_spatial = (
            Affine.translation(*alignment) *
            Affine.rotation(rotation) *
            Affine.scale(*resolution)
        )
        spatial_to_tmp = ~tmp_to_spatial
        abstract_grid_density = rect.abstract_grid_density(np.abs(resolution).min())


        tmptl = np.asarray(spatial_to_tmp * rect.tl)
        tmptl = np.around(tmptl * abstract_grid_density, 0) / abstract_grid_density
        tmptl = np.floor(tmptl)
        tl = tmp_to_spatial * tmptl
        aff = Affine.translation(*tl) * Affine.rotation(rotation) * Affine.scale(*resolution)

        to_pixel = ~aff

        rsize = np.asarray(to_pixel * rect.br)
        rsize = np.around(rsize * abstract_grid_density, 0) / abstract_grid_density
        rsize = np.ceil(rsize)

        if (rsize == 0).any():
            # Can happen if `geom` is 0d or 1d and `geom` lie on the alignement grid
            rsize = rsize.clip(1, np.iinfo(int).max)
            assert isinstance(geom, (sg.Point, sg.LineString))

        assert (rsize > 0).all()
        return self.__class__(
            gt=aff.to_gdal(),
            rsize=rsize,
        )

def _exterior_coords_iterator(geom):
    if isinstance(geom, sg.Point):
        yield np.asarray(geom)[None, ...]
    elif isinstance(geom, sg.Polygon):
        yield np.asarray(geom.exterior)
    elif isinstance(geom, sg.LineString):
        yield np.asarray(geom)
    elif isinstance(geom, sg.base.BaseMultipartGeometry):
        for part in geom:
            for coords in _exterior_coords_iterator(part):
                yield coords
    else:
        assert False
