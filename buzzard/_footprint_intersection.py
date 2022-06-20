""">>> help(IntersectionMixin)"""

import functools
import itertools

import numpy as np
from affine import Affine
import shapely.geometry as sg

from buzzard import _tools
from buzzard._env import env

class IntersectionMixin:
    """Private mixin for the Footprint class containing the `intersection` subroutines"""

    _INTERSECTION_RESOLUTIONS = {'self', 'highest', 'lowest'}
    _INTERSECTION_ROTATIONS = {'auto', 'fit'}
    _INTERSECTION_ALIGNMENTS = {'auto', 'tl'}

    def _intersection_expand_parameters(self, footprints, scale, rotation, alignment):
        if isinstance(scale, np.ndarray):
            resofp = None
            scale = scale
        elif scale == 'self':
            resofp = self
            scale = resofp.scale
        elif scale == 'highest':
            resofp = max(footprints, key=lambda fp: np.product(fp.pxsize))
            scale = resofp.scale
        elif scale == 'lowest':
            resofp = min(footprints, key=lambda fp: np.product(fp.pxsize))
            scale = resofp.scale
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

        return scale, rotation, fitrot, alignment, fitalign

    def _intersection_unsafe(self, footprints, geoms, scale, rotation, alignment):
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
        scale, rotation, fitrot, alignment, fitalign = self._intersection_expand_parameters(
            footprints, scale, rotation, alignment
        )
        del footprints

        if fitrot:
            # TODO: Make this block work with non-polygon geom
            rect = geom.minimum_rotated_rectangle
            minx, miny, maxx, maxy = rect.bounds
            if scale[0] > 0:
                abovex = minx
            else:
                abovex = maxx
            if scale[1] > 0:
                abovey = miny
            else:
                abovey = maxy

            assert rect.exterior.is_ccw
            points = np.c_[rect.exterior.xy][0:4]

            # Look for the point closes to top-left
            def _quadrance_to_above(pt):
                return (abovex - pt[0]) ** 2. + (abovey - pt[1]) ** 2.

            tli = np.array([_quadrance_to_above(pt) for pt in points]).argmin()
            if (scale[0] > 0) != (scale[1] > 0):
                rect = _tools.Rect(
                    points[tli], points[(tli + 1) % 4],
                    points[(tli + 2) % 4], points[(tli + 3) % 4],
                )
            else:
                rect = _tools.Rect(
                    points[tli], points[(tli - 1) % 4],
                    points[(tli - 2) % 4], points[(tli - 3) % 4],
                )
            rotation = rect.angle
        else:
            tmp_to_spatial = (
                Affine.translation(*geom.centroid.coords[0]) *
                Affine.rotation(rotation) *
                Affine.scale(*scale)
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

        if env.significant <= rect.significant_min(np.abs(scale).min()):
            s = ('This Footprint have large coordinates and small pixels, at least {:.2} '
                'significant digits are necessary to perform this operation, but '
                 '`buzz.env.significant` is set to {}. Increase this value by using '
                 'buzz.Env(allow_complex_footprint=True) in a `with statement`.'
            ).format(rect.significant_min(np.abs(scale).min()), env.significant)
            raise RuntimeError(s)

        if fitalign:
            alignment = rect.tl

        tmp_to_spatial = (
            Affine.translation(*alignment) *
            Affine.rotation(rotation) *
            Affine.scale(*scale)
        )
        spatial_to_tmp = ~tmp_to_spatial
        abstract_grid_density = rect.abstract_grid_density(np.abs(scale).min())


        tmptl = np.asarray(spatial_to_tmp * rect.tl)
        tmptl = np.around(tmptl * abstract_grid_density, 0) / abstract_grid_density
        tmptl = np.floor(tmptl)
        tl = tmp_to_spatial * tmptl
        aff = Affine.translation(*tl) * Affine.rotation(rotation) * Affine.scale(*scale)
        to_pixel = ~aff

        rsize = np.asarray(to_pixel * rect.br)
        rsize = np.around(rsize * abstract_grid_density, 0) / abstract_grid_density
        rsize = np.ceil(rsize)

        if (rsize == 0).any():
            # Can happen if `geom` is 0d or 1d and `geom` lie on the alignement grid
            # (or very small)
            rsize = rsize.clip(1, np.iinfo(int).max)

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
            yield from _exterior_coords_iterator(part)
    else:
        assert False
