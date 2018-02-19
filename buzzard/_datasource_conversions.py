""">>> help(DataSourceConversionsMixin)"""

import numpy as np
from osgeo import osr

from buzzard import srs
from buzzard._footprint import Footprint

class DataSourceConversionsMixin(object):
    """Private mixin for the DataSource class containing the spatial coordinates
    conversion subroutines"""

    def __init__(self, sr_work, sr_implicit, sr_origin, analyse_transformation):
        self._sr_work = sr_work
        self._sr_implicit = sr_implicit
        self._sr_origin = sr_origin
        self._analyse_transformations = bool(analyse_transformation)

    def _get_transforms(self, sr_origin, rect):
        """Retrieve the `to_work` and `to_origin` conversion functions.

        Parameters
        ----------
        sr_origin: osr.SpatialReference
        rect: Footprint or extent or None
        """
        if not self._sr_work:
            return None, None
        if self._sr_origin:
            sr_origin = self._sr_origin
        elif not sr_origin:
            if self._sr_implicit:
                sr_origin = self._sr_implicit
            else:
                raise ValueError("Missing origin's spatial reference")

        to_work = osr.CreateCoordinateTransformation(sr_origin, self._sr_work).TransformPoints
        to_origin = osr.CreateCoordinateTransformation(self._sr_work, sr_origin).TransformPoints

        to_work = self._make_transfo(to_work)
        to_origin = self._make_transfo(to_origin)

        if self._analyse_transformations:
            an = srs.Analysis(to_work, to_origin, rect)
            if rect is None:
                pass
            elif isinstance(rect, Footprint):
                if not an.ratio_valid:
                    raise ValueError('Bad coord transformation for raster proxy: {}'.format(
                        an.messages
                    ))
            else:
                minx, maxx, miny, maxy = rect
                if minx != maxx and miny != maxy:
                    if not an.inverse_valid:
                        raise ValueError(
                            'Bad coord transformation for vector proxy: {}'.format(an.messages)
                        )

        return to_work, to_origin

    @staticmethod
    def _make_transfo(osr_transfo):
        """Wrap osr coordinate transformation input/output"""

        def _f(*args):
            nargs = len(args)

            if nargs == 1:
                # When coordinates in last dimension
                arr = np.asarray(args[0])
                assert arr.ndim >= 2
                ncoord = arr.shape[-1]
                assert 2 <= ncoord <= 3
                outshape = arr.shape

                arr = arr.reshape(int(arr.size / ncoord), ncoord)
                arr = osr_transfo(arr)
                arr = np.asarray(arr)
                arr = arr[:, 0:ncoord]
                arr = arr.reshape(outshape)
                return arr
            elif 2 <= nargs <= 3:
                # When coordinates in first dimension
                arr = np.asarray(args)
                assert arr.ndim == 2
                ncoord = nargs
                arr = np.moveaxis(arr, 0, 1)
                arr = osr_transfo(arr)
                arr = np.asarray(arr)
                arr = arr[:, 0:ncoord]
                arr = np.moveaxis(arr, 0, 1)
                return tuple(arr)
            else:
                assert False # pragma: no cover

        return _f

    def _convert_footprint(self, fp, sr):
        sr_tmp = osr.GetUserInputAsWKT(sr)
        sr_tmp = osr.SpatialReference(sr_tmp)
        _, to_origin = self._get_transforms(sr_tmp, fp)
        if to_origin:
            fp = fp.move(*to_origin([fp.tl, fp.tr, fp.br]))
        return fp
