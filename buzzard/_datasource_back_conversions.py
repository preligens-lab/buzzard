import numpy as np
from osgeo import osr

from buzzard import srs
from buzzard._footprint import Footprint

class BackDataSourceConversionsMixin(object):
    """Private mixin for the DataSource class containing the spatial coordinates
    conversion subroutines"""

    def __init__(self, wkt_work, wkt_fallback, wkt_forced, analyse_transformation, **kwargs):

        if wkt_work is not None:
            sr_work = osr.SpatialReference(wkt_work)
        else:
            sr_work = None
        if wkt_fallback is not None:
            sr_fallback = osr.SpatialReference(wkt_fallback)
        else:
            sr_fallback = None
        if wkt_forced is not None:
            sr_forced = osr.SpatialReference(wkt_forced)
        else:
            sr_forced = None

        self.wkt_work = wkt_work
        self.wkt_fallback = wkt_fallback
        self.wkt_forced = wkt_forced
        self.sr_work = sr_work
        self.sr_fallback = sr_fallback
        self.sr_forced = sr_forced
        self.analyse_transformations = analyse_transformation
        super(BackDataSourceConversionsMixin, self).__init__(**kwargs)

    def get_transforms(self, sr_virtual, rect, rect_from='virtual'):
        """Retrieve the `to_work` and `to_virtual` conversion functions.

        Parameters
        ----------
        sr_virtual: osr.SpatialReference
        rect: Footprint or extent or None
        rect_from: one of ('virtual', 'work')
        """
        assert rect_from in ['virtual', 'work']

        if self.sr_work is None:
            return None, None

        assert sr_virtual is not None

        to_work = osr.CreateCoordinateTransformation(sr_virtual, self.sr_work).TransformPoints
        to_virtual = osr.CreateCoordinateTransformation(self.sr_work, sr_virtual).TransformPoints

        to_work = self._make_transfo(to_work)
        to_virtual = self._make_transfo(to_virtual)

        if self.analyse_transformations:
            if rect_from == 'virtual':
                an = srs.Analysis(to_work, to_virtual, rect)
            else:
                an = srs.Analysis(to_virtual, to_work, rect)
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

        return to_work, to_virtual

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

    def convert_footprint(self, fp, wkt):
        sr = osr.SpatialReference(wkt)
        _, to_virtual = self.get_transforms(sr, fp, 'work')
        if to_virtual:
            fp = fp.move(*to_virtual([fp.tl, fp.tr, fp.br]))
        return fp

    @staticmethod
    def virtual_of_stored_given_mode(stored, work, fallback, forced):
        virtual = stored

        # Mode 4: If `ds` mode overrides file's stored
        if forced is not None:
            virtual = forced

        # Mode 3: If stored missing and `ds` provides a fallback
        if virtual is None and fallback is not None:
            virtual = fallback

        # Mode 2: If stored missing and `ds` does not provide a fallback
        if virtual is None and work is not None:
            raise ValueError("Missing proxy's spatial reference while using a `mode 2` DataSource")

        # Mode 1:
        if work is None:
            pass

        return virtual
