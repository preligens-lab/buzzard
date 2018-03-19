""">>> help(Proxy)"""

from osgeo import osr

class Proxy(object):
    """Base class to all sources"""

    class _Constants(object):

        def __init__(self, ds, **kwargs):
            print('Proxy._Constants __init__', kwargs)
            # Opening informations
            # None

            # GDAL informations
            if 'gdal_ds' in kwargs:
                gdal_ds = kwargs.pop('gdal_ds')
                kwargs['wkt_origin'] = gdal_ds.GetProjection()
            self.wkt_origin = kwargs.pop('wkt_origin')

            if kwargs:
                raise RuntimeError('kwargs should be empty at this points of code: {}'.format(
                    list(kwargs.keys())
                ))

        @property
        def suspendable(self):
            return True

    def __init__(self, ds, wkt, rect):
        wkt_origin = wkt
        del wkt

        # If `ds` mode overrides file's origin
        if ds._wkt_origin:
            wkt_origin = ds._wkt_origin

        # If origin missing and `ds` provides a fallback origin
        if not wkt_origin and ds._wkt_implicit:
            wkt_origin = ds._wkt_implicit

        # Whether or not `ds` enforces a work projection
        if wkt_origin:
            sr_origin = osr.SpatialReference(wkt_origin)
        else:
            sr_origin = None

        to_work, to_file = ds._get_transforms(sr_origin, rect)

        self._ds = ds
        self._wkt_origin = wkt_origin
        self._sr_origin = sr_origin
        self._to_file = to_file
        self._to_work = to_work

    @property
    def wkt_origin(self):
        """File's spatial reference in WKT format"""
        return self._wkt_origin

    @property
    def proj4_origin(self):
        """File's spatial reference in proj4 format"""
        if not self._sr_origin:
            return None # pragma: no cover
        return self._sr_origin.ExportToProj4()
