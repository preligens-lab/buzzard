import sys

from osgeo import osr

from buzzard import _tools

class AProxy(object):
    """Base abstract class defining the common behavior of all sources"""

    def __init__(self, ds, back):
        self._ds = ds
        self._back = back

    @property
    def wkt_stored(self):
        """The spatial reference that can be found in the metadata of a proxy, in wkt format.

        string or None
        """
        return self._back.wkt_stored

    @property
    def proj4_stored(self):
        """The spatial reference that can be found in the metadata of a proxy, in proj4 format.

        string or None
        """
        return self._back.proj4_stored

    @property
    def wkt_virtual(self):
        """The spatial reference considered to be written in the metadata of a proxy, in wkt
        format.

        string or None
        """
        return self._back.wkt_virtual

    @property
    def proj4_virtual(self):
        """The spatial reference considered to be written in the metadata of a proxy, in proj4
        format.

        string or None
        """
        return self._back.proj4_virtual

    @property
    def close(self):
        """Close a proxy with a call or a context management.
        The `close` attribute returns an object that can be both called and used in a with statement

        Examples
        --------
        >>> ds.dem.close()
        >>> with ds.dem.close:
                # code...
        >>> with ds.acreate_raster('result.tif', fp, float, 1).close as result:
                # code...
        >>> with ds.acreate_vector('results.shp', 'linestring').close as roofs:
                # code...
        """
        def _close():
            self._back.close()
            self._ds._unregister(self)
            del self._ds
            del self._back

        return _CloseRoutine(self, _close)

    def __del__(self):
        if hasattr(self, '_ds'):
            self.close()

    # Deprecation
    wkt_origin = _tools.deprecation_pool.wrap_property(
        'wkt_virtual',
        '0.4.4'
    )
    proj4_origin = _tools.deprecation_pool.wrap_property(
        'proj4_virtual',
        '0.4.4'
    )

class ABackProxy(object):
    """Implementation of AProxy's specifications"""

    def __init__(self, back_ds, wkt_stored, rect):
        wkt_virtual = back_ds.virtual_of_stored_given_mode(
            wkt_stored, back_ds.wkt_work, back_ds.wkt_fallback, back_ds.wkt_forced,
        )

        if wkt_virtual is not None:
            sr_virtual = osr.SpatialReference(wkt_virtual)
        else:
            sr_virtual = None

        to_work, to_virtual = back_ds.get_transforms(sr_virtual, rect)

        self.back_ds = back_ds
        self.wkt_stored = wkt_stored
        self.wkt_virtual = wkt_virtual
        self.to_work = to_work
        self.to_virtual = to_virtual

    def close(self):
        """Virtual method:
        - May be overriden
        - Should always be called
        """
        del self.back_ds

    @property
    def proj4_virtual(self):
        if self.wkt_virtual is None:
            return None # pragma: no cover
        return osr.SpatialReference(self.wkt_virtual).ExportToProj4()

    @property
    def proj4_stored(self):
        if self.wkt_stored is None:
            return None # pragma: no cover
        return osr.SpatialReference(self.wkt_stored).ExportToProj4()

_CloseRoutine = type('_CloseRoutine', (_tools.CallOrContext,), {
    '__doc__': AProxy.close.__doc__,
})

if sys.version_info < (3, 6):
    # https://www.python.org/dev/peps/pep-0487/
    for k, v in AProxy.__dict__.items():
        if hasattr(v, '__set_name__'):
            v.__set_name__(AProxy, k)
