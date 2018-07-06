""">>> help(Proxy)"""

from osgeo import osr

from buzzard._tools import deprecation_pool

class Proxy(object):
    """Base class to all sources"""

    class _Constants(object):
        """Bundles all constant information about a proxy object. It allows a proxy class to:
        - function when its gdal backend object is not available (The above class is then said to be deactivated)
        - fully recreate its gdal backend object
        - be easilly pickled/unpickled

        Guidelines
        ----------
        - The constructor must be usable for constructions both from gdal pointers and unpickling
        - An information should not be duplicated inside a `_Constants` object
           - i.e. Raster.nodata can be derived from Raster._Constants.band_schema
           - i.e. Raster.fp can be derived from `ds`, `Raster._Constants.fp_stored` and
             `Proxy._Constants.wkt`
        - The `_Constants` class is contant and does not make any side effect

        """

        def __init__(self, ds, **kwargs):
            self.wkt = kwargs.pop('wkt')
            assert not kwargs, 'kwargs should be empty at this points of code: {}'.format(
                list(kwargs.keys())
            )

    def __init__(self, ds, consts, rect):
        wkt_virtual = consts.wkt

        # If `ds` mode overrides file's stored
        if ds._wkt_forced:
            wkt_virtual = ds._wkt_forced

        # If stored missing and `ds` provides a fallback stored
        if not wkt_virtual and ds._wkt_fallback:
            wkt_virtual = ds._wkt_fallback

        # Whether or not `ds` enforces a work projection
        if wkt_virtual:
            sr_virtual = osr.SpatialReference(wkt_virtual)
        else:
            sr_virtual = None

        to_work, to_virtual = ds._get_transforms(sr_virtual, rect)

        self._c = consts
        self._ds = ds
        self._wkt_virtual = wkt_virtual
        self._sr_virtual = sr_virtual
        self._to_virtual = to_virtual
        self._to_work = to_work

    @property
    def wkt_virtual(self):
        """The spatial reference considered to be written in the metadata of a raster/vector
        storage, in WKT format.
        string or None
        """
        return self._wkt_virtual

    @property
    def proj4_virtual(self):
        """The spatial reference considered to be written in the metadata of a raster/vector
        storage, in proj4 format.
        string or None
        """
        if self._wkt_virtual is None:
            return None # pragma: no cover
        return osr.SpatialReference(self._wkt_virtual).ExportToProj4()

    @property
    def wkt_stored(self):
        """The spatial reference that can be found in the metadata of a raster/vector storage
        storage, in wkt format.
        string or None
        """
        return self._c.wkt_stored

    @property
    def proj4_stored(self):
        """The spatial reference that can be found in the metadata of a raster/vector storage
        storage, in proj4 format.
        string or None
        """
        if self._c.wkt_stored is None:
            return None # pragma: no cover
        return osr.SpatialReference(self._c.wkt_stored).ExportToProj4()

    # Activation mechanisms ********************************************************************* **
    @property
    def deactivable(self):
        """Whether or not the source is deactivable.
        Those sources can't be deactivated:
        - Raster with the 'MEM' driver
        - Vector with the `Memory` driver
        - Raster recipe
        """
        return True

    @property
    def picklable(self):
        """Whether or not the source can be pickled
        Those sources can't be pickled:
        - Raster with the 'MEM' driver
        - Vector with the `Memory` driver
        """
        return True

    @property
    def activated(self):
        """Whether or not the source is currently activated"""
        raise NotImplementedError('Should be implemented by all subclasses')

    def activate(self):
        """Activate a source

        Corner cases
        ------------
        - If the source is not deactivable: fails silently
        - If the source is already activated: fails silently
        - Since some operations requires a proxy to stay activated (like Vector.iter_data), this
          function may fail if the DataSource's activation queue is full
        """
        if not self.deactivable:
            return
        self._ds._activate(self)

    def deactivate(self):
        """Deactivate a source

        Corner cases
        ------------
        - If the source is not deactivable: fails silently
        - If the source is already deactivated: fails silently
        - If some cases a source can't be deactivated, like during a `Vector.iter_data` fails
          silently
        """
        if not self.deactivable:
            return
        self._ds._deactivate(self)

    def _activate(self):
        """The actual implentation of the activation process"""
        raise NotImplementedError('Should be implemented by deactivable subclasses') # pragma: no cover

    def _deactivate(self):
        """The actual implentation of the deactivation process"""
        raise NotImplementedError('Should be implemented by deactivable subclasses') # pragma: no cover

    def _lock_activate(self):
        """Once locked a source cannot be deactivated"""
        assert self.deactivable
        self._ds._lock_activate(self)

    def _unlock_activate(self):
        """Once unlocked a source may be deactivated again"""
        assert self.deactivable
        self._ds._unlock_activate(self)

    # The end *********************************************************************************** **
    # ******************************************************************************************* **

deprecation_pool.add_deprecated_property(Proxy, 'wkt_virtual', 'wkt_origin', '0.4.4')
deprecation_pool.add_deprecated_property(Proxy, 'proj4_virtual', 'proj4_origin', '0.4.4')
