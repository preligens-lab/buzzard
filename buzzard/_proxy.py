""">>> help(Proxy)"""

from osgeo import osr

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
           - i.e. Raster.fp can be derived from `ds`, `Raster._Constants.fp_origin` and
             `Proxy._Constants.wkt_origin`
        - The `_Constants` class is contant and does not make any side effect

        """

        def __init__(self, ds, **kwargs):
            self.wkt = kwargs.pop('wkt')
            assert not kwargs, 'kwargs should be empty at this points of code: {}'.format(
                list(kwargs.keys())
            )

    def __init__(self, ds, consts, rect):
        wkt_origin = consts.wkt

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

        self._c = consts
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
