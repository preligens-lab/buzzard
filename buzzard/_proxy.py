""">>> help(Proxy)"""

from osgeo import osr

class Proxy(object):
    """Base class to all sources"""

    class _Constants(object):
        """Bundles all constant information about a instance of the above class. It allows the above
        class to:
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
        - Since some proxies cannot be deactivated (i.e. MEM, MEMORY drivers and Recipe), the
          `deactivable` property may be implemented to prevent those proxies to interfere with
          activation lru mechanisms.
        - Since some proxies cannot be pickled (i.e. MEM, MEMORY), the `picklable` property may be
          implemented to prevent all pickling attempts.
        - The `_Constants` class is contant '^_^

        """

        def __init__(self, ds, **kwargs):
            print('Proxy._Constants __init__', kwargs)
            self.wkt = kwargs.pop('wkt')

            assert not kwargs, 'kwargs should be empty at this points of code: {}'.format(
                list(kwargs.keys())
            )

        @property
        def deactivable(self):
            return True

        @property
        def picklable(self):
            return True

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
    def activated(self):
        """

        Returns
        -------
        bool
        If the source is not deactivable, returns `True`
        """
        raise NotImplementedError('Should be implemented by subclass')

    def activate(self):
        """
        Corner cases
        ------------
        - If the source is not deactivable: fails silently
        - If the source is already activated: fails silently
        - Since some operations requires a proxy to stay activated (like Vector.iter_data), this
          function may fail if the DataSource's activation queue is full
          (see DataSource.__init__@max_activated)

        """
        raise NotImplementedError('Should be implemented by subclass')

    def deactivate(self):
        """
        Corner cases
        ------------
        - If the source is not deactivable: fails silently
        - If the source is already deactivated: fails silently
        """
        raise NotImplementedError('Should be implemented by subclass')

    # The end *********************************************************************************** **
    # ******************************************************************************************* **
