from buzzard._a_stored import *
from buzzard import _tools

def AEmissary(AStored):

    @property
    def driver(self):
        """Get the driver name, such as 'GTiff' or 'GeoJSON'"""
        return self._back.driver

    @property
    def open_options(self):
        """Get the list of options used for opening"""
        return self._back.open_options

    @property
    def path(self):
        """Get the file system path of this proxy, may be the empty string if not applicable"""
        return self._back.path

    @property
    def delete(self):
        """Delete a proxy with a call or a context management. May raise an exception if not
        applicable or if `mode` = 'r'

        Example
        -------
        >>> ds.dem.delete()
        >>> with ds.dem.delete:
                # code...
        >>> with ds.acreate_raster('/tmp/tmp.tif', fp, float, 1).delete as tmp:
                # code...
        >>> with ds.acreate_vector('/tmp/tmp.shp', 'polygon').delete as tmp:
                # code...
        """
        return _DeleteRoutine(self, self._back.delete)

def ABackEmissary(ABackStored):

    def __init__(self, driver_obj, driver, open_options, path, **kwargs):
        self.driver = driver
        self.open_options = open_options
        self.path = path
        self.driver_obj = driver_obj
        super(ABackEmissary, self).__init__(**kwargs)

    def delete(self):
        """Virtual method:
        - May be overriden
        - Should always be called
        """
        self.close()

    @property
    def driver_obj(self):
        return self.driver_obj

_DeleteRoutine = type('_DeleteRoutine', (_tools.CallOrContext,), {
    '__doc__': AEmissary.delete.__doc__,
})
