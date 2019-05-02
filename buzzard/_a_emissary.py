from buzzard._a_stored import AStored, ABackStored

from buzzard import _tools

class AEmissary(AStored):
    """Base abstract class defining the common behavior of all sources that are backed by a driver.

    Features Defined
    ----------------
    - Has a `driver` (like "GTiff" for GDAL's geotiff driver)
    - Has `open_options`
    - Has a `path` (if the driver supports it)
    - Can be deleted (if the driver supports it)
    """

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
        """Get the file system path of this source, may be the empty string if not applicable"""
        return self._back.path

    @property
    def delete(self):
        """Delete a source with a call or a context management. May raise an exception if not
        applicable or if `mode` = 'r'
        The `delete` attribute returns an object that can be both called and used in a with statement

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
        if self.mode != 'w':
            raise RuntimeError('Cannot remove a read-only file')

        def _delete():
            self._back.delete()
            self.close()

        return _DeleteRoutine(self, _delete)

    remove = delete

class ABackEmissary(ABackStored):
    """Implementation of AEmissary's specifications"""

    def __init__(self, driver, open_options, path, **kwargs):
        self.driver = driver
        self.open_options = open_options
        self.path = path
        super(ABackEmissary, self).__init__(**kwargs)


    def delete(self):
        """Virtual method:
        - May be overriden
        - Should always be called
        """
        pass

_DeleteRoutine = type('_DeleteRoutine', (_tools.CallOrContext,), {
    '__doc__': AEmissary.delete.__doc__,
})
