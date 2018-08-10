from buzzard._a_emissary import AEmissary, ABackEmissary

class APooledEmissary(AEmissary):
    """Emissary that has a notion of activation/deactivation (file descriptor)"""

    def activate(self):
        """Make sure that at least one driver object is active for this Raster/Vector"""
        self._back.activate()

    def deactivate(self):
        """Collect all active driver object for this Raster/Vector. If a driver object is currently
        being used, will raise an exception."""
        self._back.deactivate()

    @property
    def active_count(self):
        """Count how many driver objects are currently active for this Raster/Vector"""
        return self._back.active_count

    @property
    def active(self):
        """Is there any driver object currently active for this Raster/Vector"""
        return self._back.active

class ABackPooledEmissary(ABackEmissary):
    """Implementation of APooledEmissary"""

    def __init__(self, uid, **kwargs):
        self.uid = uid
        super(ABackPooledEmissary, self).__init__(**kwargs)

    def activate(self):
        self.back_ds.activate(self.uid, self.allocator)

    def deactivate(self):
        self.back_ds.deactivate(self.uid)

    @property
    def active_count(self):
        return self.back_ds.active_count(self.uid)

    @property
    def active(self):
        return self.back_ds.active_count(self.uid) > 0

    def close(self):
        """Virtual method:
        - May be overriden
        - Should always be called
        """
        self.back_ds.deactivate(self.uid)
        super(ABackPooledEmissary, self).close()
