from buzzard._a_emissary import *

class APooledEmissary(AEmissary):
    def activate(self):
        self._back.activate()

    def deactivate(self):
        self._back.deactivate()

    @property
    def active_count(self):
        return self._back.active_count

    @property
    def active(self):
        return self._back.active

class ABackPooledEmissary(ABackEmissary):

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

    @property
    def acquire_driver_object(self):
        return self.back_ds.acquire_driver_object(self.uid, self.allocator)

    def close(self):
        """Virtual method:
        - May be overriden
        - Should always be called
        """
        self.back_ds.deactivate(self.uid)
        super(ABackPooledEmissary, self).close()

    # def allocator(self):
    #     raise NotImplementedError('ABackPooledEmissary.allocator is virtual pure')
