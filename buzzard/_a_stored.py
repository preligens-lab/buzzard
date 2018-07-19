from a_proxy import *

class AStored(AProxy):

    @property
    def mode(self):
        """Open mode, one of {'r', 'w'}"""
        return self._back.mode

class ABackStored(ABackProxy):

    def __init__(self, mode, **kwargs):
        self.mode = mode
        super(ABackStored, self).__init__(**kwargs)

    @property
    def mode(self):
        return self._back.mode
