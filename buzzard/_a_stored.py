from buzzard._a_proxy import AProxy, ABackProxy

class AStored(AProxy):
    """Base abstract class defining the common behavior of all sources that are stored somewhere
    (like RAM or disk).

    Features Defined
    ----------------
    - Has an opening mode
    """

    @property
    def mode(self):
        """Open mode, one of {'r', 'w'}"""
        return self._back.mode

class ABackStored(ABackProxy):
    """Implementation of AStored's specifications"""

    def __init__(self, mode, **kwargs):
        self.mode = mode
        super(ABackStored, self).__init__(**kwargs)
