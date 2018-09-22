from buzzard._a_stored_vector import AStoredVector, ABackStoredVector
from buzzard._a_emissary import AEmissary, ABackEmissary

class AEmissaryVector(AEmissary, AStoredVector):
    """Base abstract class defining the common behavior of all vectors that are backed by a driver.

    Features Defined
    ----------------
    - Has a `layer` (if the driver supports it)
    """

    @property
    def layer(self):
        return self._back.layer

class ABackEmissaryVector(ABackEmissary, ABackStoredVector):
    """Implementation of AEmissaryVector's specifications"""

    def __init__(self, layer, **kwargs):
        super(ABackEmissaryVector, self).__init__(**kwargs)
        self.layer = layer
