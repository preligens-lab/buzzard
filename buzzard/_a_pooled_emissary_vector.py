from buzzard._a_emissary_vector import AEmissaryVector, ABackEmissaryVector
from buzzard._a_pooled_emissary import APooledEmissary, ABackPooledEmissary

class APooledEmissaryVector(APooledEmissary, AEmissaryVector):
    """Base abstract class defining the common behavior of all vectors that can deactivate and
    reactivate their underlying driver at will.

    Features Defined
    ----------------
    None
    """
    pass

class ABackPooledEmissaryVector(ABackPooledEmissary, ABackEmissaryVector):
    """Implementation of APooledEmissaryVector's specifications"""
    pass
