from buzzard._a_emissary_vector import *
from buzzard._a_pooled_emissary import *

class APooledEmissaryVector(APooledEmissary, AEmissaryVector):
    """Proxy that has both PooledEmissary and Vector specifications
    """
    pass

class ABackPooledEmissaryVector(ABackPooledEmissary, ABackEmissaryVector):
    """Implementation of APooledEmissaryVector's specifications"""
    pass
