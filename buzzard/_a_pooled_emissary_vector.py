from buzzard._a_emissary_vector import AEmissaryVector, ABackEmissaryVector
from buzzard._a_pooled_emissary import APooledEmissary, ABackPooledEmissary

class APooledEmissaryVector(APooledEmissary, AEmissaryVector):
    """Proxy that has both PooledEmissary and Vector specifications"""
    pass

class ABackPooledEmissaryVector(ABackPooledEmissary, ABackEmissaryVector):
    """Implementation of APooledEmissaryVector's specifications"""
    pass
