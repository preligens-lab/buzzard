from buzzard._a_stored_raster import *
from buzzard._a_emissary import *

class AEmissaryRaster(AEmissary, AStoredRaster):
    """>>> help(AEmissary)
    >>> help(AStoredRaster)
    """
    pass

class ABackEmissaryRaster(ABackEmissary, ABackStoredRaster):
    """Implementation of AEmissaryRaster's specifications"""
    pass
