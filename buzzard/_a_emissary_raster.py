from buzzard._a_stored_raster import *
from buzzard._a_emissary import *

class AEmissaryRaster(AEmissary, AStoredRaster):
    """Proxy that has both Emissary and Raster specifications
    """
    pass

class ABackEmissaryRaster(ABackEmissary, ABackStoredRaster):
    """Implementation of AEmissaryRaster's specifications"""
    pass
