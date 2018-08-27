from buzzard._a_stored_raster import AStoredRaster, ABackStoredRaster
from buzzard._a_emissary import AEmissary, ABackEmissary

class AEmissaryRaster(AEmissary, AStoredRaster):
    """Proxy that has both Emissary and Raster specifications"""
    pass

class ABackEmissaryRaster(ABackEmissary, ABackStoredRaster):
    """Implementation of AEmissaryRaster's specifications"""
    pass
