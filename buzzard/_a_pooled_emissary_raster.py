from buzzard._a_emissary_raster import AEmissaryRaster, ABackEmissaryRaster
from buzzard._a_pooled_emissary import APooledEmissary, ABackPooledEmissary

class APooledEmissaryRaster(APooledEmissary, AEmissaryRaster):
    """Proxy that has both PooledEmissary and Raster specifications"""
    pass

class ABackPooledEmissaryRaster(ABackPooledEmissary, ABackEmissaryRaster):
    """Implementation of APooledEmissaryRaster's specifications"""
    pass
