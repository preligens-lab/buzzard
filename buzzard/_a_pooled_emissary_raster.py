from buzzard._a_emissary_raster import AEmissaryRaster, ABackEmissaryRaster
from buzzard._a_pooled_emissary import APooledEmissary, ABackPooledEmissary

class APooledEmissaryRaster(APooledEmissary, AEmissaryRaster):
    """Base abstract class defining the common behavior of all rasters that can deactivate and
    reactivate their underlying driver at will.

    Features Defined
    ----------------
    None
    """
    pass

class ABackPooledEmissaryRaster(ABackPooledEmissary, ABackEmissaryRaster):
    """Implementation of APooledEmissaryRaster's specifications"""
    pass
