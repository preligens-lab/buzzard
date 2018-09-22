from buzzard._a_stored_raster import AStoredRaster, ABackStoredRaster
from buzzard._a_emissary import AEmissary, ABackEmissary

class AEmissaryRaster(AEmissary, AStoredRaster):
    """Base abstract class defining the common behavior of all rasters that are backed by a driver.

    Features Defined
    ----------------
    None
    """
    pass

class ABackEmissaryRaster(ABackEmissary, ABackStoredRaster):
    """Implementation of AEmissaryRaster's specifications"""
    pass
