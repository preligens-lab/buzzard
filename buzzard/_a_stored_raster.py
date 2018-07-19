from buzzard._a_stored import *
from buzzard._a_proxy_raster import *

class AStoredRaster(AStored, AProxyRaster):

    def set_data(self, array, fp=None, band=1, interpolation='cv_area', mask=None, op=np.rint):
        """Write `array` located at `fp` to raster's storage. An optional `mask` may be provided.

        `fp` can be partially or fully outside of target

        If `allow_interpolation` is enabled in the DataSource constructor, it is then possible to
        use a `fp` that is not aligned  with the source raster, interpolation in then used to remap
        `array` from `fp` to raster. `nodata` values are also handled and spreaded to the raster
        through remapping.

        When remapping, if the input is not aligned with the raster file, at most one pixel pixel
        may be lost at edges due to interpolation. Provide more context in `array` to counter this effect.

        Parameters
        ----------
        array: numpy.ndarray of shape (Y, X) or (Y, X, B)
            Input data
        fp: Footprint
            Of shape (Y, X)
            Within in raster file
        band: band index or sequence of band index (see `Band Indices` below)
        interpolation: one of ('cv_area', 'cv_nearest', 'cv_linear', 'cv_cubic', 'cv_lanczos4')
            Resampling method
        mask: numpy array of shape (Y, X) OR inputs accepted by Footprint.burn_polygons
        op: None or vector function
            Rounding function following an interpolation when file type is integer

        Band Indices
        ------------
        | index type | index value     | meaning          |
        |------------|-----------------|------------------|
        | int        | -1              | All bands        |
        | int        | 1, 2, 3, ...    | Band `i`         |
        | complex    | -1j             | All bands mask   |
        | complex    | 0j              | Shared mask band |
        | complex    | 1j, 2j, 3j, ... | Mask of band `i` |

        """
        self._back.set_data(
            array=array,
            fp=fp,
            band=band,
            interpolation=interpolation,
            mask=mask,
            op=op,
        )

    def fill(self, value, band=1):
        """Fill bands with value.

        Parameters
        ----------
        value: nbr
        band: band index or sequence of band index (see `Band Indices` below)

        Band Indices
        ------------
        | index type | index value     | meaning          |
        |------------|-----------------|------------------|
        | int        | -1              | All bands        |
        | int        | 1, 2, 3, ...    | Band `i`         |
        | complex    | -1j             | All bands mask   |
        | complex    | 0j              | Shared mask band |
        | complex    | 1j, 2j, 3j, ... | Mask of band `i` |

        """
        self._back.fill(
            value=value,
            fill=fill,
        )

class ABackStoredRaster(ABackStored, ABackProxyRaster):

    def set_data(self, array, fp, band, interpolation, mask, op):
        raise NotImplementedError('ABackStoredRaster.set_data is virtual pure')

    def fill(self, value, band):
        raise NotImplementedError('ABackStoredRaster.fill is virtual pure')
