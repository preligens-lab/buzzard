import numpy as np
import cv2

from buzzard._tools import ANY

_EXN_FORMAT = """Illegal remap attempt between two Footprints that do not lie on the same grid.
full raster    -> {src!s}
argument       -> {dst!s}
scales         -> full raster:{src.scale}, argument:{dst.scale}
grids distance -> {tldiff}
`allow_interpolation` was set to `False` in `DataSource` constructor. It means that either
1. there is a mistake in your code and you did not meant to perform this operation with an unaligned Footprint,
2. or that you want to perform a resampling operation and that you need `allow_interpolation` to be `True`.
"""

class ABackProxyRasterRemapMixin(object):
    """Raster Mixin containing remap subroutine"""

    _REMAP_MASK_MODES = frozenset(['dilate', 'erode', ])
    REMAP_INTERPOLATIONS = {
        'cv_area': cv2.INTER_AREA,
        'cv_nearest': cv2.INTER_NEAREST,
        'cv_linear': cv2.INTER_LINEAR,
        'cv_cubic': cv2.INTER_CUBIC,
        'cv_lanczos4': cv2.INTER_LANCZOS4,
    }

    def build_sampling_footprint(self, fp, interpolation):
        if not fp.share_area(self.fp):
            return None
        if fp.same_grid(self.fp):
            fp = fp & self.fp
            assert fp.same_grid(self.fp)
            return fp
        if not self.back_ds.allow_interpolation: # pragma: no cover
            raise ValueError(_EXN_FORMAT.format(
                src=self.fp,
                dst=fp,
                tldiff=fp.tl - (
                    self.fp.pxtbvec * np.around(~self.fp.affine * fp.tl)[1] +
                    self.fp.pxlrvec * np.around(~self.fp.affine * fp.tl)[0]
                ) - self.fp.tl,
            ))
        if interpolation in {'cv_nearest'}:
            dilate_size = 1 * self.fp.pxsizex / fp.pxsizex # hyperparameter
        elif interpolation in {'cv_linear', 'cv_area'}:
            dilate_size = 2 * self.fp.pxsizex / fp.pxsizex # hyperparameter
        else:
            dilate_size = 4 * self.fp.pxsizex / fp.pxsizex # hyperparameter
        dilate_size = max(2, np.ceil(dilate_size)) # hyperparameter too
        fp = fp.dilate(dilate_size)
        fp = self.fp & fp
        return fp

    @classmethod
    def remap(cls, src_fp, dst_fp, array, mask, src_nodata, dst_nodata, mask_mode, interpolation):
        """Function matching the signature of RasterRecipe@resample_array parameter

        Caveat
        ------
        For performance reasons, output array might share the same memory space as the input array.
        If a nodata conversion is then performed, the input array will be modified.
        """
        # Parameters cheking ******************************************************************** **
        arr_mode = array is not None, mask is not None
        fp_mode = (
            src_fp.same_grid(dst_fp),
            src_fp.poly.contains(dst_fp.poly),
        )

        # Check array / mask ***************************************************
        if arr_mode[0]:
            assert array.shape[:2] == tuple(src_fp.shape)
            assert 2 <= array.ndim <= 3
            arr_outdtype = array.dtype
            arr_outshape = list(array.shape)
            arr_outshape[:2] = dst_fp.shape
            array = np.atleast_3d(array)
        if arr_mode[1]:
            assert mask.shape == tuple(src_fp.shape)
            assert mask.dtype == np.dtype('bool')

        # Check nodata *********************************************************
        assert dst_nodata is not None

        # Check mask_mode ******************************************************
        if mask_mode not in cls._REMAP_MASK_MODES: # pragma: no cover
            raise ValueError('mask_mode should be one of {}'.format(
                cls._REMAP_MASK_MODES,
            ))

        # Check interpolation **************************************************
        if not (interpolation is None or interpolation in cls.REMAP_INTERPOLATIONS): # pragma: no cover
            raise ValueError('interpolation should be None or one of {}'.format(
                cls.REMAP_INTERPOLATIONS.keys(),
            ))

        # Remapping ***************************************************************************** **
        if fp_mode == (True, True):
            array, mask = cls._remap_slice(
                src_fp, dst_fp,
                array, mask,
                src_nodata, dst_nodata,
            )
        elif fp_mode == (True, False):
            array, mask = cls._remap_copy(
                src_fp, dst_fp,
                array, mask,
                src_nodata, dst_nodata,
            )
        elif fp_mode == (False, ANY):
            array, mask = cls._remap_interpolate(
                src_fp, dst_fp,
                array, mask,
                src_nodata, dst_nodata,
                mask_mode, interpolation,
            )
        else:
            assert False # pragma: no cover

        # Return ******************************************************************************** **
        if arr_mode[0]:
            array = array.reshape(arr_outshape).astype(arr_outdtype, copy=False)

        if arr_mode == (True, True):
            return array, mask
        elif arr_mode == (True, False):
            return array
        elif arr_mode == (False, True):
            return mask
        else:
            assert False # pragma: no cover


    @staticmethod
    def _remap_slice(src_fp, dst_fp, array, mask, src_nodata, dst_nodata):
        src_slice = dst_fp.slice_in(src_fp)

        if array is not None:
            array = array[src_slice]
            if src_nodata is not None and dst_nodata != src_nodata:
                array[array == src_nodata] = dst_nodata
        if mask is not None:
            mask = mask[src_slice]
        return array, mask

    @staticmethod
    def _remap_copy(src_fp, dst_fp, array, mask, src_nodata, dst_nodata):
        dst_slice = src_fp.slice_in(dst_fp, clip=True)
        src_slice = dst_fp.slice_in(src_fp, clip=True)

        if array is not None:
            dstarray = np.full(np.r_[dst_fp.shape, array.shape[-1]], dst_nodata, array.dtype)
            dstarray[dst_slice] = array[src_slice]
            if src_nodata is not None and dst_nodata != src_nodata:
                dstarray[dst_slice][dstarray[dst_slice] == src_nodata] = dst_nodata
        else:
            dstarray = None # pragma: no cover

        if mask is not None:
            dstmask = np.full(dst_fp.shape, 0, mask.dtype)
            dstmask[dst_slice] = mask[src_slice]
        else:
            dstmask = None # pragma: no cover

        return dstarray, dstmask

    @classmethod
    def _remap_interpolate(cls, src_fp, dst_fp, array, mask, src_nodata, dst_nodata,
                           mask_mode, interpolation):
        if array is not None and array.dtype in [np.dtype('float64'), np.dtype('bool')]:
            raise ValueError(
                'dtype {!r} not handled by cv2.remap'.format(array.dtype)
            ) # pragma: no cover

        mapx, mapy = dst_fp.meshgrid_raster_in(src_fp, dtype='float32')
        mapx, mapy = cv2.convertMaps(
            mapx, mapy, cv2.CV_16SC2,
            nninterpolation=interpolation == 'cv_nearest',
        ) # At this point mapx/mapy are not really mapx/mapy any more, but who cares?
        interpolation = cls.REMAP_INTERPOLATIONS[interpolation]

        if array is not None:
            # "Bug" 1 with cv2.BORDER_CONSTANT *********************************
            # cv2.remap with cv2.BORDER_CONSTANT considers that the constant value is part of the
            # input values and interpolates with it. Which is obviously a problem...
            # If performing resampling for nodata and values separately, the result is safe with
            # cv2.BORDER_CONSTANT, since the pixels that have been wrongly interpolated with nodata
            # are overriden afterward.
            # "Bug" 2 with cv2.BORDER_CONSTANT *********************************
            # When building the multichannel nodata mask, only the first band works correctly near
            # borders
            # Solution *********************************************************
            # cv2.BORDER_TRANSPARENT + `dst` parameter is the safe way
            #
            #  "Bug" 3 with remap **********************************************
            # If the input array has shape (Y, X, 1), the output is (Y, X).
            if src_nodata is None:
                dstarray = np.full(np.r_[dst_fp.shape, array.shape[-1]], dst_nodata, array.dtype)
                cv2.remap(
                    array, mapx, mapy,
                    interpolation=interpolation,
                    borderMode=cv2.BORDER_TRANSPARENT,
                    dst=dstarray,
                )
            else:
                dstnodatamask = np.ones(np.r_[dst_fp.shape, array.shape[-1]], 'float32')
                cv2.remap(
                    (array == src_nodata).astype('float32'), mapx, mapy,
                    interpolation=interpolation,
                    dst=dstnodatamask,
                    borderMode=cv2.BORDER_TRANSPARENT,
                )
                dstnodatamask = dstnodatamask != 0
                dstarray = np.full(np.r_[dst_fp.shape, array.shape[-1]], dst_nodata, array.dtype)
                cv2.remap(
                    array, mapx, mapy,
                    interpolation=interpolation,
                    borderMode=cv2.BORDER_TRANSPARENT,
                    dst=dstarray,
                )
                dstarray[dstnodatamask] = dst_nodata
        else:
            dstarray = None # pragma: no cover

        if mask is not None:
            if mask_mode == 'erode':
                dstmask = cv2.remap(
                    mask.astype('float32'), mapx, mapy,
                    interpolation=interpolation,
                    borderMode=cv2.BORDER_CONSTANT,
                    borderValue=0.,
                ) == 1.
            elif mask_mode == 'dilate':
                dstmask = cv2.remap(
                    mask.astype('float32'), mapx, mapy,
                    interpolation=interpolation,
                    borderMode=cv2.BORDER_CONSTANT,
                    borderValue=0.,
                ) != 0.
            else:
                assert False # pragma: no cover
        else:
            dstmask = None # pragma: no cover

        return dstarray, dstmask
