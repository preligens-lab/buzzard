""">>> help(RemapMixin)"""

from __future__ import division, print_function

import numbers

import numpy as np
import cv2

_REMAP_MASK_MODES = frozenset(['dilate', 'erode', ])
_REMAP_INTERPOLATIONS = {
    'cv_area': cv2.INTER_AREA,
    'cv_nearest': cv2.INTER_NEAREST,
    'cv_linear': cv2.INTER_LINEAR,
    'cv_cubic': cv2.INTER_CUBIC,
    'cv_lanczos4': cv2.INTER_LANCZOS4,
}

class RemapMixin(object):
    """Footprint Mixin containing remap subroutine"""

    @staticmethod
    def _remap(src_fp, dst_fp, array=None, mask=None, nodata=None,
              mask_mode='dilate', interpolation='cv_area'):
        """Transform `array` or `mask` from `src_fp` referential to `dst_fp` referential.

        If dst_fp == src_fp, remap is the identity function
        Elif src_fp.same_grid(dst_fp), remap is a simple slicing function
        Else remap use interpolation

        If nodata missing, 0 is used.
        If dst_fp is partially or fully outside of src_fp, output is padded with nodata

        Parameters
        ----------
        src_fp: Footprint
            Source raster footprint
        dst_fp: Footprint
            Destination raster footprint
        array: None or numpy.ndarray (2 or 3 dimensions)
            Values to remap from the src_fp to the dst_fp
        mask: None or numpy.ndarray
            Mask values to remap from the src_fp to the dst_fp
        nodata: None or number
        mask_mode: one of ['dilate', 'erode']
            Interpolation behavior regarding masks
        interpolation: None or one of ['cv2_area', ]
            Interpolation algorithm to use
            if None: Don't allow interpolation

        Returns
        -------
        `dst_array` OR `dst_mask` OR `dst_array, dst_mask`

        """

        # Part 1/3 -- Parameters checking *********************************** **
        mode = (array is not None, mask is not None)
        if sum(mode) == (0, 0):
            raise ValueError('Provide at least array or mask')

        if mode[0]:
            if array.shape[:2] != tuple(src_fp.shape):
                raise ValueError("Array's shape begin with {} instead of {}".format(
                    array.shape[:2], tuple(src_fp.shape),
                ))
            if array.ndim == 2:
                array = array[:, :, np.newaxis]
                outshape = dst_fp.shape
            elif array.ndim == 3:
                outshape = np.r_[dst_fp.shape, array.shape[-1]]
            else:
                raise ValueError('Array should have 2 or 3 dimensions instead of {}'.format(
                    array.ndim,
                ))

        if mode[1]:
            if mask.shape != tuple(src_fp.shape):
                raise ValueError("Array's shape is {} instead of {}".format(
                    mask.shape, tuple(src_fp.shape),
                ))

        if mask_mode not in _REMAP_MASK_MODES:
            raise ValueError('mask_mode should be one of {}'.format(
                _REMAP_MASK_MODES,
            ))

        if not (interpolation is None or interpolation in _REMAP_INTERPOLATIONS):
            raise ValueError('interpolation should be None or one of {}'.format(
                _REMAP_INTERPOLATIONS.keys(),
            ))

        if not (nodata is None or isinstance(nodata, numbers.Real)):
            raise ValueError('nodata should be None or a number')

        # Part 2/3 -- Remapping ********************************************* **
        if dst_fp == src_fp:
            dstarray, dstmask = array, mask
        elif dst_fp.same_grid(src_fp):
            dstarray, dstmask = RemapMixin._remap_same_grid(src_fp, dst_fp, array, mask, nodata)
        else:
            if interpolation is None:
                raise ValueError('trying to remap with interpolation but interpolation is None')
            dstarray, dstmask = RemapMixin._remap_any(
                src_fp, dst_fp, array, mask, nodata, mask_mode, interpolation
            )

        # Part 3/3 -- Return ************************************************ **
        if mode == (1, 1):
            return dstarray.reshape(outshape), dstmask
        elif mode == (1, 0):
            return dstarray.reshape(outshape)
        elif mode == (0, 1):
            return dstmask
        else:
            assert False

    @staticmethod
    def _remap_same_grid(src_fp, dst_fp, array, mask, nodata):
        """
        `src_fp` to `dst_fp` requires a slicing
            If they have the same rotation, resolution and alignment
        """
        def _remap_band(dim):
            arr = array[:, :, dim]
            dstarr = np.full(dst_fp.shape, nodata or 0, arr.dtype)
            dstarr[src_fp.slice_in(dst_fp, clip=True)] = arr[dst_fp.slice_in(src_fp, clip=True)]
            return dstarr

        dstmask, dstarray = None, None
        if array is not None:
            dstbands = list(map(_remap_band, range(array.shape[-1])))
            dstarray = np.stack(dstbands, -1)
        if mask is not None:
            dstmask = np.full(dst_fp.shape, 0, mask.dtype)
            dstmask[src_fp.slice_in(dst_fp, clip=True)] = mask[dst_fp.slice_in(src_fp, clip=True)]
        return dstarray, dstmask


    @staticmethod
    def _remap_any(src_fp, dst_fp, array, mask, nodata, mask_mode, interpolation):
        """
        `src_fp` to `dst_fp` requires a remapping
            - If they have the same rotation and resolution but different alignment
            - Or if they have same rotation but different resolution
            - Or if they have different rotation
        """

        def _remap_band(dim):
            arr = array[:, :, dim]
            if nodata is None:
                dstband = cv2.remap(
                    arr, mapx, mapy,
                    interpolation=interpolation,
                    borderMode=cv2.BORDER_CONSTANT,
                    borderValue=0.,
                )
            else:
                border = 4
                arr = np.pad(arr, border, mode='constant', constant_values=nodata)
                dstnodatamask = cv2.remap(
                    (arr == nodata).astype('float32'), mapx + border, mapy + border,
                    interpolation=interpolation,
                    borderMode=cv2.BORDER_CONSTANT,
                    borderValue=1.,
                ) != 0.
                dstband = cv2.remap(
                    arr, mapx + border, mapy + border,
                    interpolation=interpolation,
                    borderMode=cv2.BORDER_CONSTANT,
                    borderValue=nodata,
                )
                dstband[dstnodatamask] = nodata
            return dstband

        if array is not None and array.dtype in [np.dtype('float64'), np.dtype('bool')]:
            raise ValueError(
                'Type %s not handled by cv2.remap, (is interpolation enabled in DataSource?)' % (
                    repr(array.dtype)
                )
            ) # pragma: no cover

        mapx, mapy = dst_fp.meshgrid_raster_in(src_fp, dtype='float32')
        interpolation = _REMAP_INTERPOLATIONS[interpolation]

        dstmask, dstarray = None, None

        if array is not None:
            dstbands = list(map(_remap_band, range(array.shape[-1])))
            dstarray = np.stack(dstbands, -1)

        if mask is not None:
            border = 4
            if mask_mode == 'erode':
                mask = np.pad(mask, border, mode='constant', constant_values=0)
                dstmask = cv2.remap(
                    mask.astype('float32'), mapx + border, mapy + border,
                    interpolation=interpolation,
                    borderMode=cv2.BORDER_CONSTANT,
                    borderValue=0.,
                ) == 1.
            elif mask_mode == 'dilate':
                mask = np.pad(mask, border, mode='constant', constant_values=1)
                dstmask = cv2.remap(
                    mask.astype('float32'), mapx + border, mapy + border,
                    interpolation=interpolation,
                    borderMode=cv2.BORDER_CONSTANT,
                    borderValue=0.,
                ) != 0.
            else:
                assert False
        return dstarray, dstmask
