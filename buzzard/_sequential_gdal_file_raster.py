
import uuid

from osgeo import gdal
import cv2

from buzzard._a_pooled_emissary_raster import *
from buzzard._tools import ANY

class SequentialGDALFileRaster(APooledEmissaryRaster):

    def __init__(self, ds, path, driver, open_options, mode):
        back_ds = ds._back

        # uuid = uuid.uuid4()
        # with back_ds.acquire(uuid, lambda: BackSequentialGDALFileRaster._open_file(path, driver, open_options, mode)) as gdal_ds:
        #     path = gdal_ds.GetDescription()
        #     driver = gdal_ds.GetDriver().ShortName
        #     fp_stored = Footprint(
        #         gt=gdal_ds.GetGeoTransform(),
        #         rsize=(gdal_ds.RasterXSize, gdal_ds.RasterYSize),
        #     )
        #     band_schema = Raster._band_schema_of_gdal_ds(gdal_ds)
        #     dtype = conv.dtype_of_gdt_downcast(gdal_ds.GetRasterBand(1).DataType)
        #     wkt_stored = gdal_ds.GetProjection()

        back = BackSequentialGDALFileRaster(
            back_ds,
            ...
        )

        super(SequentialGDALFileRaster, self).__init__(ds=ds, back=back)

class BackSequentialGDALFileRaster(ABackPooledEmissaryRaster):

    def __init__(self, back_ds,
                 # gdal_ds,
                 # path, driver, open_options, mode
    ):

        super(..., self).__init__(
            back_ds=back_ds,
            wkt_stored=wkt_stored,
            band_schema=band_schema,
            dtype=dtype,
            fp_stored=fp_stored,
            mode=mode,
            driver=driver,
            open_options=open_options,
            path=path,
            uuid=uuid,
        )
        functools.partial(
            _sample_bands, allocator=...,
        )

    def _build_sampling_footprint(self, fp):
        if not fp.share_area(dst_fp):
            return None
        if fp.same_grid(self.fp):
            fp = fp & self.fp
            assert fp.same_grid(self.fp)
            return fp
        if not self.back_ds.allow_interpolation:
            raise TODOTheRightMessage
        dilate_size = 4 * self.fp.pxsizex / fp.pxsizex # hyperparameter
        dilate_size = max(2, np.ceil(dilate_size)) # hyperparameter too
        fp = fp.dilate(dilate_size)
        fp = fp & self.fp
        return fp

    _REMAP_MASK_MODES = frozenset(['dilate', 'erode', ])
    _REMAP_INTERPOLATIONS = {
        'cv_area': cv2.INTER_AREA,
        'cv_nearest': cv2.INTER_NEAREST,
        'cv_linear': cv2.INTER_LINEAR,
        'cv_cubic': cv2.INTER_CUBIC,
        'cv_lanczos4': cv2.INTER_LANCZOS4,
    }

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
            dstarray = None

        if mask is not None:
            dstmask = np.full(dst_fp.shape, 0, mask.dtype)
            dstmask[dst_slice] = mask[src_slice]
        else:
            dstmask = None

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
            nninterpolation=interpolation=='cv_nearest',
        ) # At this point mapx/mapy are not really mapx/mapy any more, but who cares?
        interpolation = cls._REMAP_INTERPOLATIONS[interpolation]

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
                # TODO: Remove comment
                # print('nodata mask before:')
                # a = (array == src_nodata).astype('int')
                # a = np.atleast_3d(a)
                # for i in range(a.shape[-1]):
                #     print(f'channel {i}')
                #     print(a[..., i])

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
                print('dstarray', dstarray.shape, array.shape)
                dstarray[dstnodatamask] = dst_nodata
        else:
            dstarray = None

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
                assert False
        else:
            dstmask = None

        return dstarray, dstmask

    @classmethod
    def _remap(cls, src_fp, dst_fp, array, mask, src_nodata, dst_nodata, mask_mode, interpolation):
        """Function matching the signature of RasterRecipe@resample_array parameter

        Caveat
        ------
        For performance reasons, output array might share the same memory space as the input array.
        If a nodata conversion is then performed, the input array will be modified.
        """
        # Parameters cheking ******************************************************************** **
        arr_mode = array is not None, mask is not None
        fp_mode = (
            src_fp == dst_fp,
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
        if mask_mode not in cls._REMAP_MASK_MODES:
            raise ValueError('mask_mode should be one of {}'.format(
                _REMAP_MASK_MODES,
            ))

        # Check interpolation **************************************************
        if not (interpolation is None or interpolation in cls._REMAP_INTERPOLATIONS):
            raise ValueError('interpolation should be None or one of {}'.format(
                _REMAP_INTERPOLATIONS.keys(),
            ))

        # Remapping ***************************************************************************** **
        if fp_mode == (True, True, True):
            pass
        elif fp_mode == (False, True, True):
            array, mask = cls._remap_slice(
                src_fp, dst_fp,
                array, mask,
                src_nodata, dst_nodata,
            )
        elif fp_mode == (False, True, False):
            array, mask = cls._remap_copy(
                src_fp, dst_fp,
                array, mask,
                src_nodata, dst_nodata,
            )
        elif fp_mode == (False, False, ANY):
            array, mask = cls._remap_interpolate(
                src_fp, dst_fp,
                array, mask,
                src_nodata, dst_nodata,
                mask_mode, interpolation,
            )
        else:
            assert False

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
            assert False

    @staticmethod
    def _sample(fp, bands, prim_arrays, prim_fps, raster):
        """Function matching the signature of RasterRecipe@compute_array parameter"""
        # import six # TODO: Move
        # assert six.viewkeys(prim_arrays) == six.viewkeys(prim_fps)
        assert not prim_arrays
        assert not prim_fps
        assert fp.same_grid(raster.fp)

        with raster._back.back_ds.acquire(raster._back.uuid, raster._back._allocator) as gdal_obj:
            return _sample_gdal_ds(fp, bands, gdal_ds, raster)

        # if raster is None:
        #     @contextlib.contextmanager
        #     def _f():
        #         yield allocator()
        #     context = _f()
        # else:
        #     if isinstance(raster, (SequentialGDALFileRaster, ConcurrentGDALFileRaster)): # TODO! How do we get those pointers from back!?!?!?!?!?!
        #         context = raster.back_ds.acquire(raster.uuid, raster._allocator)
        #     elif isinstance(raster, GDALMemRaster):
        #         context = contextlib.contextmanager(lambda: iter([self._gdal_obj]))
        #     else:
        #         assert False

        # with context as gdal_ds:

    @staticmethod
    def _sample_gdal_ds(fp, bands, gdal_ds, raster):
        rtlx, rtly = raster.fp.spatial_to_raster(fp.tl)
        assert rtlx >= 0 and rtlx < raster.fp.rsizex
        assert rtly >= 0 and rtly < raster.fp.rsizey

        dstarray = np.empty(np.r_[fp.shape, len(bands)], raster.dtype)
        for i, band in enumerate(bands):
            gdal_band = _gdalband_of_index(gdal_ds, band)
            a = gdal_band.ReadAsArray(
                int(rtlx),
                int(rtly),
                int(fp.rsizex),
                int(fp.rsizey),
                buf_obj=dst_array[..., i],
            )
            if a is None:
                raise ValueError('Could not read array (gdal error: `{}`)'.format(
                    gdal.GetLastErrorMsg()
                ))
        return dstarray

    def get_data(self, fp, bands, outshape, dst_nodata, interpolation):
        samplefp = self._build_sampling_footprint(fp)
        array = self._sample(samplefp, bands, {}, {}, None)
        array = self._remap(
            samplefp,
            fp,
            array=array,
            mask=None,
            src_nodata=self.nodata,
            dst_nodata=dst_nodata,
            mask_mode='erode',
            interpolation=interpolation,
        )
        array = array.astype(self.dtype, copy=False)
        array = array.reshape(outshape)
        return array

    def set_data(self): pass

    def fill(self, value, bands):
        with self.back_ds.acquire_driver_object(self, self.uuid, self._allocator) as gdal_ds:
            for gdalband in [self._gdalband_of_index(gdal_ds, i) for i in bands]:
                gdalband.Fill(value)

    def delete(self):
        super(BackSequentialGDALFileRaster, self).delete()

        dr = gdal.GetDriverByName(self.driver)
        err = dr.Delete(self.path)
        if err:
            raise RuntimeError('Could not delete `{}` (gdal error: `{}`)'.format(
                self.path, str(gdal.GetLastErrorMsg()).strip('\n')
            ))

    def _allocator(self):
        return TODO._open_file(self.path, self.driver, self.open_options, self.mode)

    @staticmethod
    def _gdalband_of_index(gdal_ds, index):
        """Convert a band index to a gdal band"""
        if isinstance(index, int):
            return gdal_ds.GetRasterBand(index)
        else:
            return gdal_ds.GetRasterBand(int(index.imag)).GetMaskBand()

    @staticmethod
    def _open_file(path, driver, options, mode):
        """Open a raster dataset"""
        gdal_ds = gdal.OpenEx(
            path,
            conv.of_of_mode(mode) | conv.of_of_str('raster'),
            [driver],
            options,
        )
        if gdal_ds is None:
            raise ValueError('Could not open `{}` with `{}` (gdal error: `{}`)'.format(
                path, driver, str(gdal.GetLastErrorMsg()).strip('\n')
            ))
        return gdal_ds
