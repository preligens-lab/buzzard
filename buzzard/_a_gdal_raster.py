import os

import numpy as np

from osgeo import gdal

from buzzard._a_stored_raster import ABackStoredRaster
from buzzard._tools import conv, GDALErrorCatcher
from buzzard import _tools

class ABackGDALRaster(ABackStoredRaster):
    """Abstract class defining the common implementation of all GDAL rasters"""

    # get_data implementation ******************************************************************* **
    def get_data(self, fp, channel_ids, dst_nodata, interpolation):
        samplefp = self.build_sampling_footprint(fp, interpolation)
        if samplefp is None:
            return np.full(
                np.r_[fp.shape, len(channel_ids)],
                dst_nodata,
                self.dtype
            )
        with self.acquire_driver_object() as gdal_ds:
            array = self.sample_bands_driver(samplefp, channel_ids, gdal_ds)
        array = self.remap(
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
        return array

    def sample_bands_driver(self, fp, channel_ids, gdal_ds):
        rtlx, rtly = self.fp.spatial_to_raster(fp.tl)
        assert rtlx >= 0 and rtlx < self.fp.rsizex, '{} >= 0 and {} < {}'.format(rtlx, rtlx, self.fp.rsizex)
        assert rtly >= 0 and rtly < self.fp.rsizey, '{} >= 0 and {} < {}'.format(rtly, rtly, self.fp.rsizey)

        dstarray = np.empty(np.r_[fp.shape, len(channel_ids)], self.dtype)
        for i, channel_id in enumerate(channel_ids):
            gdal_band = gdal_ds.GetRasterBand(channel_id + 1)
            success, payload = GDALErrorCatcher(gdal_band.ReadAsArray, none_is_error=True)(
                int(rtlx),
                int(rtly),
                int(fp.rsizex),
                int(fp.rsizey),
                buf_obj=dstarray[..., i],
            )
            if not success: # pragma: no cover
                raise ValueError('Could not read array (gdal error: `{}`)'.format(
                    payload[1]
                ))
        return dstarray

    # set_data implementation ******************************************************************* **
    def set_data(self, array, fp, channel_ids, interpolation, mask):
        if not fp.share_area(self.fp):
            return
        if not fp.same_grid(self.fp) and mask is None:
            mask = np.ones(fp.shape, bool)

        dstfp = self.fp.intersection(fp)

        # Remap ****************************************************************
        ret = self.remap(
            fp,
            dstfp,
            array=array,
            mask=mask,
            src_nodata=self.nodata,
            dst_nodata=self.nodata or 0,
            mask_mode='erode',
            interpolation=interpolation,
        )
        if mask is not None:
            array, mask = ret
        else:
            array = ret
        del ret
        array = array.astype(self.dtype, copy=False)
        fp = dstfp
        del dstfp

        # Write ****************************************************************
        # TODO: Close all but 1 driver? Or let user do this
        with self.acquire_driver_object() as gdal_ds:
            for i, channel_id in enumerate(channel_ids):
                leftx, topy = self.fp.spatial_to_raster(fp.tl)
                gdalband = gdal_ds.GetRasterBand(channel_id + 1)

                for sl in _tools.slices_of_matrix(mask):
                    a = array[:, :, i][sl]
                    assert a.ndim == 2
                    x = int(sl[1].start + leftx)
                    y = int(sl[0].start + topy)
                    assert x >= 0
                    assert y >= 0
                    assert x + a.shape[1] <= self.fp.rsizex
                    assert y + a.shape[0] <= self.fp.rsizey
                    gdalband.WriteArray(a, x, y)

    # fill implementation *********************************************************************** **
    def fill(self, value, channel_ids):
        with self.acquire_driver_object() as gdal_ds:
            for gdalband in [gdal_ds.GetRasterBand(channel_id + 1) for channel_id in channel_ids]:
                gdalband.Fill(value)

    # Misc ************************************************************************************** **
    def acquire_driver_object(self): # pragma: no cover
        raise NotImplementedError('ABackGDALRaster.acquire_driver_object is virtual pure')

    @classmethod
    def create_file(cls, path, fp, dtype, channel_count, channels_schema, driver, options, wkt, ow):
        """Create a raster dataset"""

        # Step 0 - Find driver ********************************************** **
        success, payload = GDALErrorCatcher(gdal.GetDriverByName, none_is_error=True)(driver)
        if not success:
            raise ValueError('Could not find a driver named `{}` (gdal error: `{}`)'.format(
                driver, payload[1]
            ))
        dr = payload

        # Step 1 - Overwrite ************************************************ **
        if dr.ShortName != 'MEM' and os.path.exists(path):
            if ow:
                success, payload = GDALErrorCatcher(dr.Delete, nonzero_int_is_error=True)(path)
                if not success:
                    msg = 'Could not delete `{}` using driver `{}` (gdal error: `{}`)'.format(
                        path, dr.ShortName, payload[1]
                    )
                    raise RuntimeError(msg)
            else:
                msg = "Can't create `{}` with `ow=False` (overwrite) because file exist".format(
                    path,
                )
                raise RuntimeError(msg)

        # Step 2 - Create gdal_ds ******************************************* **
        options = [str(arg) for arg in options]
        success, payload = GDALErrorCatcher(dr.Create)(
            path, fp.rsizex, fp.rsizey, channel_count, conv.gdt_of_any_equiv(dtype), options
        )
        if not success: # pragma: no cover
            raise RuntimeError('Could not create `{}` using driver `{}` (gdal error: `{}`)'.format(
                path, dr.ShortName, payload[1]
            ))
        gdal_ds = payload

        # Step 3 - Set spatial reference ************************************ **
        if wkt is not None:
            gdal_ds.SetProjection(wkt)
        gdal_ds.SetGeoTransform(fp.gt)

        # Step 4 - Set channels schema ************************************** **
        channels_schema = _tools.sanitize_channels_schema(channels_schema, channel_count)
        cls._apply_channels_schema(gdal_ds, channels_schema)

        gdal_ds.FlushCache()
        return gdal_ds

    @staticmethod
    def _apply_channels_schema(gdal_ds, channels_schema):
        """Used on file creation"""
        if 'nodata' in channels_schema:
            for i, val in enumerate(channels_schema['nodata'], 1):
                if val is not None:
                    gdal_ds.GetRasterBand(i).SetNoDataValue(val)
        if 'interpretation' in channels_schema:
            for i, val in enumerate(channels_schema['interpretation'], 1):
                val = conv.gci_of_str(val)
                gdal_ds.GetRasterBand(i).SetColorInterpretation(val)
        if 'offset' in channels_schema:
            for i, val in enumerate(channels_schema['offset'], 1):
                gdal_ds.GetRasterBand(i).SetOffset(val)
        if 'scale' in channels_schema:
            for i, val in enumerate(channels_schema['scale'], 1):
                gdal_ds.GetRasterBand(i).SetScale(val)
        if 'mask' in channels_schema:
            shared_bit = conv.gmf_of_str('per_dataset')
            for i, val in enumerate(channels_schema['mask'], 1):
                val = conv.gmf_of_str(val)
                if val & shared_bit:
                    gdal_ds.CreateMaskBand(val)
                    break
            for i, val in enumerate(channels_schema['mask'], 1):
                val = conv.gmf_of_str(val)
                if not val & shared_bit:
                    gdal_ds.GetRasterBand(i).CreateMaskBand(val)

    @staticmethod
    def _channels_schema_of_gdal_ds(gdal_ds):
        """Used on file opening"""
        bands = [gdal_ds.GetRasterBand(i + 1) for i in range(gdal_ds.RasterCount)]
        return {
            'nodata': [band.GetNoDataValue() for band in bands],
            'interpretation': [conv.str_of_gci(band.GetColorInterpretation()) for band in bands],
            'offset': [band.GetOffset() if band.GetOffset() is not None else 0. for band in bands],
            'scale': [band.GetScale() if band.GetScale() is not None else 1. for band in bands],
            'mask': [conv.str_of_gmf(band.GetMaskFlags()) for band in bands],
        }
