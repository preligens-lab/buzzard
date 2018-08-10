import os

import numpy as np

from osgeo import gdal

from buzzard._a_stored_raster import ABackStoredRaster
from buzzard._tools import conv
from buzzard import _tools

class ABackGDALRaster(ABackStoredRaster):
    """Abstract class defining the common implementation of all GDAL rasters"""

    # get_data implementation ******************************************************************* **
    def get_data(self, fp, band_ids, dst_nodata, interpolation):
        samplefp = self.build_sampling_footprint(fp, interpolation)
        if samplefp is None:
            return np.full(
                np.r_[fp.shape, len(band_ids)],
                dst_nodata,
                self.dtype
            )
        with self.acquire_driver_object() as gdal_ds:
            array = self.sample_bands_driver(samplefp, band_ids, gdal_ds)
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

    def sample_bands_driver(self, fp, band_ids, gdal_ds):
        rtlx, rtly = self.fp.spatial_to_raster(fp.tl)
        assert rtlx >= 0 and rtlx < self.fp.rsizex
        assert rtly >= 0 and rtly < self.fp.rsizey

        dstarray = np.empty(np.r_[fp.shape, len(band_ids)], self.dtype)
        for i, band_id in enumerate(band_ids):
            gdal_band = self._gdalband_of_band_id(gdal_ds, band_id)
            a = gdal_band.ReadAsArray(
                int(rtlx),
                int(rtly),
                int(fp.rsizex),
                int(fp.rsizey),
                buf_obj=dstarray[..., i],
            )
            if a is None: # pragma: no cover
                raise ValueError('Could not read array (gdal error: `{}`)'.format(
                    gdal.GetLastErrorMsg()
                ))
        return dstarray

    # set_data implementation ******************************************************************* **
    def set_data(self, array, fp, band_ids, interpolation, mask):
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
        with self.acquire_driver_object() as gdal_ds:
            for i, band_id in enumerate(band_ids):
                leftx, topy = self.fp.spatial_to_raster(fp.tl)
                gdalband = self._gdalband_of_band_id(gdal_ds, band_id)

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
    def fill(self, value, band_ids):
        with self.acquire_driver_object() as gdal_ds:
            for gdalband in [self._gdalband_of_band_id(gdal_ds, band_id) for band_id in band_ids]:
                gdalband.Fill(value)

    # Misc ************************************************************************************** **
    def acquire_driver_object(self): # pragma: no cover
        raise NotImplementedError('ABackGDALRaster.acquire_driver_object is virtual pure')

    @classmethod
    def create_file(cls, path, fp, dtype, band_count, band_schema, driver, options, wkt):
        """Create a raster datasource"""
        dr = gdal.GetDriverByName(driver)
        if os.path.isfile(path):
            err = dr.Delete(path)
            if err: # pragma: no cover
                raise Exception('Could not delete %s' % path)

        options = [str(arg) for arg in options]
        gdal_ds = dr.Create(
            path, fp.rsizex, fp.rsizey, band_count, conv.gdt_of_any_equiv(dtype), options
        )
        if gdal_ds is None: # pragma: no cover
            raise Exception('Could not create gdal dataset (%s)' % str(gdal.GetLastErrorMsg()).strip('\n'))
        if wkt is not None:
            gdal_ds.SetProjection(wkt)
        gdal_ds.SetGeoTransform(fp.gt)

        band_schema = _tools.sanitize_band_schema(band_schema, band_count)
        cls._apply_band_schema(gdal_ds, band_schema)

        gdal_ds.FlushCache()
        return gdal_ds

    @staticmethod
    def _gdalband_of_band_id(gdal_ds, id):
        """Convert a band identifier to a gdal band"""
        if isinstance(id, int):
            return gdal_ds.GetRasterBand(id)
        else:
            return gdal_ds.GetRasterBand(int(id.imag)).GetMaskBand()

    @staticmethod
    def _apply_band_schema(gdal_ds, band_schema):
        """Used on file creation"""
        if 'nodata' in band_schema:
            for i, val in enumerate(band_schema['nodata'], 1):
                if val is not None:
                    gdal_ds.GetRasterBand(i).SetNoDataValue(val)
        if 'interpretation' in band_schema:
            for i, val in enumerate(band_schema['interpretation'], 1):
                val = conv.gci_of_str(val)
                gdal_ds.GetRasterBand(i).SetColorInterpretation(val)
        if 'offset' in band_schema:
            for i, val in enumerate(band_schema['offset'], 1):
                gdal_ds.GetRasterBand(i).SetOffset(val)
        if 'scale' in band_schema:
            for i, val in enumerate(band_schema['scale'], 1):
                gdal_ds.GetRasterBand(i).SetScale(val)
        if 'mask' in band_schema:
            shared_bit = conv.gmf_of_str('per_dataset')
            for i, val in enumerate(band_schema['mask'], 1):
                val = conv.gmf_of_str(val)
                if val & shared_bit:
                    gdal_ds.CreateMaskBand(val)
                    break
            for i, val in enumerate(band_schema['mask'], 1):
                val = conv.gmf_of_str(val)
                if not val & shared_bit:
                    gdal_ds.GetRasterBand(i).CreateMaskBand(val)

    @staticmethod
    def _band_schema_of_gdal_ds(gdal_ds):
        """Used on file opening"""
        bands = [gdal_ds.GetRasterBand(i + 1) for i in range(gdal_ds.RasterCount)]
        return {
            'nodata': [band.GetNoDataValue() for band in bands],
            'interpretation': [conv.str_of_gci(band.GetColorInterpretation()) for band in bands],
            'offset': [band.GetOffset() if band.GetOffset() is not None else 0. for band in bands],
            'scale': [band.GetScale() if band.GetScale() is not None else 1. for band in bands],
            'mask': [conv.str_of_gmf(band.GetMaskFlags()) for band in bands],
        }
