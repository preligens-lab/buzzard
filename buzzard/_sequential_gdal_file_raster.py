
import uuid
import os

from osgeo import gdal
import cv2

from buzzard._a_pooled_emissary_raster import *
from buzzard._tools import ANY, conv

class SequentialGDALFileRaster(APooledEmissaryRaster):

    def __init__(self, ds, allocator, open_options, mode):
        back = BackSequentialGDALFileRaster(
            ds._back, allocator, open_options, mode,
        )

        super(SequentialGDALFileRaster, self).__init__(ds=ds, back=back)

class BackSequentialGDALFileRaster(ABackPooledEmissaryRaster):

    def __init__(self, back_ds, allocator, open_options, mode):
        uid = uuid.uuid4()

        with back_ds.acquire_driver_object(uid, allocator) as gdal_ds:
            path = gdal_ds.GetDescription()
            driver = gdal_ds.GetDriver().ShortName
            fp_stored = Footprint(
                gt=gdal_ds.GetGeoTransform(),
                rsize=(gdal_ds.RasterXSize, gdal_ds.RasterYSize),
            )
            band_schema = self._band_schema_of_gdal_ds(gdal_ds)
            dtype = conv.dtype_of_gdt_downcast(gdal_ds.GetRasterBand(1).DataType)
            wkt_stored = gdal_ds.GetProjection()

        super(BackSequentialGDALFileRaster, self).__init__(
            back_ds=back_ds,
            wkt_stored=wkt_stored,
            band_schema=band_schema,
            dtype=dtype,
            fp_stored=fp_stored,
            mode=mode,
            driver=driver,
            open_options=open_options,
            path=path,
            uid=uid,
        )

    def _sample(self, fp, band_ids):
        assert fp.same_grid(self.fp)
        with self.back_ds.acquire_driver_object(self.uid, self._allocator) as gdal_ds:
            return self.get_data_driver(fp, band_ids, gdal_ds)

    def get_data_driver(self, fp, band_ids, driver_obj):
        rtlx, rtly = self.fp.spatial_to_raster(fp.tl)
        assert rtlx >= 0 and rtlx < self.fp.rsizex
        assert rtly >= 0 and rtly < self.fp.rsizey

        dstarray = np.empty(np.r_[fp.shape, len(band_ids)], self.dtype)
        for i, band_id in enumerate(band_ids):
            gdal_band = self._gdalband_of_band_id(driver_obj, band_id)
            a = gdal_band.ReadAsArray(
                int(rtlx),
                int(rtly),
                int(fp.rsizex),
                int(fp.rsizey),
                buf_obj=dstarray[..., i],
            )
            if a is None:
                raise ValueError('Could not read array (gdal error: `{}`)'.format(
                    gdal.GetLastErrorMsg()
                ))
        return dstarray

    def get_data(self, fp, band_ids, outshape, dst_nodata, interpolation):
        samplefp = self.build_sampling_footprint(fp, interpolation)
        if samplefp is None:
            assert False, 'todo'
        array = self._sample(samplefp, band_ids)
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
        array = array.reshape(outshape)
        return array

    def set_data(self): pass

    def fill(self, value, band_ids):
        with self.back_ds.acquire_driver_object(self.uid, self._allocator) as gdal_ds:
            for gdalband in [self._gdalband_of_band_id(gdal_ds, band_id) for band_id in band_ids]:
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
        return self._open_file(self.path, self.driver, self.open_options, self.mode)

    @staticmethod
    def _gdalband_of_band_id(gdal_ds, id):
        """Convert a band identifier to a gdal band"""
        if isinstance(id, int):
            return gdal_ds.GetRasterBand(id)
        else:
            return gdal_ds.GetRasterBand(int(id.imag)).GetMaskBand()

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

    @classmethod
    def _create_file(cls, path, fp, dtype, band_count, band_schema, driver, options, sr):
        """Create a raster datasource"""
        dr = gdal.GetDriverByName(driver)
        if os.path.isfile(path):
            err = dr.Delete(path)
            if err:
                raise Exception('Could not delete %s' % path)

        options = [str(arg) for arg in options]
        gdal_ds = dr.Create(
            path, fp.rsizex, fp.rsizey, band_count, conv.gdt_of_any_equiv(dtype), options
        )
        if gdal_ds is None:
            raise Exception('Could not create gdal dataset (%s)' % str(gdal.GetLastErrorMsg()).strip('\n'))
        if sr is not None:
            gdal_ds.SetProjection(osr.GetUserInputAsWKT(sr))
        gdal_ds.SetGeoTransform(fp.gt)

        band_schema = cls._sanitize_band_schema(band_schema, band_count)
        cls._apply_band_schema(gdal_ds, band_schema)

        gdal_ds.FlushCache()
        return gdal_ds

    _BAND_SCHEMA_PARAMS = {
        'nodata', 'interpretation', 'offset', 'scale', 'mask',
    }

    @staticmethod
    def _apply_band_schema(gdal_ds, band_schema):
        """Used on file creation"""
        if 'nodata' in band_schema:
            for i, val in enumerate(band_schema['nodata'], 1):
                if val is not None:
                    gdal_ds.GetRasterBand(i).SetNoDataValue(val)
        if 'interpretation' in band_schema:
            for i, val in enumerate(band_schema['interpretation'], 1):
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
                if val & shared_bit:
                    gdal_ds.CreateMaskBand(val)
                    break
            for i, val in enumerate(band_schema['mask'], 1):
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

    @classmethod
    def _sanitize_band_schema(cls, band_schema, band_count):
        """Used on file/recipe creation"""
        import numbers # TODO: move
        ret = {}

        def _test_length(val, name):
            count = len(val)
            if count > band_count:
                raise ValueError('Too many values provided for %s (%d instead of %d)' % (
                    name, count, band_count
                ))
            elif count < band_count:
                raise ValueError('Not enough values provided for %s (%d instead of %d)' % (
                    name, count, band_count
                ))

        if band_schema is None:
            return {}
        diff = set(band_schema.keys()) - cls._BAND_SCHEMA_PARAMS
        if diff:
            raise ValueError('Unknown band_schema keys `%s`' % diff)

        def _normalize_multi_layer(name, val, type_, cleaner, default):
            if val is None:
                for _ in range(band_count):
                    yield default
            elif isinstance(val, type_):
                val = cleaner(val)
                for _ in range(band_count):
                    yield val
            else:
                _test_length(val, name)
                for elt in val:
                    if elt is None:
                        yield default
                    elif isinstance(elt, type_):
                        yield cleaner(elt)
                    else:
                        raise ValueError('`{}` cannot use value `{}`'.format(name, elt))

        if 'nodata' in band_schema:
            ret['nodata'] = list(_normalize_multi_layer(
                'nodata',
                band_schema['nodata'],
                numbers.Number,
                lambda val: float(val),
                None,
            ))

        if 'interpretation' in band_schema:
            val = band_schema['interpretation']
            if isinstance(val, str):
                ret['interpretation'] = [conv.gci_of_str(val)] * band_count
            else:
                _test_length(val, 'nodata')
                ret['interpretation'] = [conv.gci_of_str(elt) for elt in val]

        if 'offset' in band_schema:
            ret['offset'] = list(_normalize_multi_layer(
                'offset',
                band_schema['offset'],
                numbers.Number,
                lambda val: float(val),
                0.,
            ))

        if 'scale' in band_schema:
            ret['scale'] = list(_normalize_multi_layer(
                'scale',
                band_schema['scale'],
                numbers.Number,
                lambda val: float(val),
                1.,
            ))

        if 'mask' in band_schema:
            val = band_schema['mask']
            if isinstance(val, str):
                ret['mask'] = [conv.gmf_of_str(val)] * band_count
            else:
                _test_length(val, 'mask')
                ret['mask'] = [conv.gmf_of_str(elt) for elt in val]
                shared_bit = conv.gmf_of_str('per_dataset')
                shared = [elt for elt in ret['mask'] if elt & shared_bit]
                if len(set(shared)) > 1:
                    raise ValueError('per_dataset mask must be shared with same flags')

        return ret
