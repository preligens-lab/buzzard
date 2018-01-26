""">>> help(RasterUtilsMixin)"""

import numbers

from buzzard._tools import conv

class RasterUtilsMixin(object):
    """Private mixin for the Raster class containing subroutines for raster attributes manipulations"""

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
