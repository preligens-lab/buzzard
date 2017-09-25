""">>> help(RasterRecipe)"""

from __future__ import division, print_function
import xml.etree.ElementTree as xml
import uuid
import logging
import functools

from osgeo import gdal

from buzzard._footprint import Footprint
from buzzard._tools import conv
from buzzard._raster import Raster
from buzzard._env import Env

LOGGER = logging.getLogger('buzzard')

class RasterRecipe(Raster):
    """Concrete class of recipe raster sources"""

    _callback_registry = {}

    @classmethod
    def _create_vrt(cls, fp, dtype, band_count, band_schema, sr):
        band_schema = cls._sanitize_band_schema(band_schema, band_count)
        vrt_xml = cls._create_vrt_xml_str(fp, dtype, band_count, band_schema, sr)
        gdal_ds = gdal.OpenEx(
            vrt_xml,
            conv.of_of_mode('w') | conv.of_of_str('raster'),
            ['VRT'],
        )
        if gdal_ds is None:
            raise Exception('Could not create gdal dataset (%s)' % gdal.GetLastErrorMsg())
        gdal_ds.FlushCache()
        return gdal_ds

    def __init__(self, ds, gdal_ds, fn_list):
        """Instanciated by DataSource class, instanciation by user is undefined"""
        self._uuid = gdal_ds.GetMetadataItem('UUID')
        self._callback_registry[self._uuid] = self
        Raster.__init__(self, ds, gdal_ds)
        self._fn_list = fn_list

    # pylint: disable=arguments-differ
    @functools.wraps(Raster.get_data)
    def get_data(self, *args, **kwargs):
        with Env(_gdal_trust_buzzard=True):
            return Raster.get_data(self, *args, **kwargs)

    @classmethod
    def _create_vrt_xml_str(cls, fp, dtype, band_count, band_schema, sr):
        uuidstr = str(uuid.uuid4())

        top = xml.Element('VRTDataset')
        top.set('rasterXSize', str(fp.rw))
        top.set('rasterYSize', str(fp.rh))

        if sr is not None:
            elt = xml.Element('SRS')
            elt.text = sr
            top.append(elt)

        elt = xml.Element('GeoTransform')
        elt.text = str(fp.gt.tolist())[1:-1].replace(' ', '')
        top.append(elt)

        elt = xml.Element('Metadata')
        elt2 = xml.Element('MDI')
        elt2.set('key', 'UUID')
        elt2.text = uuidstr
        elt.append(elt2)
        top.append(elt)

        for i in range(1, band_count + 1):
            meta = {
                'nodata': None,
                'interpretation': None,
                'offset': None,
                'scale': None,
                'mask': None,
            }
            meta.update({k: v[i - 1] for (k, v) in band_schema.items()})
            top.append(cls._create_vrt_band_xml(i, uuidstr, dtype, **meta))
        return xml.tostring(top, 'unicode')

    @staticmethod
    def _create_vrt_band_xml(i, uuidstr, dtype, nodata, interpretation, offset, scale, mask):
        band = xml.Element('VRTRasterBand')
        band.set('dataType', gdal.GetDataTypeName(conv.gdt_of_any_equiv(dtype)))
        band.set('band', str(i))
        band.set('subClass', 'VRTDerivedRasterBand')

        elt = xml.Element('PixelFunctionLanguage')
        elt.text = 'Python'
        band.append(elt)

        elt = xml.Element('PixelFunctionType')
        elt.text = 'buzzard._raster_recipe._pixel_function_entry_point'
        band.append(elt)

        elt = xml.Element('PixelFunctionArguments')
        elt.set('band_index', str(i))
        elt.set('proxy_uuid', uuidstr)
        band.append(elt)

        if nodata is not None:
            elt = xml.Element('NoDataValue')
            elt.text = str(nodata)
            band.append(elt)
        if interpretation is not None:
            elt = xml.Element('ColorInterp')
            elt.text = conv.str_of_gci(interpretation).replace('band', '')
            band.append(elt)
        if offset is not None:
            elt = xml.Element('Offset')
            elt.text = str(offset)
            band.append(elt)
        if scale is not None:
            elt = xml.Element('Scale')
            elt.text = str(scale)
            band.append(elt)
        if mask is not None:
            elt = xml.Element('MaskBand')
            elt.text = str(mask)
            band.append(elt)
        return band

# pylint: disable=too-many-arguments, unused-argument
def _pixel_function_entry_point(in_ar, out_ar, xoff, yoff, xsize, ysize, raster_xsize,
                                raster_ysize, radius, gt, band_index, proxy_uuid, **kwargs):
    if kwargs:
        raise RuntimeError('Too many aguments to _pixel_function_entry_point `{}`'.format(
            kwargs.keys()
        ))
    if in_ar:
        raise RuntimeError('in_ar should be empty')
    fp = Footprint(gt=gt, rsize=(raster_xsize, raster_ysize))
    fp = fp.clip(xoff, yoff, xoff + out_ar.shape[1], yoff + out_ar.shape[0])
    fp = fp.dilate(radius)
    proxy_uuid = proxy_uuid.decode('utf-8')
    band_index = int(band_index)
    prox = RasterRecipe._callback_registry[proxy_uuid]
    fn = prox._fn_list[band_index - 1]
    try:
        out_ar[:] = fn(fp)
    except:
        # This block changes the behavior of GDAL regarding exceptions stacktrace.
        raise
