"""Conversions between gdal types python representations

http://www.gdal.org/gdal_8h.html
http://www.gdal.org/ogr__core_8h.html
"""

import datetime
import json
import numbers
import collections

import shapely.wkt as sw
from osgeo import ogr, gdal
import numpy as np

# WKB (Well Known Bytes) <-> str **************************************************************** **
_WKBGEOM_KEYS = [
    key for key in dir(ogr)
    if (key.startswith('wkb') and
        isinstance(getattr(ogr, key), numbers.Integral) and
        key not in {'wkbXDR', 'wkbNDR'}
       )
]

_STR_OF_WKBGEOM = {
    getattr(ogr, key): key[3:]
    for key in _WKBGEOM_KEYS
}

_WKBGEOM_OF_STR = dict(
    [(v, k) for k, v in _STR_OF_WKBGEOM.items()] +
    [(v.lower(), k) for k, v in _STR_OF_WKBGEOM.items()]
)

def wkbgeom_of_str(str_):
    return _WKBGEOM_OF_STR[str_]

def str_of_wkbgeom(wkbgeom):
    return _STR_OF_WKBGEOM[wkbgeom]

# OGR Geometry <-> shapely/geojson-coordinates ************************************************** **
def ogr_of_shapely(geom):
    return ogr.CreateGeometryFromWkt(geom.wkt)

def ogr_of_coordinates(geom, type_):

    def _to_list(obj):
        if isinstance(obj, np.ndarray):
            return obj.tolist()
        elif isinstance(obj, collections.Iterable):
            return [_to_list(elt) for elt in obj]
        else:
            return obj

    d = {
        'type': type_,
        'coordinates': _to_list(geom),
    }
    d = json.dumps(d)
    d = ogr.CreateGeometryFromJson(d)
    return d

def shapely_of_ogr(geom):
    return sw.loads(geom.ExportToWkt())

def coordinates_of_ogr(geom):
    return json.loads(geom.ExportToJson())['coordinates']

# OFT (OGR Field Type) <-> type/str ************************************************************* **
# Read to parse user choices in create_vector
# Contains keys
#   - all lowercast ogr.OFT*
#   - other strings that are not interpretable by numpy
_OFT_OF_STR = {
    'binary': ogr.OFTBinary,
    'date': ogr.OFTDate,
    'datetime': ogr.OFTDateTime,
    'time': ogr.OFTTime,
    'integer': ogr.OFTInteger,
    'integer64': ogr.OFTInteger64,
    'real': ogr.OFTReal,
    'string': ogr.OFTString,
    'integer64list': ogr.OFTInteger64List,
    'int list': ogr.OFTInteger64List,
    'integerlist': ogr.OFTIntegerList,
    'reallist': ogr.OFTRealList,
    'float list': ogr.OFTRealList,
    'str list': ogr.OFTStringList,
    'stringlist': ogr.OFTStringList,
}

# Read to transform ogr.FieldDefn.type to field dict
# Contains keys
#   - All ogr.OFT*
_STR_OF_OFT = {
    ogr.OFTBinary: 'binary',
    ogr.OFTDate: 'date',
    ogr.OFTDateTime: 'datetime',
    ogr.OFTInteger64: 'integer64',
    ogr.OFTInteger64List: 'integer64list',
    ogr.OFTInteger: 'integer',
    ogr.OFTIntegerList: 'integerlist',
    ogr.OFTReal: 'real',
    ogr.OFTRealList: 'reallist',
    ogr.OFTString: 'string',
    ogr.OFTStringList: 'stringlist',
    ogr.OFTTime: 'time',
}

# Read to parse user choices in create_vector
# Contains keys
#   - All non-numpy types
#   - All numpy types
_OFT_OF_TYPE = {
    bytes: ogr.OFTBinary,
    datetime.datetime: ogr.OFTDateTime,
    np.dtype('datetime64'): ogr.OFTDateTime,
    int: ogr.OFTInteger64,
    np.dtype('int32'): ogr.OFTInteger,
    np.dtype('int64'): ogr.OFTInteger64,
    float: ogr.OFTReal,
    np.dtype('float32'): ogr.OFTReal,
    np.dtype('float64'): ogr.OFTReal,
    str: ogr.OFTString,
    np.dtype('str'): ogr.OFTString,
}

# Read to convert ogr.Field to user type
# Contains keys
#   - All ogr.OFT*
_TYPE_OF_OFT = {
    ogr.OFTBinary: np.bytes_,
    ogr.OFTDate: str,
    ogr.OFTDateTime: str,
    ogr.OFTTime: str,
    ogr.OFTInteger64: int,
    ogr.OFTInteger64List: np.int64,
    ogr.OFTInteger: int,
    ogr.OFTIntegerList: np.int32,
    ogr.OFTReal: float,
    ogr.OFTRealList: np.float64,
    ogr.OFTString: str,
    ogr.OFTStringList: np.str_,
}

def oft_of_any(obj):
    if obj in _OFT_OF_STR:
        return _OFT_OF_STR[obj]
    if obj in _OFT_OF_TYPE:
        return _OFT_OF_TYPE[obj]
    if np.dtype(obj) in _OFT_OF_TYPE:
        return _OFT_OF_TYPE[np.dtype(obj)]
    raise ValueError('Unknown type')

def str_of_oft(oft):
    return _STR_OF_OFT[oft]

def type_of_oftstr(str_):
    return _TYPE_OF_OFT[_OFT_OF_STR[str_]]

# OF (Open Flag) <-> str ************************************************************************ **
_OF_OF_STR = {
    'all': gdal.OF_ALL,
    'gnm': gdal.OF_GNM,
    'raster': gdal.OF_RASTER,
    'readonly': gdal.OF_READONLY,
    'shared': gdal.OF_SHARED,
    'update': gdal.OF_UPDATE,
    'vector': gdal.OF_VECTOR,
    'verbose_error': gdal.OF_VERBOSE_ERROR,
}

def of_of_str(str_):
    return _OF_OF_STR[str_]

_OF_OF_MODE = {
    'r': gdal.OF_READONLY,
    'w': gdal.OF_UPDATE,
}

def of_of_mode(mode):
    return _OF_OF_MODE[mode]

# GCI (GDAL Color Interpretation) <-> str ******************************************************* **
_STR_OF_GCI = {
    gdal.GCI_Undefined: 'undefined',
    gdal.GCI_GrayIndex: 'grayindex',
    gdal.GCI_PaletteIndex: 'paletteindex',
    gdal.GCI_RedBand: 'redband',
    gdal.GCI_GreenBand: 'greenband',
    gdal.GCI_BlueBand: 'blueband',
    gdal.GCI_AlphaBand: 'alphaband',
    gdal.GCI_HueBand: 'hueband',
    gdal.GCI_SaturationBand: 'saturationband',
    gdal.GCI_LightnessBand: 'lightnessband',
    gdal.GCI_CyanBand: 'cyanband',
    gdal.GCI_MagentaBand: 'magentaband',
    gdal.GCI_YellowBand: 'yellowband',
    gdal.GCI_BlackBand: 'blackband',
}

_GCI_OF_STR = {v: k for (k, v) in _STR_OF_GCI.items()}

def gci_of_str(str_):
    return _GCI_OF_STR[str_]

def str_of_gci(gci):
    return _STR_OF_GCI[gci]

# GMF (GDAL Mask Flags) <-> ********************************************************************* **
_STR_OF_GMF = {
    gdal.GMF_ALL_VALID: 'all_valid',
    gdal.GMF_PER_DATASET: 'per_dataset',
    gdal.GMF_ALPHA: 'alpha',
    gdal.GMF_NODATA: 'nodata',
}

_GMF_OF_STR = {v: k for (k, v) in _STR_OF_GMF.items()}

def gmf_of_str(str_):
    l = str_.replace(',', ' ').split(' ')
    l = [v for v in l if len(v)]

    diff = set(l) - set(_GMF_OF_STR.keys())
    if diff:
        raise ValueError('Unknown gmf %s' % diff)
    return sum(_GMF_OF_STR[elt] for elt in l)

def str_of_gmf(gmf):
    l = []
    check = 0
    for str_, val in _GMF_OF_STR.items():
        if gmf & val:
            check |= val
            l += [str_]

    if check != gmf:
        raise ValueError('Unknown gmf bits %#x' % gmf)
    return ','.join(l)
