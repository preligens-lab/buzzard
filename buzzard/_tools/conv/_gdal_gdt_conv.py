"""Conversions between numpy dtypes and gdal GDTs

All types are not available on all platforms, hence the `_eval_filter_dict_key({` declarations.
"""

from osgeo import gdal
import numpy as np

DTYPE_OF_NAME = {np.dtype(v).name: np.dtype(v) for v in np.typeDict.values()}

def _eval_filter_dict_key(d):
    return {
        DTYPE_OF_NAME[k]: v
        for (k, v) in d.items()
        if k in DTYPE_OF_NAME
    }

def _eval_filter_dict_value(d):
    return {
        k: DTYPE_OF_NAME[v]
        for (k, v) in d.items()
        if v in DTYPE_OF_NAME
    }

# DTYPE -> GDT CONVERSIONS ********** **
_GDT_OF_DTYPE_EQUIV = _eval_filter_dict_key({
    'bool': gdal.GDT_Byte,
    'uint8': gdal.GDT_Byte,

    'float32': gdal.GDT_Float32,
    'float64': gdal.GDT_Float64,
    'int16': gdal.GDT_Int16,
    'int32': gdal.GDT_Int32,
    'uint16': gdal.GDT_UInt16,
    'uint32': gdal.GDT_UInt32,

    'complex128': gdal.GDT_CFloat64,
    'complex64': gdal.GDT_CFloat32,
})

_GDT_OF_DTYPE_UPCAST = _eval_filter_dict_key({
    'float16': gdal.GDT_Float32, # 16 to 32 bits
    'int8': gdal.GDT_Int16, # 8 to 16 bits
})

_GDT_OF_DTYPE_DOWNCAST = _eval_filter_dict_key({
    'float128': gdal.GDT_Float64, # 128 to 64 bits
    'complex256': gdal.GDT_CFloat64, # 128 to 64 bits
    'uint64': gdal.GDT_CFloat64, # 64 to 53 bits
    'int64': gdal.GDT_CFloat64, # 64 to 53 bits
})

_GDT_OF_DTYPE_NOEQUIV = set([
    np.dtype(np.datetime64),
    np.dtype(np.object_),
    np.dtype(np.str_),
    np.dtype(np.timedelta64),
    np.dtype(np.void),
])

# GDT -> DTYPE CONVERSIONS ********** **
_DTYPE_OF_GDT_EQUIV = _eval_filter_dict_value({
    gdal.GDT_Byte: 'uint8',
    gdal.GDT_Int16: 'int16',
    gdal.GDT_Int32: 'int32',
    gdal.GDT_UInt16: 'uint16',
    gdal.GDT_UInt32: 'uint32',
    gdal.GDT_Float32: 'float32',
    gdal.GDT_Float64: 'float64',
    gdal.GDT_CFloat32: 'complex64',
    gdal.GDT_CFloat64: 'complex128',
})

_DTYPE_OF_GDT_UPCAST = _eval_filter_dict_value({
    gdal.GDT_CInt16: 'complex64', # 16 to 24 bits
    gdal.GDT_CInt32: 'complex128', # 32 to 53 bits
})

_DTYPE_OF_GDT_DOWNCAST = dict()

_DTYPE_OF_GDT_NOEQUIV = set()

# STRINGIFICATION ******************* **
def _str_of_gdt(gdt):
    return {
        gdal.GDT_Byte: 'GDT_Byte',
        gdal.GDT_Int16: 'GDT_Int16',
        gdal.GDT_Int32: 'GDT_Int32',
        gdal.GDT_UInt16: 'GDT_UInt16',
        gdal.GDT_UInt32: 'GDT_UInt32',
        gdal.GDT_Float32: 'GDT_Float32',
        gdal.GDT_Float64: 'GDT_Float64',
        gdal.GDT_CFloat32: 'GDT_CFloat32',
        gdal.GDT_CFloat64: 'GDT_CFloat64',
        gdal.GDT_CInt16: 'GDT_CInt16',
        gdal.GDT_CInt32: 'GDT_CInt32',
    }[gdt]

def _str_of_dtype(dtype):
    return str(dtype)

# NORMALIZATION ********************* **
def _to_gdt_opt(gdt):
    """Unchecked GDT -> {None | GDT}"""
    if gdt in _DTYPE_OF_GDT_EQUIV:
        return gdt
    if gdt in _DTYPE_OF_GDT_UPCAST:
        return gdt
    if gdt in _DTYPE_OF_GDT_DOWNCAST:
        return gdt
    if gdt in _DTYPE_OF_GDT_NOEQUIV:
        return gdt
    return None

def _to_dtype_opt(dtype):
    """Unchecked dtype -> {None | dtype}"""
    try:
        dtype = np.dtype(dtype)
    except TypeError:
        return None
    else:
        if dtype in _GDT_OF_DTYPE_EQUIV:
            return dtype
        if dtype in _GDT_OF_DTYPE_UPCAST:
            return dtype
        if dtype in _GDT_OF_DTYPE_DOWNCAST:
            return dtype
        if dtype in _GDT_OF_DTYPE_NOEQUIV:
            return dtype
        return None

# PUBLIC **************************** **
def gdt_of_dtype_equiv(dtype):
    """
    Convert a dtype (numpy data type) to an equivalent GDT (GDAL type).
    If impossible an exception is raised.
    """
    param = dtype
    dtype = _to_dtype_opt(dtype)
    if dtype is None:
        raise ValueError('`%s` is not a dtype' % param)
    gdt = _GDT_OF_DTYPE_EQUIV.get(dtype)
    if gdt is None:
        raise ValueError('`%s` has no equivalent gdt' % _str_of_dtype(dtype))
    return gdt

def gdt_of_dtype_upcast(dtype):
    """
    Convert a dtype (numpy data type) to an equivalent or larger GDT (GDAL type).
    If impossible an exception is raised.
    """
    param = dtype
    dtype = _to_dtype_opt(dtype)
    if dtype is None:
        raise ValueError('`%s` is not a dtype' % param)
    gdt = _GDT_OF_DTYPE_EQUIV.get(dtype) or _GDT_OF_DTYPE_UPCAST.get(dtype)
    if gdt is None:
        raise ValueError('`%s` has no equivalent or upcast gdt' % _str_of_dtype(dtype))
    return gdt

def gdt_of_dtype_downcast(dtype):
    """
    Convert a dtype (numpy data type) to an equivalent or larger or smaller GDT (GDAL type).
    If impossible an exception is raised.
    """
    param = dtype
    dtype = _to_dtype_opt(dtype)
    if dtype is None:
        raise ValueError('`%s` is not a dtype' % param)
    gdt = (
        _GDT_OF_DTYPE_EQUIV.get(dtype) or
        _GDT_OF_DTYPE_UPCAST.get(dtype) or
        _GDT_OF_DTYPE_DOWNCAST.get(dtype)
    )
    if gdt is None:
        raise ValueError('`%s` has no equivalent or upcast or downcast gdt' % _str_of_dtype(dtype))
    return gdt

def dtype_of_gdt_equiv(gdt):
    """
    Convert a GDT (GDAL type) to an equivalent dtype (numpy data type).
    If impossible an exception is raised.
    """
    param = gdt
    gdt = _to_gdt_opt(gdt)
    if gdt is None:
        raise ValueError('`%s` is not a gdt' % param)
    dtype = _DTYPE_OF_GDT_EQUIV.get(gdt)
    if dtype is None:
        raise ValueError('`%s` has no equivalent dtype' % _str_of_gdt(gdt))
    return dtype

def dtype_of_gdt_upcast(gdt):
    """
    Convert a GDT (GDAL type) to an equivalent or larger dtype (numpy data type).
    If impossible an exception is raised.
    """
    param = gdt
    gdt = _to_gdt_opt(gdt)
    if gdt is None:
        raise ValueError('`%s` is not a gdt' % param)
    dtype = _DTYPE_OF_GDT_EQUIV.get(gdt)
    if dtype is None:
        dtype = _DTYPE_OF_GDT_UPCAST.get(gdt)
    if dtype is None:
        raise ValueError('`%s` has no equivalent or upcast dtype' % _str_of_gdt(gdt))
    return dtype

def dtype_of_gdt_downcast(gdt):
    """
    Convert a GDT (GDAL type) to an equivalent or larger or smaller dtype (numpy data type).
    If impossible an exception is raised.
    """
    param = gdt
    gdt = _to_gdt_opt(gdt)
    if gdt is None:
        raise ValueError('`%s` is not a gdt' % param)
    dtype = _DTYPE_OF_GDT_EQUIV.get(gdt)
    if dtype is None:
        dtype = _DTYPE_OF_GDT_UPCAST.get(gdt)
    if dtype is None:
        dtype = _DTYPE_OF_GDT_DOWNCAST.get(gdt)
    if dtype is None:
        raise ValueError('`%s` has no equivalent or upcast or downcast dtype' % _str_of_gdt(gdt))
    return dtype

def gdt_of_any_equiv(obj):
    """
    Convert a GDT (GDAL type) or a dtype (numpy data type) to an equivalent GDT.
    If impossible an exception is raised.
    """
    gdt = _to_gdt_opt(obj)
    if gdt is not None:
        return gdt
    dtype = _to_dtype_opt(obj)
    if dtype is not None:
        return gdt_of_dtype_equiv(obj)
    raise ValueError('Unknown type')

def gdt_of_any_upcast(obj):
    """
    Convert a GDT (GDAL type) or a dtype (numpy data type) to an equivalent or larger GDT.
    If impossible an exception is raised.
    """
    gdt = _to_gdt_opt(obj)
    if gdt is not None:
        return gdt
    dtype = _to_dtype_opt(obj)
    if dtype is not None:
        return gdt_of_dtype_upcast(obj)
    raise ValueError('Unknown type')

def gdt_of_any_downcast(obj):
    """
    Convert a GDT (GDAL type) or a dtype (numpy data type) to an equivalent or larger or smaller
    GDT.
    If impossible an exception is raised.
    """
    gdt = _to_gdt_opt(obj)
    if gdt is not None:
        return gdt
    dtype = _to_dtype_opt(obj)
    if dtype is not None:
        return gdt_of_dtype_downcast(obj)
    raise ValueError('Unknown type')

def dtype_of_any_equiv(obj):
    """
    Convert a GDT (GDAL type) or a dtype (numpy data type) to an equivalent dtype.
    If impossible an exception is raised.
    """
    dtype = _to_dtype_opt(obj)
    if dtype is not None:
        return dtype
    gdt = _to_gdt_opt(obj)
    if gdt is not None:
        return dtype_of_gdt_equiv(obj)
    raise ValueError('Unknown type')

def dtype_of_any_upcast(obj):
    """
    Convert a GDT (GDAL type) or a dtype (numpy data type) to an equivalent or larger dtype.
    If impossible an exception is raised.
    """
    dtype = _to_dtype_opt(obj)
    if dtype is not None:
        return dtype
    gdt = _to_gdt_opt(obj)
    if gdt is not None:
        return dtype_of_gdt_upcast(obj)
    raise ValueError('Unknown type')

def dtype_of_any_downcast(obj):
    """
    Convert a GDT (GDAL type) or a dtype (numpy data type) to an equivalent or larger or smaller
    dtype.
    If impossible an exception is raised.
    """
    dtype = _to_dtype_opt(obj)
    if dtype is not None:
        return dtype
    gdt = _to_gdt_opt(obj)
    if gdt is not None:
        return dtype_of_gdt_downcast(obj)
    raise ValueError('Unknown type')
