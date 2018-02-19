"""Collection of spatial references manipuation utilities,
might be filled/improved/moved in the future
"""

from osgeo import gdal, osr, ogr
import numpy as np
import affine

from buzzard._pint_interop import REG
from buzzard._env import env, Env
from buzzard._tools import conv
from buzzard import Footprint
from buzzard.srs._analysis import *

def wkt_of_any(string):
    """Wkt of user input"""
    out = osr.GetUserInputAsWKT(string)
    if isinstance(out, str):
        return out
    else:
        prj = None
        with Env(_osgeo_use_exceptions=False):
            gdal_ds = gdal.OpenEx(string, conv.of_of_str('raster'))
            if gdal_ds is not None:
                prj = gdal_ds.GetProjection()
            gdal_ds = gdal.OpenEx(string, conv.of_of_str('vector'))
            if gdal_ds is not None:
                lyr = gdal_ds.GetLayerByIndex(0)
                if lyr is not None:
                    prj = lyr.GetSpatialRef()
        if prj is not None:
            return prj.ExportToWkt()
    raise ValueError('Could not convert to wkt ({})'.format(gdal.GetLastErrorMsg()))

def wkt_same(a, b):
    """Are two wkt equivalent"""
    if a == b:
        return True
    sra = osr.SpatialReference(a)
    srb = osr.SpatialReference(b)
    return bool(sra.IsSame(srb))

def _details_of_file(path):
    with Env(_osgeo_use_exceptions=False):
        gdal_ds = gdal.OpenEx(path, conv.of_of_str('raster'))
        if gdal_ds is not None:
            aff = affine.Affine.from_gdal(*gdal_ds.GetGeoTransform())
            w, h = gdal_ds.RasterXSize, gdal_ds.RasterYSize
            cx, cy = aff * [w / 2, h / 2]
            return gdal_ds.GetProjection(), (cx, cy)
        gdal_ds = gdal.OpenEx(path, conv.of_of_str('vector'))
        if gdal_ds is not None:
            lyr = gdal_ds.GetLayerByIndex(0)
            if lyr is None:
                raise ValueError('Could not open file layer')
            extent = lyr.GetExtent()
            if extent is None:
                raise ValueError('Could not compute extent')
            minx, maxx, miny, maxy = extent
            cx, cy = maxx - minx, maxy - miny
            return lyr.GetSpatialRef().ExportToWkt(), (cx, cy)
        raise ValueError('Could not open file')

def wkt_of_file(path, center=False, unit=None, implicit_unit='m'):
    """Retrieve projection of file as wkt.
    Optionally recenter projection.
    Optionally change unit of projection.
    """
    wkt, centroid = _details_of_file(path)

    if center is False and unit is None:
        return wkt
    sr = osr.SpatialReference(wkt)

    if center:
        fe, fn = sr.GetProjParm('false_easting', 0.0), sr.GetProjParm('false_northing', 0.0)
        sr.SetProjParm('false_easting', fe - centroid[0])
        sr.SetProjParm('false_northing', fn - centroid[1])

    if unit is not None:
        src_name = sr.GetLinearUnitsName()
        if src_name is None or src_name == '':
            src_name = implicit_unit
        src_u = REG.parse_units(src_name.lower()) * 1.0
        dst_u = REG.parse_units(unit) * 1.0
        if dst_u.dimensionality != {'[length]': 1.0}:
            raise ValueError('todo')
        if src_u != dst_u:
            sr.SetLinearUnitsAndUpdateParameters(unit, float(dst_u / REG.m))
    return sr.ExportToWkt()
