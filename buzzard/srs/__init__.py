"""Collection of spatial references manipulation utilities,

Will be filled/improved/moved in the future
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
    raise ValueError('Could not convert to wkt ({})'.format(str(gdal.GetLastErrorMsg()).strip('\n')))

def wkt_same(a, b):
    """Are two wkt equivalent"""
    if a == b:
        return True
    sra = osr.SpatialReference(a)
    srb = osr.SpatialReference(b)
    return bool(sra.IsSame(srb))

def _details_of_file(path):
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
        cx, cy = (maxx + minx) / 2, (maxy + miny) / 2
        return lyr.GetSpatialRef().ExportToWkt(), (cx, cy)
    raise ValueError('Could not open file')

def wkt_of_file(path, center=False, unit=None, implicit_unit='m'):
    """Retrieve projection of file as wkt, optionally recenter projection, optionally change
    unit of projection. (Experimental function!)

    Parameters
    ----------
    path: str
    center: bool or nbr or (nbr, nbr)
    unit: None or str
    implicit_unit: str

    Returns
    -------
    str

    """
    wkt, centroid = _details_of_file(path)

    if center is False and unit is None:
        return wkt
    sr = osr.SpatialReference(wkt)

    if center is not False:
        if not sr.IsProjected():
            raise ValueError("Can't shift a spatial reference that is not projected")
        if center is True:
            center = (0, 0)
        else:
            center = np.asarray(center)
            if center.size == 1:
                center = (center, center)
            else:
                center = center.reshape(2)
        fe, fn = sr.GetProjParm('false_easting', 0.0), sr.GetProjParm('false_northing', 0.0)
        sr.SetProjParm('false_easting', fe - centroid[0] + center[0])
        sr.SetProjParm('false_northing', fn - centroid[1] + center[1])
        sr.SetAuthority('PROJCS', '', 0)

    if unit is not None:
        src_name = sr.GetLinearUnitsName()
        if src_name is None or src_name == '':
            src_name = implicit_unit
        src_u = REG.meter * sr.GetLinearUnits()
        dst_u = REG.parse_units(unit) * 1.0
        if dst_u.dimensionality != {'[length]': 1.0}:
            raise ValueError('todo')
        if src_u != dst_u:
            sr.SetLinearUnitsAndUpdateParameters(unit, float(dst_u / REG.m))
        sr.SetAuthority('PROJCS', '', 0)
    return sr.ExportToPrettyWkt()
