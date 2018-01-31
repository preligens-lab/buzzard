"""Test tools"""

from __future__ import division, print_function
import itertools
import logging

import numpy as np
from osgeo import gdal

import buzzard as buzz

LOGGER = logging.getLogger('buzzard')

_SRS = [
    (2154, 489353.59, 6587552.20, -378305.81, 6093283.21, 1212610.74, 7186901.68, """PROJCS["RGF93 / Lambert-93",GEOGCS["RGF93",DATUM["Reseau_Geodesique_Francais_1993",SPHEROID["GRS 1980",6378137,298.257222101,AUTHORITY["EPSG","7019"]],TOWGS84[0,0,0,0,0,0,0],AUTHORITY["EPSG","6171"]],PRIMEM["Greenwich",0,AUTHORITY["EPSG","8901"]],UNIT["degree",0.0174532925199433,AUTHORITY["EPSG","9122"]],AUTHORITY["EPSG","4171"]],PROJECTION["Lambert_Conformal_Conic_2SP"],PARAMETER["standard_parallel_1",49],PARAMETER["standard_parallel_2",44],PARAMETER["latitude_of_origin",46.5],PARAMETER["central_meridian",3],PARAMETER["false_easting",700000],PARAMETER["false_northing",6600000],UNIT["metre",1,AUTHORITY["EPSG","9001"]],AXIS["X",EAST],AXIS["Y",NORTH],AUTHORITY["EPSG","2154"]]"""), # pylint: disable=line-too-long
    (27561, 552587.60, 215784.19, 64398.20, 74408.53, 1012167.98, 398560.88, """PROJCS["NTF (Paris) / Lambert Nord France",GEOGCS["NTF (Paris)",DATUM["Nouvelle_Triangulation_Francaise_Paris",SPHEROID["Clarke 1880 (IGN)",6378249.2,293.4660212936265,AUTHORITY["EPSG","7011"]],TOWGS84[-168,-60,320,0,0,0,0],AUTHORITY["EPSG","6807"]],PRIMEM["Paris",2.33722917,AUTHORITY["EPSG","8903"]],UNIT["grad",0.01570796326794897,AUTHORITY["EPSG","9105"]],AUTHORITY["EPSG","4807"]],PROJECTION["Lambert_Conformal_Conic_1SP"],PARAMETER["latitude_of_origin",55],PARAMETER["central_meridian",0],PARAMETER["scale_factor",0.999877341],PARAMETER["false_easting",600000],PARAMETER["false_northing",200000],UNIT["metre",1,AUTHORITY["EPSG","9001"]],AXIS["X",EAST],AXIS["Y",NORTH],AUTHORITY["EPSG","27561"]]"""), # pylint: disable=line-too-long
    (5627, 836179.68, 5889250.81, -927864.00, 3972451.32, 1395572.16, 8094223.77, """PROJCS["ED50 / TM 6 NE",GEOGCS["ED50",DATUM["European_Datum_1950",SPHEROID["International 1924",6378388,297,AUTHORITY["EPSG","7022"]],TOWGS84[-87,-98,-121,0,0,0,0],AUTHORITY["EPSG","6230"]],PRIMEM["Greenwich",0,AUTHORITY["EPSG","8901"]],UNIT["degree",0.0174532925199433,AUTHORITY["EPSG","9122"]],AUTHORITY["EPSG","4230"]],PROJECTION["Transverse_Mercator"],PARAMETER["latitude_of_origin",0],PARAMETER["central_meridian",6],PARAMETER["scale_factor",0.9996],PARAMETER["false_easting",500000],PARAMETER["false_northing",0],UNIT["metre",1,AUTHORITY["EPSG","9001"]],AXIS["Easting",EAST],AXIS["Northing",NORTH],AUTHORITY["EPSG","5627"]]"""), # pylint: disable=line-too-long
    (3950, 1488760.97, 8798334.99, 613684.96, 8306333.45, 2211114.59, 9398784.57, """PROJCS["RGF93 / CC50",GEOGCS["RGF93",DATUM["Reseau_Geodesique_Francais_1993",SPHEROID["GRS 1980",6378137,298.257222101,AUTHORITY["EPSG","7019"]],TOWGS84[0,0,0,0,0,0,0],AUTHORITY["EPSG","6171"]],PRIMEM["Greenwich",0,AUTHORITY["EPSG","8901"]],UNIT["degree",0.0174532925199433,AUTHORITY["EPSG","9122"]],AUTHORITY["EPSG","4171"]],PROJECTION["Lambert_Conformal_Conic_2SP"],PARAMETER["standard_parallel_1",49.25],PARAMETER["standard_parallel_2",50.75],PARAMETER["latitude_of_origin",50],PARAMETER["central_meridian",3],PARAMETER["false_easting",1700000],PARAMETER["false_northing",9200000],UNIT["metre",1,AUTHORITY["EPSG","9001"]],AXIS["X",EAST],AXIS["Y",NORTH],AUTHORITY["EPSG","3950"]]"""), # pylint: disable=line-too-long
]

SRS = [
    dict(
        epsg=epsg,
        cx=cx, cy=cy, minx=minx, miny=miny, maxx=maxx, maxy=maxy, wkt=wkt,
        w=maxx - minx, h=maxy - miny
    )
    for epsg, cx, cy, minx, miny, maxx, maxy, wkt in _SRS
]

def eq(*items, **kwargs):
    """Numeric items are all almost equal"""
    tol = kwargs.pop('tol', 10e-5)
    assert not kwargs
    for a, b in itertools.combinations(items, 2):
        diff = np.abs(np.asarray(a) - np.asarray(b))
        if not (diff <= tol).all():
            return False
    return True


def eqall(items, **kwargs):
    """Numeric items are all almost equal"""
    return eq(*items, **kwargs)


def fpeq(*items, **kwargs):
    """Footprint items are all almost equal"""
    tol = kwargs.pop('tol', 10e-5)
    assert not kwargs
    for a, b in itertools.combinations(items, 2):
        diff = np.abs(a.gt - b.gt)
        if not (diff <= tol).all():
            return False
        diff = np.abs(a.rsize - b.rsize)
        if not (diff <= tol).all():
            return False
    return True


def poly_relation(a, b):
    """Describe 2 polygons relation"""
    rels = {
        'a contains b': a.contains(b),
        'a crosses b': a.crosses(b),
        'a disjoint b': a.disjoint(b),
        'a intersects b': a.intersects(b),
        'a touches b': a.touches(b),
        'a within b': a.within(b),
        'a covers b': a.covers(b),
        'a overlaps b': a.overlaps(b),
        'b contains a': b.contains(a),
        'b crosses a': b.crosses(a),
        'b disjoint a': b.disjoint(a),
        'b intersects a': b.intersects(a),
        'b touches a': b.touches(a),
        'b within a': b.within(a),
        'b covers a': b.covers(a),
        'b overlaps a': b.overlaps(a),
    }
    return ', '.join(
        k for k, v in rels.items() if v
    )

ROOT_TL = np.asarray([296455., 71495.])


def make_tif(path, tloffset=(0, 0), reso=(0.25, -0.25), rsize=(20, 10),
             proj=SRS[0]['wkt'], band_count=1, dtype=gdal.GDT_Float32):
    """Create a tiff files and return info about it"""
    tl = ROOT_TL + tloffset
    reso = np.asarray(reso)
    fp = buzz.Footprint(tl=tl, rsize=rsize, size=np.abs(reso * rsize))
    x, y = fp.meshgrid_spatial
    x = np.abs(x) - abs(ROOT_TL[0])
    y = abs(ROOT_TL[1]) - np.abs(y)
    x *= 15
    y *= 15
    a = x / 2 + y / 2
    a = np.around(a).astype('float32')
    driver = gdal.GetDriverByName('GTiff')
    dataset = driver.Create(path, rsize[0], rsize[1], band_count, dtype)
    dataset.SetGeoTransform(fp.gt)
    dataset.SetProjection(proj)
    for i in range(band_count):
        dataset.GetRasterBand(i + 1).WriteArray(a)
        dataset.GetRasterBand(i + 1).SetNoDataValue(-32000.)
    dataset.FlushCache()
    return path, fp, a


def make_tif2(path, reso=(1., -1.), rsize=(10, 10), tl=(0., 10.),
              proj=SRS[0]['wkt'], band_count=1, dtype=gdal.GDT_Float32,
              nodata=-32000, nodata_border_size=(0, 0, 0, 0)):
    """Create a tiff files"""
    reso = np.asarray(reso)
    fp = buzz.Footprint(tl=tl, rsize=rsize, size=np.abs(reso * rsize))
    x, y = fp.meshgrid_raster
    a = x + y
    if nodata_border_size != 0:
        l, r, t, b = nodata_border_size
        if t != 0:
            a[None:t, None:None] = nodata
        if b != 0:
            a[-b:None, None:None] = nodata
        if l != 0:
            a[None:None, None:l] = nodata
        if r != 0:
            a[None:None, -r:None] = nodata

    LOGGER.info('TIFF ARRAY:%s\n', a)
    gdal.UseExceptions()
    driver = gdal.GetDriverByName('GTiff')
    dataset = driver.Create(path, int(rsize[0]), int(rsize[1]), band_count, dtype)
    dataset.SetGeoTransform(fp.gt)
    dataset.SetProjection(proj)
    for i in range(band_count):
        dataset.GetRasterBand(i + 1).WriteArray(a)
        dataset.GetRasterBand(i + 1).SetNoDataValue(nodata)
    dataset.FlushCache()

def dump_tiles(tiles):
    """Print tiles"""
    for y, x in itertools.product(range(tiles.shape[0]), range(tiles.shape[1])):
        print(tiles[y, x])

def assert_tiles_eq(mata, matb, tol=10e-5):
    """Assert that two matrices contain the same tiles"""
    mata = np.asarray(mata)
    matb = np.asarray(matb)

    def _dump():
        print('--------')
        dump_tiles(mata)
        print('--------')
        dump_tiles(matb)

    if not mata.shape == matb.shape:
        _dump()
        assert mata.shape == matb.shape
    for a, b in zip(mata.flatten(), matb.flatten()):
        if not fpeq(a, b, tol=tol):
            _dump()
            assert fpeq(a, b, tol=tol)
