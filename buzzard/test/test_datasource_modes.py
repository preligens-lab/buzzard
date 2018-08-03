"""
TODO: Test insert_data under several modes
"""

# pylint: disable=redefined-outer-name, unused-argument

from __future__ import division, print_function
import os
import tempfile
import uuid
import string

import numpy as np
from osgeo import gdal
import pytest
import shapely.ops

import buzzard as buzz
from buzzard.test.tools import fpeq, sreq, eq
from buzzard.test import make_tile_set
from .tools import  get_srs_by_name

SR1 = get_srs_by_name('EPSG:2154')
SR2 = get_srs_by_name('EPSG:2154 centered')

# FIXTURES ************************************************************************************** **
@pytest.fixture(scope='module')
def fps():
    """See make_tile_set
    A B C
    D E F
    G H I
    """
    return make_tile_set.make_tile_set(3, [0.1, -0.1])

@pytest.fixture()
def env():
    with buzz.Env(significant=8):
        yield

@pytest.fixture(scope='module')
def shp1_path(fps):
    """Create a shapefile in SR1 containing all single letter polygons from `fps` fixture"""
    path = '{}/{}.shp'.format(tempfile.gettempdir(), uuid.uuid4())

    ds = buzz.DataSource()
    ds.create_vector('poly', path, 'polygon', sr=SR1['wkt'])
    for letter in string.ascii_uppercase[:9]:
        ds.poly.insert_data(fps[letter].poly)
    ds.poly.close()
    del ds
    yield path
    gdal.GetDriverByName('ESRI Shapefile').Delete(path)

@pytest.fixture(scope='module')
def tif1_path(fps):
    """Create a tif in SR1 with all single letter footprints from `fps` fixture burnt in it"""
    path = '{}/{}.tif'.format(tempfile.gettempdir(), uuid.uuid4())

    ds = buzz.DataSource()
    ds.create_raster('rast', path, fps.AI, 'int32', 1, sr=SR1['wkt'])
    for letter in string.ascii_uppercase[:9]:
        fp = fps[letter]
        arr = np.full(fp.shape, ord(letter), dtype=int)
        ds.rast.set_data(arr, fp=fp)
    ds.rast.close()
    del ds
    yield path
    gdal.GetDriverByName('GTiff').Delete(path)

@pytest.fixture(scope='module')
def shp2_path(fps):
    """Create a shapefile in SR2 containing all single letter polygons from `fps` fixture"""
    path = '{}/{}.shp'.format(tempfile.gettempdir(), uuid.uuid4())

    ds = buzz.DataSource()
    ds.create_vector('poly', path, 'polygon', sr=SR2['wkt'])
    for letter in string.ascii_uppercase[:9]:
        ds.poly.insert_data(fps[letter].poly)
    ds.poly.close()
    del ds
    yield path
    gdal.GetDriverByName('ESRI Shapefile').Delete(path)

@pytest.fixture(scope='module')
def tif2_path(fps):
    """Create a tif in SR2 with all single letter footprints from `fps` fixture burnt in it"""
    path = '{}/{}.tif'.format(tempfile.gettempdir(), uuid.uuid4())

    ds = buzz.DataSource()
    with ds.acreate_raster(path, fps.AI, 'int32', 1, sr=SR2['wkt']).close as r:
        for letter in string.ascii_uppercase[:9]:
            fp = fps[letter]
            arr = np.full(fp.shape, ord(letter), dtype=int)
            r.set_data(arr, fp=fp)
    yield path
    gdal.GetDriverByName('GTiff').Delete(path)


@pytest.fixture(scope='module')
def shp3_path(fps):
    """Create a shapefile without SR containing all single letter polygons from `fps` fixture"""
    path = '{}/{}.shp'.format(tempfile.gettempdir(), uuid.uuid4())

    ds = buzz.DataSource()
    ds.create_vector('poly', path, 'polygon', sr=None)
    for letter in string.ascii_uppercase[:9]:
        ds.poly.insert_data(fps[letter].poly)
    ds.poly.close()
    del ds
    yield path
    gdal.GetDriverByName('ESRI Shapefile').Delete(path)

@pytest.fixture(scope='module')
def tif3_path(fps):
    """Create a tif witwhout SR with all single letter footprints from `fps` fixture burnt in it"""
    path = '{}/{}.tif'.format(tempfile.gettempdir(), uuid.uuid4())

    ds = buzz.DataSource()
    with ds.acreate_raster(path, fps.AI, 'int32', 1, sr=None).close as r:
        for letter in string.ascii_uppercase[:9]:
            fp = fps[letter]
            arr = np.full(fp.shape, ord(letter), dtype=int)
            r.set_data(arr, fp=fp)
    yield path
    gdal.GetDriverByName('GTiff').Delete(path)

@pytest.fixture()
def random_path_shp():
    """Create a temporary path, and take care of cleanup afterward"""
    path = '{}/{}.shp'.format(tempfile.gettempdir(), uuid.uuid4())
    yield path
    if os.path.isfile(path):
        try:
            gdal.GetDriverByName('ESRI Shapefile').Delete(path)
        except:
            os.remove(path)

@pytest.fixture()
def random_path_tif():
    """Create a temporary path, and take care of cleanup afterward"""
    path = '{}/{}.tif'.format(tempfile.gettempdir(), uuid.uuid4())
    yield path
    if os.path.isfile(path):
        try:
            gdal.GetDriverByName('GTiff').Delete(path)
        except:
            os.remove(path)

# TESTS ***************************************************************************************** **
def test_mode1(fps, shp1_path, tif1_path, shp2_path, tif2_path, shp3_path, tif3_path):
    ds = buzz.DataSource()
    ds.open_raster('tif1', tif1_path)
    ds.open_vector('shp1', shp1_path)
    ds.open_raster('tif2', tif2_path)
    ds.open_vector('shp2', shp2_path)
    ds.open_raster('tif3', tif3_path)
    ds.open_vector('shp3', shp3_path)

    # Test SR equality
    assert sreq(
        ds.tif1.wkt_virtual,
        ds.tif1.wkt_stored,
        ds.tif1.proj4_virtual,
        ds.tif1.proj4_stored,
        ds.shp1.wkt_virtual,
        ds.shp1.wkt_stored,
        ds.shp1.proj4_virtual,
        ds.shp1.proj4_stored,
    )
    assert sreq(
        ds.tif2.wkt_virtual,
        ds.tif2.wkt_stored,
        ds.tif2.proj4_virtual,
        ds.tif2.proj4_stored,
        ds.shp2.wkt_virtual,
        ds.shp2.wkt_stored,
        ds.shp2.proj4_virtual,
        ds.shp2.proj4_stored,
    )
    assert not sreq(ds.tif1.wkt_stored, ds.tif2.wkt_stored)
    assert eq(
        None ==
        ds.wkt ==
        ds.proj4 ==
        ds.tif3.wkt_virtual ==
        ds.tif3.wkt_stored ==
        ds.tif3.proj4_virtual ==
        ds.tif3.proj4_stored ==
        ds.shp3.wkt_virtual ==
        ds.shp3.wkt_stored ==
        ds.shp3.proj4_virtual ==
        ds.shp3.proj4_stored
    )

    # Test footprints equality
    assert fpeq(
        fps.AI,
        # tif/shp 1
        ds.tif1.fp,
        ds.tif1.fp_origin,
        buzz.Footprint.of_extent(ds.shp1.extent, fps.AI.scale),
        buzz.Footprint.of_extent(ds.shp1.extent_stored, fps.AI.scale),
        buzz.Footprint.of_extent(ds.shp1.bounds[[0, 2, 1, 3]], fps.AI.scale),
        buzz.Footprint.of_extent(ds.shp1.bounds_stored[[0, 2, 1, 3]], fps.AI.scale),
        # tif/shp 2
        ds.tif2.fp,
        ds.tif2.fp_origin,
        buzz.Footprint.of_extent(ds.shp2.extent, fps.AI.scale),
        buzz.Footprint.of_extent(ds.shp2.extent_stored, fps.AI.scale),
        buzz.Footprint.of_extent(ds.shp2.bounds[[0, 2, 1, 3]], fps.AI.scale),
        buzz.Footprint.of_extent(ds.shp2.bounds_stored[[0, 2, 1, 3]], fps.AI.scale),
        # tif/shp 3
        ds.tif3.fp,
        ds.tif3.fp_origin,
        buzz.Footprint.of_extent(ds.shp3.extent, fps.AI.scale),
        buzz.Footprint.of_extent(ds.shp3.extent_stored, fps.AI.scale),
        buzz.Footprint.of_extent(ds.shp3.bounds[[0, 2, 1, 3]], fps.AI.scale),
        buzz.Footprint.of_extent(ds.shp3.bounds_stored[[0, 2, 1, 3]], fps.AI.scale),
    )

    # Test what's written in all 6 files
    tif1 = ds.tif1.get_data()
    tif2 = ds.tif2.get_data()
    tif3 = ds.tif3.get_data()
    assert np.all(tif1 == tif2)
    assert np.all(tif1 == tif3)
    for i, letter in enumerate(string.ascii_uppercase[:9]):
        # tif/shp 1
        shp1 = ds.shp1.get_data(i, None)
        raster1_polys = ds.tif1.fp.find_polygons(tif1 == ord(letter))
        assert len(raster1_polys) == 1
        assert (shp1 ^ raster1_polys[0]).is_empty

        # tif/shp 2
        shp2 = ds.shp2.get_data(i, None)
        raster2_polys = ds.tif2.fp.find_polygons(tif2 == ord(letter))
        assert len(raster2_polys) == 1
        assert (shp2 ^ raster2_polys[0]).is_empty

        # tif/shp 3
        shp3 = ds.shp3.get_data(i, None)
        raster3_polys = ds.tif3.fp.find_polygons(tif3 == ord(letter))
        assert len(raster3_polys) == 1
        assert (shp3 ^ raster3_polys[0]).is_empty

def test_mode2(fps, shp1_path, tif1_path, shp2_path, tif2_path, shp3_path, tif3_path,
               random_path_shp, random_path_tif, env):
    ds = buzz.DataSource(sr_work=SR1['wkt'])
    ds.open_raster('tif1', tif1_path)
    ds.open_vector('shp1', shp1_path)
    ds.open_raster('tif2', tif2_path)
    ds.open_vector('shp2', shp2_path)

    # Test file creation/opening without spatial reference
    with buzz.Env(allow_complex_footprint=True):
        with pytest.raises(ValueError, match='spatial refe'):
            ds.acreate_vector(random_path_shp, 'polygon', [], sr=None)
        with pytest.raises(ValueError, match='spatial refe'):
            ds.acreate_raster(random_path_tif, fps.AI, 'int32', 1, {}, sr=None)
        with pytest.raises(ValueError, match='spatial refe'):
            ds.aopen_raster(tif3_path)
        with pytest.raises(ValueError, match='spatial refe'):
            ds.aopen_vector(shp3_path)

    # Test SR equality
    assert sreq(
        ds.wkt,
        ds.proj4,
        ds.tif1.wkt_virtual,
        ds.tif1.wkt_stored,
        ds.tif1.proj4_virtual,
        ds.tif1.proj4_stored,
        ds.shp1.wkt_virtual,
        ds.shp1.wkt_stored,
        ds.shp1.proj4_virtual,
        ds.shp1.proj4_stored,
    )
    assert sreq(
        ds.tif2.wkt_virtual,
        ds.tif2.proj4_virtual,
        ds.shp2.wkt_virtual,
        ds.shp2.proj4_virtual,
        ds.tif2.wkt_stored,
        ds.tif2.proj4_stored,
        ds.shp2.wkt_stored,
        ds.shp2.proj4_stored,
    )
    assert not sreq(ds.tif1.wkt_stored, ds.tif2.wkt_stored)

    # Test foorprints equality
    assert fpeq(
        fps.AI,
        # tif/shp 1
        ds.tif1.fp,
        ds.tif1.fp_origin,
        buzz.Footprint.of_extent(ds.shp1.extent, fps.AI.scale),
        buzz.Footprint.of_extent(ds.shp1.extent_stored, fps.AI.scale),
        buzz.Footprint.of_extent(ds.shp1.bounds[[0, 2, 1, 3]], fps.AI.scale),
        buzz.Footprint.of_extent(ds.shp1.bounds_stored[[0, 2, 1, 3]], fps.AI.scale),

        # tif/shp 2
        ds.tif2.fp_origin,
        buzz.Footprint.of_extent(ds.shp2.extent_stored, fps.AI.scale),
        buzz.Footprint.of_extent(ds.shp2.bounds_stored[[0, 2, 1, 3]], fps.AI.scale),
    )
    assert fpeq(
        ds.tif2.fp,
        buzz.Footprint.of_extent(ds.shp2.extent, fps.AI.scale),
        buzz.Footprint.of_extent(ds.shp2.bounds[[0, 2, 1, 3]], fps.AI.scale),
    )
    assert ds.tif2.fp != ds.tif1.fp

    # Test file creation with/without conversion of footprint
    with buzz.Env(allow_complex_footprint=True):
        with ds.acreate_raster(random_path_tif, fps.AI, 'int32', 1, sr=SR1['wkt']).delete as r:
            assert fpeq(
                fps.AI,
                r.fp,
                r.fp_origin
            )
        with ds.acreate_raster(random_path_tif, fps.AI, 'int32', 1, sr=SR2['wkt']).delete as r:
            assert fpeq(
                fps.AI,
                r.fp,
            )
            assert fps.AI != r.fp_origin

    # Test what's written in all 4 files
    tif1 = ds.tif1.get_data()
    tif2 = ds.tif2.get_data()
    assert np.all(tif1 == tif2)

    def f(x, y, z=None):
        return np.around(x, 6), np.around(y, 6)

    for i, letter in enumerate(string.ascii_uppercase[:9]):
        # tif/shp 1
        shp1 = ds.shp1.get_data(i, None)
        shp1 = shapely.ops.transform(f, shp1)

        raster1_polys = ds.tif1.fp.find_polygons(tif1 == ord(letter))
        assert len(raster1_polys) == 1
        raster1_poly = raster1_polys[0]
        raster1_poly = shapely.ops.transform(f, shp1)

        assert (shp1 ^ raster1_poly).is_empty

        # tif/shp 2
        shp2 = ds.shp2.get_data(i, None)
        shp2 = shapely.ops.transform(f, shp2)

        raster2_polys = ds.tif2.fp.find_polygons(tif2 == ord(letter))
        assert len(raster2_polys) == 1
        raster2_poly = raster2_polys[0]
        raster2_poly = shapely.ops.transform(f, shp2)

        assert (shp2 ^ raster2_poly).is_empty

def test_mode3(fps, shp1_path, tif1_path, shp2_path, tif2_path, shp3_path, tif3_path,
               random_path_shp, random_path_tif, env):
    ds = buzz.DataSource(sr_work=SR1['wkt'], sr_fallback=SR2['wkt'])
    ds.open_raster('tif1', tif1_path)
    ds.open_vector('shp1', shp1_path)
    ds.open_raster('tif2', tif2_path)
    ds.open_vector('shp2', shp2_path)
    ds.open_raster('tif3', tif3_path)
    ds.open_vector('shp3', shp3_path)

    # Test file creation without spatial reference
    with buzz.Env(allow_complex_footprint=True):
        with ds.acreate_vector(random_path_shp, 'polygon', [], sr=None).close as r:
            assert r.wkt_stored == None
            assert sreq(r.wkt_virtual, SR2['wkt'])
        with ds.acreate_raster(random_path_tif, fps.AI, 'int32', 1, {}, sr=None).close as v:
            assert v.wkt_stored == None
            assert sreq(v.wkt_virtual, SR2['wkt'])

    # Test SR equality
    assert sreq(
        ds.wkt,
        ds.proj4,
        ds.tif1.wkt_virtual,
        ds.tif1.wkt_stored,
        ds.tif1.proj4_virtual,
        ds.tif1.proj4_stored,
        ds.shp1.wkt_virtual,
        ds.shp1.wkt_stored,
        ds.shp1.proj4_virtual,
        ds.shp1.proj4_stored,

    )
    assert sreq(
        ds.tif2.wkt_virtual,
        ds.tif2.proj4_virtual,
        ds.shp2.wkt_virtual,
        ds.shp2.proj4_virtual,
        ds.tif2.wkt_stored,
        ds.tif2.proj4_stored,
        ds.shp2.wkt_stored,
        ds.shp2.proj4_stored,

        ds.tif3.wkt_virtual,
        ds.tif3.proj4_virtual,
        ds.shp3.wkt_virtual,
        ds.shp3.proj4_virtual,
    )
    assert not sreq(ds.tif1.wkt_stored, ds.tif2.wkt_stored)
    assert eq(
        None ==
        ds.tif3.wkt_stored ==
        ds.tif3.proj4_stored ==
        ds.shp3.wkt_stored ==
        ds.shp3.proj4_stored
    )

    # Test foorprints equality
    assert fpeq(
        fps.AI,
        # tif/shp 1
        ds.tif1.fp,
        ds.tif1.fp_origin,
        buzz.Footprint.of_extent(ds.shp1.extent, fps.AI.scale),
        buzz.Footprint.of_extent(ds.shp1.extent_stored, fps.AI.scale),
        buzz.Footprint.of_extent(ds.shp1.bounds[[0, 2, 1, 3]], fps.AI.scale),
        buzz.Footprint.of_extent(ds.shp1.bounds_stored[[0, 2, 1, 3]], fps.AI.scale),

        # tif/shp 2
        ds.tif2.fp_origin,
        buzz.Footprint.of_extent(ds.shp2.extent_stored, fps.AI.scale),
        buzz.Footprint.of_extent(ds.shp2.bounds_stored[[0, 2, 1, 3]], fps.AI.scale),

        # tif/shp 3
        ds.tif3.fp_origin,
        buzz.Footprint.of_extent(ds.shp3.extent_stored, fps.AI.scale),
        buzz.Footprint.of_extent(ds.shp3.bounds_stored[[0, 2, 1, 3]], fps.AI.scale),
    )
    assert fpeq(
        ds.tif2.fp,
        buzz.Footprint.of_extent(ds.shp2.extent, fps.AI.scale),
        buzz.Footprint.of_extent(ds.shp2.bounds[[0, 2, 1, 3]], fps.AI.scale),

        ds.tif3.fp,
        buzz.Footprint.of_extent(ds.shp3.extent, fps.AI.scale),
        buzz.Footprint.of_extent(ds.shp3.bounds[[0, 2, 1, 3]], fps.AI.scale),
    )
    assert ds.tif2.fp != ds.tif1.fp

    # Test what's written in all 6 files
    tif1 = ds.tif1.get_data()
    tif2 = ds.tif2.get_data()
    tif3 = ds.tif3.get_data()
    assert np.all(tif1 == tif2)
    assert np.all(tif1 == tif3)

    def f(x, y, z=None):
        return np.around(x, 6), np.around(y, 6)

    for i, letter in enumerate(string.ascii_uppercase[:9]):
        # tif/shp 1
        shp1 = ds.shp1.get_data(i, None)
        shp1 = shapely.ops.transform(f, shp1)

        raster1_polys = ds.tif1.fp.find_polygons(tif1 == ord(letter))
        assert len(raster1_polys) == 1
        raster1_poly = raster1_polys[0]
        raster1_poly = shapely.ops.transform(f, shp1)

        assert (shp1 ^ raster1_poly).is_empty

        # tif/shp 2
        shp2 = ds.shp2.get_data(i, None)
        shp2 = shapely.ops.transform(f, shp2)

        raster2_polys = ds.tif2.fp.find_polygons(tif2 == ord(letter))
        assert len(raster2_polys) == 1
        raster2_poly = raster2_polys[0]
        raster2_poly = shapely.ops.transform(f, shp2)

        assert (shp2 ^ raster2_poly).is_empty

        # tif/shp 3
        shp3 = ds.shp3.get_data(i, None)
        shp3 = shapely.ops.transform(f, shp3)

        raster3_polys = ds.tif3.fp.find_polygons(tif3 == ord(letter))
        assert len(raster3_polys) == 1
        raster3_poly = raster3_polys[0]
        raster3_poly = shapely.ops.transform(f, shp3)

        assert (shp3 ^ raster3_poly).is_empty

def test_mode4(fps, shp1_path, tif1_path, shp2_path, tif2_path, shp3_path, tif3_path,
               random_path_shp, random_path_tif, env):
    ds = buzz.DataSource(sr_work=SR1['wkt'], sr_forced=SR2['wkt'])
    ds.open_raster('tif1', tif1_path)
    ds.open_vector('shp1', shp1_path)
    ds.open_raster('tif2', tif2_path)
    ds.open_vector('shp2', shp2_path)
    ds.open_raster('tif3', tif3_path)
    ds.open_vector('shp3', shp3_path)

    # Test file creation without spatial reference
    with buzz.Env(allow_complex_footprint=True):
        with ds.acreate_vector(random_path_shp, 'polygon', [], sr=None).close as r:
            assert r.wkt_stored == None
            assert sreq(r.wkt_virtual, SR2['wkt'])
        with ds.acreate_raster(random_path_tif, fps.AI, 'int32', 1, {}, sr=None).close as v:
            assert v.wkt_stored == None
            assert sreq(v.wkt_virtual, SR2['wkt'])

    # Test SR equality
    assert sreq(
        ds.wkt,
        ds.proj4,
        ds.tif1.wkt_stored,
        ds.tif1.proj4_stored,
        ds.shp1.wkt_stored,
        ds.shp1.proj4_stored,
    )
    assert sreq(
        ds.tif1.wkt_virtual,
        ds.tif1.proj4_virtual,
        ds.shp1.wkt_virtual,
        ds.shp1.proj4_virtual,

        ds.tif2.wkt_virtual,
        ds.tif2.proj4_virtual,
        ds.shp2.wkt_virtual,
        ds.shp2.proj4_virtual,
        ds.tif2.wkt_stored,
        ds.tif2.proj4_stored,
        ds.shp2.wkt_stored,
        ds.shp2.proj4_stored,

        ds.tif3.wkt_virtual,
        ds.tif3.proj4_virtual,
        ds.shp3.wkt_virtual,
        ds.shp3.proj4_virtual,
    )
    assert not sreq(ds.tif1.wkt_stored, ds.tif2.wkt_stored)
    assert eq(
        None ==
        ds.tif3.wkt_stored ==
        ds.tif3.proj4_stored ==
        ds.shp3.wkt_stored ==
        ds.shp3.proj4_stored
    )

    # Test foorprints equality
    assert fpeq(
        fps.AI,
        # tif/shp 1
        ds.tif1.fp_origin,
        buzz.Footprint.of_extent(ds.shp1.extent_stored, fps.AI.scale),
        buzz.Footprint.of_extent(ds.shp1.bounds_stored[[0, 2, 1, 3]], fps.AI.scale),

        # tif/shp 2
        ds.tif2.fp_origin,
        buzz.Footprint.of_extent(ds.shp2.extent_stored, fps.AI.scale),
        buzz.Footprint.of_extent(ds.shp2.bounds_stored[[0, 2, 1, 3]], fps.AI.scale),

        # tif/shp 3
        ds.tif3.fp_origin,
        buzz.Footprint.of_extent(ds.shp3.extent_stored, fps.AI.scale),
        buzz.Footprint.of_extent(ds.shp3.bounds_stored[[0, 2, 1, 3]], fps.AI.scale),
    )
    assert fpeq(
        # tif/shp 1
        ds.tif1.fp,
        buzz.Footprint.of_extent(ds.shp1.extent, fps.AI.scale),
        buzz.Footprint.of_extent(ds.shp1.bounds[[0, 2, 1, 3]], fps.AI.scale),

        # tif/shp 2
        ds.tif2.fp,
        buzz.Footprint.of_extent(ds.shp2.extent, fps.AI.scale),
        buzz.Footprint.of_extent(ds.shp2.bounds[[0, 2, 1, 3]], fps.AI.scale),

        # tif/shp 3
        ds.tif3.fp,
        buzz.Footprint.of_extent(ds.shp3.extent, fps.AI.scale),
        buzz.Footprint.of_extent(ds.shp3.bounds[[0, 2, 1, 3]], fps.AI.scale),
    )
    assert ds.tif1.fp != ds.tif1.fp_stored

    # Test what's written in all 6 files
    tif1 = ds.tif1.get_data()
    tif2 = ds.tif2.get_data()
    tif3 = ds.tif3.get_data()
    assert np.all(tif1 == tif2)
    assert np.all(tif1 == tif3)

    def f(x, y, z=None):
        return np.around(x, 6), np.around(y, 6)

    for i, letter in enumerate(string.ascii_uppercase[:9]):
        # tif/shp 1
        shp1 = ds.shp1.get_data(i, None)
        shp1 = shapely.ops.transform(f, shp1)

        raster1_polys = ds.tif1.fp.find_polygons(tif1 == ord(letter))
        assert len(raster1_polys) == 1
        raster1_poly = raster1_polys[0]
        raster1_poly = shapely.ops.transform(f, shp1)

        assert (shp1 ^ raster1_poly).is_empty

        # tif/shp 2
        shp2 = ds.shp2.get_data(i, None)
        shp2 = shapely.ops.transform(f, shp2)

        raster2_polys = ds.tif2.fp.find_polygons(tif2 == ord(letter))
        assert len(raster2_polys) == 1
        raster2_poly = raster2_polys[0]
        raster2_poly = shapely.ops.transform(f, shp2)

        assert (shp2 ^ raster2_poly).is_empty

        # tif/shp 3
        shp3 = ds.shp3.get_data(i, None)
        shp3 = shapely.ops.transform(f, shp3)

        raster3_polys = ds.tif3.fp.find_polygons(tif3 == ord(letter))
        assert len(raster3_polys) == 1
        raster3_poly = raster3_polys[0]
        raster3_poly = shapely.ops.transform(f, shp3)

        assert (shp3 ^ raster3_poly).is_empty
