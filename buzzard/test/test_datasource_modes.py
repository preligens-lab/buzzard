
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
from buzzard.test.tools import fpeq
from buzzard.test import make_tile_set
from .tools import SRS

@pytest.fixture(scope='module')
def fps():
    """
    See make_tile_set
    A B C
    D E F
    G H I
    """
    return make_tile_set.make_tile_set(3, [0.1, -0.1])

@pytest.fixture()
def env():
    with buzz.Env(significant=6):
        yield

@pytest.fixture(scope='module')
def poly_path(fps):
    path = '{}/{}.shp'.format(tempfile.gettempdir(), uuid.uuid4())

    ds = buzz.DataSource()
    ds.create_vector('poly', path, 'polygon', sr=SRS[0]['wkt'])
    for letter in string.ascii_uppercase[:9]:
        ds.poly.insert_data(fps[letter].poly)
    del ds
    yield path
    gdal.GetDriverByName('ESRI Shapefile').Delete(path)

@pytest.fixture(scope='module')
def rast_path(fps):
    path = '{}/{}.tif'.format(tempfile.gettempdir(), uuid.uuid4())

    ds = buzz.DataSource()
    ds.create_raster('rast', path, fps.AI, 'int32', 1, sr=SRS[0]['wkt'])
    for letter in string.ascii_uppercase[:9]:
        fp = fps[letter]
        arr = np.full(fp.shape, ord(letter), dtype=int)
        ds.rast.set_data(arr, fp=fp)
    del ds
    yield path
    gdal.GetDriverByName('ESRI Shapefile').Delete(path)


@pytest.fixture()
def path_shp():
    """Create a temporary path, and take care of cleanup afterward"""
    path = '{}/{}.shp'.format(tempfile.gettempdir(), uuid.uuid4())
    yield path
    if os.path.isfile(path):
        try:
            gdal.GetDriverByName('ESRI Shapefile').Delete(path)
        except:
            os.remove(path)

@pytest.fixture()
def path_tif():
    """Create a temporary path, and take care of cleanup afterward"""
    path = '{}/{}.tif'.format(tempfile.gettempdir(), uuid.uuid4())
    yield path
    if os.path.isfile(path):
        try:
            gdal.GetDriverByName('GTiff').Delete(path)
        except:
            os.remove(path)

def test_mode1(fps, poly_path, rast_path):
    ds = buzz.DataSource()
    ds.open_raster('rast', rast_path)
    ds.open_vector('poly', poly_path)

    assert fpeq(
        ds.rast.fp,
        ds.rast.fp_origin,
        fps.AI,
        buzz.Footprint.of_extent(ds.poly.extent, fps.AI.scale),
    )
    rast = ds.rast.get_data()

    for i, letter in enumerate(string.ascii_uppercase[:9]):
        poly = ds.poly.get_data(i, None)
        raster_polys = ds.rast.fp.find_polygons(rast == ord(letter))
        assert len(raster_polys) == 1
        assert (poly ^ raster_polys[0]).is_empty


def test_mode2(fps, poly_path, rast_path, path_shp, path_tif, env):
    wkt_work = buzz.srs.wkt_of_file(rast_path, center=True)

    ds = buzz.DataSource(wkt_work)
    ds.open_raster('rast', rast_path)
    ds.open_vector('poly', poly_path)

    with buzz.Env(allow_complex_footprint=True):
        with pytest.raises(ValueError, match='spatial refe'):
            ds.create_avector(path_shp, 'polygon', [], sr=None)
        with pytest.raises(ValueError, match='spatial refe'):
            ds.create_araster(path_tif, fps.AI, 'int32', 1, {}, sr=None)

    fp_poly = buzz.Footprint.of_extent(ds.poly.extent, ds.rast.fp.scale)
    fp_poly_origin = buzz.Footprint.of_extent(ds.poly.extent_origin, ds.rast.fp_origin.scale)
    assert fpeq(
        ds.rast.fp,
        fp_poly,
    )
    assert fpeq(
        ds.rast.fp_origin,
        fps.AI,
        fp_poly_origin,
    )
    rast = ds.rast.get_data()

    def f(x, y, z=None):
        return np.around(x, 6), np.around(y, 6)

    for i, letter in enumerate(string.ascii_uppercase[:9]):
        poly = ds.poly.get_data(i, None)
        raster_polys = ds.rast.fp.find_polygons(rast == ord(letter))
        assert len(raster_polys) == 1
        raster_poly = raster_polys[0]
        del raster_polys
        poly = shapely.ops.transform(f, poly)
        raster_poly = shapely.ops.transform(f, poly)
        assert (poly ^ raster_poly).is_empty


def test_mode3(fps, poly_path, rast_path, path_shp, path_tif, env):
    wkt_origin = buzz.srs.wkt_of_file(rast_path)
    wkt_work = buzz.srs.wkt_of_file(rast_path, center=True)

    ds = buzz.DataSource(wkt_work, sr_implicit=wkt_origin)
    ds.open_raster('rast', rast_path)
    ds.open_vector('poly', poly_path)

    with buzz.Env(allow_complex_footprint=True):
        ds.create_avector(path_shp, 'polygon', [], sr=None).close()
        ds.create_araster(path_tif, fps.AI, 'int32', 1, {}, sr=None).close()

    fp_poly = buzz.Footprint.of_extent(ds.poly.extent, ds.rast.fp.scale)
    fp_poly_origin = buzz.Footprint.of_extent(ds.poly.extent_origin, ds.rast.fp_origin.scale)
    assert fpeq(
        ds.rast.fp,
        fp_poly,
    )
    assert fpeq(
        ds.rast.fp_origin,
        fps.AI,
        fp_poly_origin,
    )
    rast = ds.rast.get_data()

    def f(x, y, z=None):
        return np.around(x, 6), np.around(y, 6)

    for i, letter in enumerate(string.ascii_uppercase[:9]):
        poly = ds.poly.get_data(i, None)
        raster_polys = ds.rast.fp.find_polygons(rast == ord(letter))
        assert len(raster_polys) == 1
        raster_poly = raster_polys[0]
        del raster_polys
        poly = shapely.ops.transform(f, poly)
        raster_poly = shapely.ops.transform(f, poly)
        assert (poly ^ raster_poly).is_empty

def test_mode4(fps, poly_path, rast_path, path_shp, path_tif, env):
    wkt_origin = buzz.srs.wkt_of_file(rast_path)
    wkt_work = buzz.srs.wkt_of_file(rast_path, center=True)

    ds = buzz.DataSource(wkt_work, sr_origin=wkt_origin)
    ds.open_raster('rast', rast_path)
    ds.open_vector('poly', poly_path)

    with buzz.Env(allow_complex_footprint=True):
        ds.create_avector(path_shp, 'polygon', [], sr=None).close()
        ds.create_araster(path_tif, fps.AI, 'int32', 1, {}, sr=None).close()

    fp_poly = buzz.Footprint.of_extent(ds.poly.extent, ds.rast.fp.scale)
    fp_poly_origin = buzz.Footprint.of_extent(ds.poly.extent_origin, ds.rast.fp_origin.scale)
    assert fpeq(
        ds.rast.fp,
        fp_poly,
    )
    assert fpeq(
        ds.rast.fp_origin,
        fps.AI,
        fp_poly_origin,
    )
    rast = ds.rast.get_data()

    def f(x, y, z=None):
        return np.around(x, 6), np.around(y, 6)

    for i, letter in enumerate(string.ascii_uppercase[:9]):
        poly = ds.poly.get_data(i, None)
        raster_polys = ds.rast.fp.find_polygons(rast == ord(letter))
        assert len(raster_polys) == 1
        raster_poly = raster_polys[0]
        del raster_polys
        poly = shapely.ops.transform(f, poly)
        raster_poly = shapely.ops.transform(f, poly)
        assert (poly ^ raster_poly).is_empty

def test_raster(fps, path_tif):

    def _asserts(should_exist, should_be_open, is_anonymous=False):

        exist = os.path.isfile(path_tif)
        assert should_exist == exist

        if not is_anonymous:
            is_open_key = 'test' in ds
            is_open_prox = test in ds
            assert is_open_key == is_open_prox
            assert is_open_key == should_be_open
            if is_open_key:
                assert test is ds.test is ds['test']
        else:
            is_open = test in ds
            assert is_open == should_be_open

        if should_be_open:
            assert fps.A == test.fp == test.fp_origin
            assert len(test) == 1
            assert test.dtype == np.float64
            assert test.nodata == -32727 == test.get_nodata()
            assert buzz.srs.wkt_same(SRS[0]['wkt'], test.wkt_origin)
            assert test.path == path_tif
            assert test.band_schema == {
                'nodata': [-32727.0],
                'interpretation': ['grayindex'],
                'offset': [0.0],
                'scale': [1.0],
                'mask': ['nodata']
            }

    schema = {
        'nodata': -32727
    }
    ds = buzz.DataSource()
    assert ds.proj4 is None
    assert ds.wkt is None

    # Raster test 1
    test = None
    _asserts(False, False)
    test = ds.create_raster('test', path_tif, fps.A, float, 1, schema, sr=SRS[0]['wkt'])
    _asserts(True, True)
    ds.test.close()
    _asserts(True, False)

    test = ds.open_araster(path_tif)
    _asserts(True, True, True)
    test.close()
    _asserts(True, False)

    test = ds.open_raster('test', path_tif, mode='w')
    _asserts(True, True)
    test.delete()
    _asserts(False, False)

    # Raster test 2
    with ds.create_raster('test', path_tif, fps.A, float, 1, schema, sr=SRS[0]['wkt']).close as test:
        _asserts(True, True)
    _asserts(True, False)
    with ds.open_raster('test', path_tif, mode='w').delete as test:
        _asserts(True, True)
    _asserts(False, False)

    # Raster test 3
    with ds.create_araster(path_tif, fps.A, float, 1, schema, sr=SRS[0]['wkt']).delete as test:
        _asserts(True, True, True)
    _asserts(False, False)

    # Raster test 4
    with ds.create_raster('test', path_tif, fps.A, float, 1, schema, sr=SRS[0]['wkt']).delete as test:
        _asserts(True, True)
    _asserts(False, False)



def test_vector(path_shp):

    def _asserts(should_exist, should_be_open, is_anonymous=False):

        exist_shp = os.path.isfile(path_shp)
        exist_dbf = os.path.isfile(path_shp[:-3] + 'dbf')
        assert exist_shp == exist_dbf
        assert should_exist == exist_shp

        if not is_anonymous:
            is_open_key = 'test' in ds
            is_open_prox = test in ds
            assert is_open_key == is_open_prox
            assert is_open_key == should_be_open
            if is_open_key:
                assert test is ds.test is ds['test']
        else:
            is_open = test in ds
            assert is_open == should_be_open

        if should_be_open:
            assert test.type == 'Point'
            assert len(test.fields) == 1
            assert (test.fields[0]['name'], test.fields[0]['type']) == ('area', 'real')
            assert buzz.srs.wkt_same(SRS[0]['wkt'], test.wkt_origin)
            assert len(test) == 0
            assert test.path == path_shp

    fields = [{'name': 'area', 'type': np.float64}]
    ds = buzz.DataSource()
    assert ds.proj4 is None
    assert ds.wkt is None

    # Vector test 1
    test = None
    _asserts(False, False)
    test = ds.create_vector('test', path_shp, 'point', fields, sr=SRS[0]['wkt'])
    _asserts(True, True)
    ds.test.close()
    _asserts(True, False)

    test = ds.open_avector(path_shp)
    _asserts(True, True, True)
    test.close()
    _asserts(True, False)

    test = ds.open_vector('test', path_shp, mode='w')
    _asserts(True, True)
    test.delete()
    _asserts(False, False)

    # Vector test 2
    with ds.create_vector('test', path_shp, 'point', fields, sr=SRS[0]['wkt']).close as test:
        _asserts(True, True)
    _asserts(True, False)
    with ds.open_vector('test', path_shp, mode='w').delete as test:
        _asserts(True, True)
    _asserts(False, False)

    # Vector test 3
    with ds.create_avector(path_shp, 'point', fields, sr=SRS[0]['wkt']).delete as test:
        _asserts(True, True, True)
    _asserts(False, False)

    # Vector test 4
    with ds.create_vector('test', path_shp, 'point', fields, sr=SRS[0]['wkt']).delete as test:
        _asserts(True, True)
    _asserts(False, False)
