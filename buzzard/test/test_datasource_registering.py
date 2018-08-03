# pylint: disable=redefined-outer-name, unused-argument

from __future__ import division, print_function
import os
import tempfile
import uuid
import string
import weakref
import gc

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
def random_path_tif():
    """Create a temporary path, and take care of cleanup afterward"""
    path = '{}/{}.tif'.format(tempfile.gettempdir(), uuid.uuid4())
    yield path
    if os.path.isfile(path):
        try:
            gdal.GetDriverByName('GTiff').Delete(path)
        except:
            os.remove(path)

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

def test_raster(fps, random_path_tif):

    def _asserts(should_exist, should_be_open, is_anonymous=False):

        exist = os.path.isfile(random_path_tif)
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

        if should_be_open and not is_anonymous:
            assert ds['test'] == ds.test
        if should_be_open:
            assert len(ds) == 1
        else:
            assert len(ds) == 0

    ds = buzz.DataSource()

    # Raster test 1
    test = None
    _asserts(False, False)
    test = ds.create_raster('test', random_path_tif, fps.A, float, 1)
    _asserts(True, True)
    ds.test.close()
    _asserts(True, False)

    test = ds.aopen_raster(random_path_tif)
    _asserts(True, True, True)
    test.close()
    _asserts(True, False)

    test = ds.open_raster('test', random_path_tif, mode='w')
    _asserts(True, True)
    test.delete()
    _asserts(False, False)

    # Raster test 2 - context/close
    with ds.create_raster('test', random_path_tif, fps.A, float, 1).close as test:
        _asserts(True, True)
    _asserts(True, False)
    with ds.open_raster('test', random_path_tif, mode='w').delete as test:
        _asserts(True, True)
    _asserts(False, False)

    # Raster test 3 - context/close/anonymous
    with ds.acreate_raster(random_path_tif, fps.A, float, 1).delete as test:
        _asserts(True, True, True)
    _asserts(False, False)

    # Raster test 4 - context/delete
    with ds.create_raster('test', random_path_tif, fps.A, float, 1).delete as test:
        _asserts(True, True)
    _asserts(False, False)

    # Raster test 5 - MEM
    with ds.create_raster('test', '', fps.A, float, 1, driver='MEM').close as test:
        _asserts(False, True)

    # Raster test 6 - numpy
    with ds.wrap_numpy_raster('test', fps.A, np.zeros(fps.A.shape)).close as test:
        _asserts(False, True)

    # Raster test 7 - gc
    del ds
    ws = weakref.WeakSet()
    def _test():
        ds = buzz.DataSource()
        assert len(ds) == len(ws) == 0

        prox = ds.create_raster('test', random_path_tif, fps.A, float, 1)
        ws.add(prox)
        assert len(ds) == len(ws) == 1

        prox = ds.acreate_raster('', fps.A, float, 1, driver='MEM')
        ws.add(prox)
        assert len(ds) == len(ws) == 2

        prox = ds.awrap_numpy_raster(fps.A, np.zeros(fps.A.shape))
        ws.add(prox)
        assert len(ds) == len(ws) == 3

        ws.add(ds)
        assert len(ws) == 4

    _test()
    gc.collect()
    assert len(ws) == 0


def test_vector(fps, random_path_shp):

    def _asserts(should_exist, should_be_open, is_anonymous=False):

        exist = os.path.isfile(random_path_shp)
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

        if should_be_open and not is_anonymous:
            assert ds['test'] == ds.test
        if should_be_open:
            assert len(ds) == 1
        else:
            assert len(ds) == 0

    ds = buzz.DataSource()

    # Vector test 1
    test = None
    _asserts(False, False)
    test = ds.create_vector('test', random_path_shp, 'polygon')
    _asserts(True, True)
    ds.test.close()
    _asserts(True, False)

    test = ds.aopen_vector(random_path_shp)
    _asserts(True, True, True)
    test.close()
    _asserts(True, False)

    test = ds.open_vector('test', random_path_shp, mode='w')
    _asserts(True, True)
    test.delete()
    _asserts(False, False)

    # Vector test 2 - context/close
    with ds.create_vector('test', random_path_shp, 'polygon').close as test:
        _asserts(True, True)
    _asserts(True, False)
    with ds.open_vector('test', random_path_shp, mode='w').delete as test:
        _asserts(True, True)
    _asserts(False, False)

    # Vector test 3 - context/close/anonymous
    with ds.acreate_vector(random_path_shp, 'polygon').delete as test:
        _asserts(True, True, True)
    _asserts(False, False)

    # Vector test 4 - context/delete
    with ds.create_vector('test', random_path_shp, 'polygon').delete as test:
        _asserts(True, True)
    _asserts(False, False)

    # Vector test 5 - MEM
    with ds.create_vector('test', '', 'polygon', driver='Memory').close as test:
        _asserts(False, True)

    # Vector test 6 - gc
    del ds
    ws = weakref.WeakSet()
    def _test():
        ds = buzz.DataSource()
        assert len(ds) == len(ws) == 0

        prox = ds.create_vector('test', random_path_shp, 'polygon')
        ws.add(prox)
        assert len(ds) == len(ws) == 1

        prox = ds.acreate_vector('', 'polygon', driver='Memory')
        ws.add(prox)
        assert len(ds) == len(ws) == 2

        ws.add(ds)
        assert len(ws) == 3

    _test()
    gc.collect()
    assert len(ws) == 0
