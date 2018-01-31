"""Test big files creation / write / read"""

# pylint: disable=redefined-outer-name

from __future__ import division, print_function
import tempfile
import uuid
import os

import numpy as np
import pytest
from osgeo import gdal

import buzzard as buzz
from buzzard.test import make_tile_set
from .tools import SRS

@pytest.fixture(scope='module')
def fps():
    """
    len(fps) = 784
    A B C D E F G
    H I J K L M N
    O P Q R S T U
    V W X Y Z a b
    c d e f g h i
    j k l m n o p
    q r s t u v w
    """
    return make_tile_set.make_tile_set(
        7, [1, -1], (6480, -6480)
    )

def pytest_generate_tests(metafunc):
    """Fixture generator"""
    if 'tif_options' in metafunc.fixturenames:
        metafunc.parametrize(
            argnames='tif_options,rast_fp_name',
            argvalues=[
                [['BIGTIFF=NO', 'SPARSE_OK=FALSE'], 'Ag'],
                [['BIGTIFF=YES', 'SPARSE_OK=FALSE'], 'Ao'],
                [['BIGTIFF=NO', 'SPARSE_OK=TRUE'], 'Ag'],
                [['BIGTIFF=YES', 'SPARSE_OK=TRUE'], 'Ag'],
                [['BIGTIFF=YES', 'SPARSE_OK=TRUE'], 'Aw'],
                [['SPARSE_OK=TRUE'], 'Aw'],
            ],
        )

@pytest.mark.skip(reason="No enough RAM on circleci")
def test_truc(fps, tif_options, rast_fp_name):
    path = '{}/{}{}'.format(tempfile.gettempdir(), uuid.uuid4(), '.tif')
    try:
        _launch_test(fps, tif_options, rast_fp_name, path)
    finally:
        print('Removing', path, 'of size', os.path.getsize(path) / 1024 ** 3, 'GB')
        if os.path.isfile(path):
            try:
                gdal.GetDriverByName('GTiff').Delete(path)
            except:
                os.remove(path)

def _launch_test(fps, tif_options, rast_fp_name, path):
    fp = fps[rast_fp_name]
    print('Working with fp={}, with options={}, ~size={:.3} GB'.format(
        fp, tif_options,
        4 * fp.rarea / 1024**3,
    ))
    ds = buzz.DataSource()
    dsm = ds.create_raster(
        path, fp, np.float32, 1, {'nodata': -32727}, key='dsm', sr=SRS[0]['wkt'],
        options=tif_options,
    )
    fp_l = [fps.A, fps.Q, fps.L]

    tile_size = (50000, 1000) # small height, full width, to wrap file's bands

    def _build_ar(shape):
        ar = np.ones(shape, dtype='float32')
        diag_indices = np.diag_indices_from(ar[:min(shape), :min(shape)])
        ar[diag_indices] = dsm.nodata
        return ar

    for fp in fp_l:
        print('set', fp)
        assert fp.poly.within(dsm.fp.poly)
        tiles = fp.tile(tile_size, boundary_effect='shrink') # ~1GB with float32
        for tile in tiles.flatten():
            ar = _build_ar(tile.shape)
            print("  ar size {}GB".format(ar.dtype.itemsize * np.prod(ar.shape) / 1024 ** 3))
            dsm.set_data(ar, fp=tile)

    dsm.close()

    dsm = ds.open_raster('dsm', path)
    for fp in fp_l:
        print('check', fp)
        assert fp.poly.within(dsm.fp.poly)

        tiles = fp.tile(tile_size, boundary_effect='shrink') # ~1GB with float32
        for tile in tiles.flatten():
            print('  ', tile)
            ar = _build_ar(tile.shape)
            ar2 = dsm.get_data(fp=tile)
            print("  ar size {}GB".format(ar.dtype.itemsize * np.prod(ar.shape) / 1024 ** 3))
            print("  ar2 size {}GB".format(ar2.dtype.itemsize * np.prod(ar2.shape) / 1024 ** 3))
            same = ar == ar2
            is_ok = same.all()
            assert is_ok
    dsm.close()
