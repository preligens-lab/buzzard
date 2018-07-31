
# pylint: disable=redefined-outer-name

from __future__ import division, print_function
import itertools
import os
import uuid
import tempfile

import numpy as np
import pytest
from osgeo import gdal

from .tools import  get_srs_by_name
from buzzard import Footprint, DataSource

SR1 = get_srs_by_name('EPSG:2154')

DRIVER_STORES_SRS = {
    'GTiff': True,
    'BMP': True,
    'BT': True,
    'RMF': False,
    'ERS': False,
    'GTX': False,
    'RST': False,
    'MEM': True,
}

TESTS = [
    dict(
        subtests=[
            dict(driver='GTiff', ext='.tif', options=[]),
            dict(driver='MEM', ext='', options=[]),
        ],
        dtype='float32',
        band_count=1,
        band_schema= {
            'nodata': [42.0],
            'interpretation': ['greenband'],
            'offset': [0.0],
            'scale': [1.0],
            'mask': ['nodata']
        },
        sr=SR1['wkt'],
    ),

    dict(
        subtests=[
            dict(driver='ERS', ext='.ers', options=[]),
            dict(driver='MEM', ext='', options=[]),
        ],
        dtype='float32',
        band_count=1,
        band_schema= {
            'nodata': [0.0],
            'interpretation': ['undefined'],
            'offset': [0.0],
            'scale': [1.0],
            'mask': ['nodata']
        },
        sr=SR1['wkt'],
    ),

    dict(
        subtests=[
            dict(driver='BT', ext='.bt', options=[]),
            dict(driver='GTX', ext='.gtx', options=[]),
            dict(driver='MEM', ext='', options=[]),
        ],
        dtype='float32',
        band_count=1,
        band_schema= {
            'nodata': [-32768.0],
            'interpretation': ['undefined'],
            'offset': [0.0],
            'scale': [1.0],
            'mask': ['nodata']
        },
        sr=SR1['wkt'],
    ),

    dict(
        subtests=[
            dict(driver='RST', ext='.rst', options=[]),
            dict(driver='MEM', ext='', options=[]),
        ],
        dtype='float32',
        band_count=1,
        band_schema= {
            'nodata': [-32768.0],
            'interpretation': ['grayindex'],
            'offset': [0.0],
            'scale': [1.0],
            'mask': ['nodata']
        },
        sr=SR1['wkt'],
    ),

    dict(
        subtests=[
            dict(driver='GTiff', ext='.tif', options=[]),
            dict(driver='MEM', ext='', options=[]),
        ],
        dtype='float64',
        band_count=1,
        band_schema= {
            'nodata': [42.0],
            'interpretation': ['greenband'],
            'offset': [0.0],
            'scale': [1.0],
            'mask': ['nodata']
        },
        sr=SR1['wkt'],
    ),

    dict(
        subtests=[
            dict(driver='RMF', ext='.mtw', options=['MTW=ON']),
            dict(driver='ERS', ext='.ers', options=[]),
            dict(driver='MEM', ext='', options=[]),
        ],
        dtype='float64',
        band_count=1,
        band_schema= {
            'nodata': [0.0],
            'interpretation': ['undefined'],
            'offset': [0.0],
            'scale': [1.0],
            'mask': ['nodata']
        },
        sr=SR1['wkt'],
    ),

    dict(
        subtests=[
            dict(driver='GTiff', ext='.tif', options=[]),
            dict(driver='BMP', ext='.bmp', options=[]),
            dict(driver='MEM', ext='', options=[]),
        ],
        dtype='uint8',
        band_count=3,
        band_schema= {
            'nodata': [None, None, None],
            'interpretation': ['redband', 'greenband', 'blueband'],
            'offset': [0.0, 0.0, 0.0],
            'scale': [1.0, 1.0, 1.0],
            'mask': ['all_valid', 'all_valid', 'all_valid'],
        },
        sr=SR1['wkt'],
    ),

    dict(
        subtests=[
            dict(driver='BMP', ext='.bmp', options=[]),
            dict(driver='RMF', ext='.rsw', options=['MTW=OFF']),
            dict(driver='MEM', ext='', options=[]),
        ],
        dtype='uint8',
        band_count=3,
        band_schema= {
            'nodata': [0., 0., 0.],
            'interpretation': ['redband', 'greenband', 'blueband'],
            'offset': [0.0, 0.0, 0.0],
            'scale': [1.0, 1.0, 1.0],
            'mask': ['nodata', 'nodata', 'nodata'],
        },
        sr=SR1['wkt'],
    ),
]

def pytest_generate_tests(metafunc):
    tests = []
    for test in TESTS:
        for subtest in test['subtests']:
            meta = dict(test)
            del meta['subtests']
            ext = subtest.pop('ext')
            meta.update(subtest)
            tests.append((meta, ext))
    metafunc.parametrize(
        argnames='meta,ext',
        argvalues=tests,
    )

@pytest.fixture
def path(meta, ext):
    path = '{}/{}{}'.format(tempfile.gettempdir(), uuid.uuid4(), ext)
    yield path
    if os.path.isfile(path):
        try:
            gdal.GetDriverByName(meta['driver']).Delete(path)
        except:
            os.remove(path)

def test_create_open(meta, path):

    ds = DataSource()
    fp = Footprint(
        tl=(0, 10), size=(10, 10), rsize=(30, 30)
    )
    arr = np.add(*fp.meshgrid_raster)
    arr = np.repeat(arr[..., np.newaxis], meta['band_count'], -1)

    with ds.acreate_raster(path, fp, **meta).close as r:
        r.set_data(arr, band=-1)
        # TODO: Test only specified fields of band_schema, add tests for classic rgb tif
        # TODO: Test Numpy attributes

        if DRIVER_STORES_SRS[meta['driver']]:
            assert r.wkt_stored == meta['sr']
            assert r.wkt_virtual == meta['sr']
        assert r.band_schema == meta['band_schema']
        assert r.dtype == np.dtype(meta['dtype'])
        assert r.fp_stored == fp
        assert r.nodata == meta['band_schema']['nodata'][0]
        assert len(r) == meta['band_count']
        assert r.fp == fp
        assert r.mode == 'w'
        assert r.driver == meta['driver']
        assert r.open_options == meta['options']
        if meta['driver'] != 'MEM':
            assert r.path == path
        assert np.all(r.get_data(band=[-1]) == arr)

    if meta['driver'] != 'MEM':
        assert os.path.isfile(path)
        with ds.aopen_raster(path, driver=meta['driver']).close as r:

            if DRIVER_STORES_SRS[meta['driver']]:
                assert r.wkt_stored == meta['sr']
                assert r.wkt_virtual == meta['sr']
            assert r.band_schema == meta['band_schema']
            assert r.dtype == np.dtype(meta['dtype'])
            assert r.fp_stored == fp
            assert r.nodata == meta['band_schema']['nodata'][0]
            assert len(r) == meta['band_count']
            assert r.fp == fp
            assert r.mode == 'r'
            assert r.driver == meta['driver']
            # assert r.open_options == meta['options']
            assert r.path == path
            assert np.all(r.get_data(band=[-1]) == arr)

            with pytest.raises(RuntimeError):
                r.delete()
        assert os.path.isfile(path)
        with ds.aopen_raster(path, driver=meta['driver'], mode='w').delete as r:
            assert r.mode == 'w'
        assert not os.path.isfile(path)
