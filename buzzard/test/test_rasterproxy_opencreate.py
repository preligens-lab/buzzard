
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

    dict( # Classic dsm
        subtests=[
            dict(driver='GTiff', ext='.tif', options=[]),
            dict(driver='MEM', options=[]),
            dict(driver='numpy'),
        ],
        dtype='float32',
        band_count=1,
        band_schema= {'nodata': [-32767]},
        sr=SR1['wkt'],
        fp=Footprint(tl=(0, 10), size=(10, 10), rsize=(30, 30)),
    ),

    dict( # Classic rgb
        subtests=[
            dict(driver='GTiff', ext='.tif', options=[]),
            dict(driver='MEM', options=[]),
            dict(driver='numpy'),
        ],
        dtype='uint8',
        band_count=3,
        band_schema= {},
        sr=SR1['wkt'],
        fp=Footprint(tl=(0, 10), size=(10, 10), rsize=(30, 30)),
    ),

    dict( # Classic rgba
        subtests=[
            dict(driver='GTiff', ext='.tif', options=[]),
            dict(driver='MEM', options=[]),
            dict(driver='numpy'),
        ],
        dtype='uint8',
        band_count=4,
        band_schema= {},
        sr=SR1['wkt'],
        fp=Footprint(tl=(0, 10), size=(10, 10), rsize=(30, 30)),
    ),

    dict( # Ununsual band_schema
        subtests=[
            dict(driver='GTiff', ext='.tif', options=[]),
            dict(driver='MEM', options=[]),
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
        fp=Footprint(tl=(0, 10), size=(10, 10), rsize=(30, 30)),
    ),

    dict( # Ununsual band_schema
        subtests=[
            dict(driver='ERS', ext='.ers', options=[]),
            dict(driver='MEM', options=[]),
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
        fp=Footprint(tl=(0, 10), size=(10, 10), rsize=(30, 30)),
    ),

    dict( # Ununsual band_schema
        subtests=[
            dict(driver='BT', ext='.bt', options=[]),
            dict(driver='GTX', ext='.gtx', options=[]),
            dict(driver='MEM', options=[]),
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
        fp=Footprint(tl=(0, 10), size=(10, 10), rsize=(30, 30)),
    ),

    dict( # Ununsual band_schema
        subtests=[
            dict(driver='RST', ext='.rst', options=[]),
            dict(driver='MEM', options=[]),
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
        fp=Footprint(tl=(0, 10), size=(10, 10), rsize=(30, 30)),
    ),

    dict( # Ununsual band_schema
        subtests=[
            dict(driver='GTiff', ext='.tif', options=[]),
            dict(driver='MEM', options=[]),
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
        fp=Footprint(tl=(0, 10), size=(10, 10), rsize=(30, 30)),
    ),

    dict( # Ununsual band_schema
        subtests=[
            dict(driver='RMF', ext='.mtw', options=['MTW=ON']),
            dict(driver='ERS', ext='.ers', options=[]),
            dict(driver='MEM', options=[]),
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
        fp=Footprint(tl=(0, 10), size=(10, 10), rsize=(30, 30)),
    ),

    dict( # Ununsual band_schema
        subtests=[
            dict(driver='GTiff', ext='.tif', options=[]),
            dict(driver='BMP', ext='.bmp', options=[]),
            dict(driver='MEM', options=[]),
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
        fp=Footprint(tl=(0, 10), size=(10, 10), rsize=(30, 30)),
    ),

    dict( # Ununsual band_schema
        subtests=[
            dict(driver='BMP', ext='.bmp', options=[]),
            dict(driver='RMF', ext='.rsw', options=['MTW=OFF']),
            dict(driver='MEM', options=[]),
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
        fp=Footprint(tl=(0, 10), size=(10, 10), rsize=(30, 30)),
    ),
]

def pytest_generate_tests(metafunc):
    tests = []
    for test in TESTS:
        for subtest in test['subtests']:
            subtest = dict(subtest)
            meta = dict(test)
            del meta['subtests']

            if 'meta_file' in metafunc.fixturenames and subtest['driver'] not in {'MEM', 'numpy'}:
                ext = subtest.pop('ext')
                meta.update(subtest)
                if meta['driver'] not in {'MEM'}:
                    tests.append((meta, ext))
            if 'meta_mem' in metafunc.fixturenames and subtest['driver'] == 'MEM':
                meta['path'] = ''
                meta.update(subtest)
                tests.append((meta, 'MEM', DataSource.acreate_raster))
            if 'meta_mem' in metafunc.fixturenames and subtest['driver'] == 'numpy':
                meta = dict(
                    fp=meta['fp'],
                    array=np.empty(
                        np.r_[meta['fp'].shape, meta['band_count']],
                        meta['dtype']
                    ),
                    band_schema=meta['band_schema'],
                    sr=meta['sr'],
                )
                tests.append((meta, 'numpy', DataSource.awrap_numpy_raster))

    if 'meta_file' in metafunc.fixturenames:
        metafunc.parametrize(
            argnames='meta_file,ext',
            argvalues=tests,
        )
    if 'meta_mem' in metafunc.fixturenames:
        metafunc.parametrize(
            argnames='meta_mem,driver,ds_method',
            argvalues=tests,
        )

@pytest.fixture
def path(meta_file, ext):
    path = '{}/{}{}'.format(tempfile.gettempdir(), uuid.uuid4(), ext)
    yield path
    if os.path.isfile(path):
        try:
            gdal.GetDriverByName(meta['driver']).Delete(path)
        except:
            os.remove(path)

def test_file(meta_file, path):
    meta = meta_file
    fp = meta['fp']

    ds = DataSource()
    arr = np.add(*fp.meshgrid_raster)
    arr = np.repeat(arr[..., np.newaxis], meta['band_count'], -1)

    with ds.acreate_raster(path, **meta).close as r:
        r.set_data(arr, band=-1)
        # TODO: Test Numpy attributes

        if DRIVER_STORES_SRS[meta['driver']]:
            assert r.wkt_stored == meta['sr']
            assert r.wkt_virtual == meta['sr']
        for k, v in meta['band_schema'].items():
            assert r.band_schema[k] == v
        assert r.dtype == np.dtype(meta['dtype'])
        assert r.fp_stored == fp
        if 'nodata' in meta['band_schema']:
            assert r.nodata == meta['band_schema']['nodata'][0]
        else:
            assert r.nodata == None
        assert len(r) == meta['band_count']
        assert r.fp == fp
        assert r.mode == 'w'
        assert r.driver == meta['driver']
        assert r.open_options == meta['options']
        assert r.path == path
        assert np.all(r.get_data(band=[-1]) == arr)

    assert os.path.isfile(path)
    with ds.aopen_raster(path, driver=meta['driver']).close as r:

        if DRIVER_STORES_SRS[meta['driver']]:
            assert r.wkt_stored == meta['sr']
            assert r.wkt_virtual == meta['sr']
        for k, v in meta['band_schema'].items():
            assert r.band_schema[k] == v
        assert r.dtype == np.dtype(meta['dtype'])
        assert r.fp_stored == fp
        if 'nodata' in meta['band_schema']:
            assert r.nodata == meta['band_schema']['nodata'][0]
        else:
            assert r.nodata == None
        assert len(r) == meta['band_count']
        assert r.fp == fp
        assert r.mode == 'r'
        assert r.driver == meta['driver']
        assert r.path == path
        assert np.all(r.get_data(band=[-1]) == arr)

        with pytest.raises(RuntimeError):
            r.delete()
    assert os.path.isfile(path)
    with ds.aopen_raster(path, driver=meta['driver'], mode='w').delete as r:
        assert r.mode == 'w'
    assert not os.path.isfile(path)

def test_mem(meta_mem, driver, ds_method):
    meta = meta_mem
    fp = meta['fp']
    if driver == 'MEM':
        band_count = meta['band_count']
        dtype = meta['dtype']
    elif driver == 'numpy':
        band_count = meta['array'].shape[-1]
        dtype = meta['array'].dtype
    else:
        assert False

    ds = DataSource()
    arr = np.add(*fp.meshgrid_raster)
    arr = np.repeat(arr[..., np.newaxis], band_count, -1)

    r = ds_method(ds, **meta)
    with r.close as r:
        r.set_data(arr, band=-1)

        assert r.wkt_stored == meta['sr']
        assert r.wkt_virtual == meta['sr']
        for k, v in meta['band_schema'].items():
            assert r.band_schema[k] == v
        assert r.dtype == np.dtype(dtype)
        assert r.fp_stored == fp
        if 'nodata' in meta['band_schema']:
            assert r.nodata == meta['band_schema']['nodata'][0]
        else:
            assert r.nodata == None
        assert len(r) == band_count
        assert r.fp == fp
        assert r.mode == 'w'
        assert np.all(r.get_data(band=[-1]) == arr)
