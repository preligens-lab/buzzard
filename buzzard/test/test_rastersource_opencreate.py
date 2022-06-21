"""Test:
- Raster opening
- Raster creation
- Attributes validity
- Raster deletion
"""

# pylint: disable=redefined-outer-name

import itertools
import os
import uuid
import tempfile

import numpy as np
import pytest
from osgeo import gdal

from .tools import  get_srs_by_name
from buzzard import Footprint, Dataset

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
        channel_count=1,
        channels_schema= {'nodata': [-32767]},
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
        channel_count=3,
        channels_schema= {},
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
        channel_count=4,
        channels_schema= {},
        sr=SR1['wkt'],
        fp=Footprint(tl=(0, 10), size=(10, 10), rsize=(30, 30)),
    ),

    dict( # Ununsual channels_schema
        subtests=[
            dict(driver='GTiff', ext='.tif', options=[]),
            dict(driver='MEM', options=[]),
        ],
        dtype='float32',
        channel_count=1,
        channels_schema= {
            'nodata': [42.0],
            'interpretation': ['greenband'],
            'offset': [0.0],
            'scale': [1.0],
            'mask': ['nodata']
        },
        sr=SR1['wkt'],
        fp=Footprint(tl=(0, 10), size=(10, 10), rsize=(30, 30)),
    ),

    dict( # Ununsual channels_schema
        subtests=[
            dict(driver='ERS', ext='.ers', options=[]),
            dict(driver='MEM', options=[]),
        ],
        dtype='float32',
        channel_count=1,
        channels_schema= {
            'nodata': [0.0],
            'interpretation': ['undefined'],
            'offset': [0.0],
            'scale': [1.0],
            'mask': ['nodata']
        },
        sr=SR1['wkt'],
        fp=Footprint(tl=(0, 10), size=(10, 10), rsize=(30, 30)),
    ),

    dict( # Ununsual channels_schema
        subtests=[
            dict(driver='BT', ext='.bt', options=[]),
            dict(driver='GTX', ext='.gtx', options=[]),
            dict(driver='MEM', options=[]),
        ],
        dtype='float32',
        channel_count=1,
        channels_schema= {
            'nodata': [-32768.0],
            'interpretation': ['undefined'],
            'offset': [0.0],
            'scale': [1.0],
            'mask': ['nodata']
        },
        sr=SR1['wkt'],
        fp=Footprint(tl=(0, 10), size=(10, 10), rsize=(30, 30)),
    ),

    dict( # Ununsual channels_schema
        subtests=[
            dict(driver='RST', ext='.rst', options=[]),
            dict(driver='MEM', options=[]),
        ],
        dtype='float32',
        channel_count=1,
        channels_schema= {
            'nodata': [-32768.0],
            'interpretation': ['grayindex'],
            'offset': [0.0],
            'scale': [1.0],
            'mask': ['nodata']
        },
        sr=SR1['wkt'],
        fp=Footprint(tl=(0, 10), size=(10, 10), rsize=(30, 30)),
    ),

    dict( # Ununsual channels_schema
        subtests=[
            dict(driver='GTiff', ext='.tif', options=[]),
            dict(driver='MEM', options=[]),
        ],
        dtype='float64',
        channel_count=1,
        channels_schema= {
            'nodata': [42.0],
            'interpretation': ['greenband'],
            'offset': [0.0],
            'scale': [1.0],
            'mask': ['nodata']
        },
        sr=SR1['wkt'],
        fp=Footprint(tl=(0, 10), size=(10, 10), rsize=(30, 30)),
    ),

    dict( # Ununsual channels_schema
        subtests=[
            dict(driver='RMF', ext='.mtw', options=['MTW=ON']),
            dict(driver='ERS', ext='.ers', options=[]),
            dict(driver='MEM', options=[]),
        ],
        dtype='float64',
        channel_count=1,
        channels_schema= {
            'nodata': [0.0],
            'interpretation': ['undefined'],
            'offset': [0.0],
            'scale': [1.0],
            'mask': ['nodata']
        },
        sr=SR1['wkt'],
        fp=Footprint(tl=(0, 10), size=(10, 10), rsize=(30, 30)),
    ),

    dict( # Ununsual channels_schema
        subtests=[
            dict(driver='GTiff', ext='.tif', options=[]),
            dict(driver='BMP', ext='.bmp', options=[]),
            dict(driver='MEM', options=[]),
        ],
        dtype='uint8',
        channel_count=3,
        channels_schema= {
            'nodata': [None, None, None],
            'interpretation': ['redband', 'greenband', 'blueband'],
            'offset': [0.0, 0.0, 0.0],
            'scale': [1.0, 1.0, 1.0],
            'mask': ['all_valid', 'all_valid', 'all_valid'],
        },
        sr=SR1['wkt'],
        fp=Footprint(tl=(0, 10), size=(10, 10), rsize=(30, 30)),
    ),

    dict( # Ununsual channels_schema
        subtests=[
            dict(driver='BMP', ext='.bmp', options=[]),
            dict(driver='RMF', ext='.rsw', options=['MTW=OFF']),
            dict(driver='MEM', options=[]),
        ],
        dtype='uint8',
        channel_count=3,
        channels_schema= {
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
                tests.append((meta, ext))
            if 'meta_mem' in metafunc.fixturenames and subtest['driver'] == 'MEM':
                meta['path'] = ''
                meta.update(subtest)
                tests.append(meta)
            if 'meta_numpy' in metafunc.fixturenames and subtest['driver'] == 'numpy':
                meta = dict(
                    fp=meta['fp'],
                    array=np.empty(
                        np.r_[meta['fp'].shape, meta['channel_count']],
                        meta['dtype']
                    ),
                    channels_schema=meta['channels_schema'],
                    sr=meta['sr'],
                )
                tests.append(meta)

    if 'meta_file' in metafunc.fixturenames:
        metafunc.parametrize(
            argnames='meta_file,ext',
            argvalues=tests,
        )
    if 'meta_mem' in metafunc.fixturenames:
        metafunc.parametrize(
            argnames='meta_mem',
            argvalues=tests,
        )
    if 'meta_numpy' in metafunc.fixturenames:
        metafunc.parametrize(
            argnames='meta_numpy',
            argvalues=tests,
        )

@pytest.fixture
def path(meta_file, ext):
    path = f'{tempfile.gettempdir()}/{uuid.uuid4()}{ext}'
    yield path
    if os.path.isfile(path):
        try:
            gdal.GetDriverByName(meta['driver']).Delete(path)
        except:
            os.remove(path)

def test_file(meta_file, path):
    meta = meta_file
    fp = meta['fp']

    ds = Dataset()
    arr = np.add(*fp.meshgrid_raster)
    arr = np.repeat(arr[..., np.newaxis], meta['channel_count'], -1)

    with ds.acreate_raster(path, **meta).close as r:
        r.set_data(arr)

        if DRIVER_STORES_SRS[meta['driver']]:
            assert r.wkt_stored == meta['sr']
            assert r.wkt_virtual == meta['sr']
        for k, v in meta['channels_schema'].items():
            assert r.channels_schema[k] == v
        assert r.dtype == np.dtype(meta['dtype'])
        assert r.fp_stored == fp
        if 'nodata' in meta['channels_schema']:
            assert r.nodata == meta['channels_schema']['nodata'][0]
        else:
            assert r.nodata == None
        assert len(r) == meta['channel_count']
        assert r.fp == fp
        assert r.mode == 'w'
        assert r.driver == meta['driver']
        assert r.open_options == meta['options']
        assert r.path == path
        assert np.all(r.get_data(channels=slice(None)) == arr)

    assert os.path.isfile(path)
    with ds.aopen_raster(path, driver=meta['driver']).close as r:

        if DRIVER_STORES_SRS[meta['driver']]:
            assert r.wkt_stored == meta['sr']
            assert r.wkt_virtual == meta['sr']
        for k, v in meta['channels_schema'].items():
            assert r.channels_schema[k] == v
        assert r.dtype == np.dtype(meta['dtype'])
        assert r.fp_stored.almost_equals(fp)
        if 'nodata' in meta['channels_schema']:
            assert r.nodata == meta['channels_schema']['nodata'][0]
        else:
            assert r.nodata == None
        assert len(r) == meta['channel_count']
        assert r.fp.almost_equals(fp)
        assert r.mode == 'r'
        assert r.driver == meta['driver']
        assert r.path == path
        assert np.all(r.get_data(channels=slice(None)) == arr)

        with pytest.raises(RuntimeError):
            r.delete()

    # Test error - driver name
    meta = meta_file.copy()
    meta['driver'] = 'Truc42'
    with pytest.raises(ValueError, match='driver'):
        ds.acreate_raster(path, **meta)

    # # Test error - need overwrite
    meta = meta_file.copy()
    meta['ow'] = False
    with pytest.raises(RuntimeError, match='overwrite'):
        ds.acreate_raster(path, **meta)

    # Test deletion
    with ds.aopen_raster(path, driver=meta['driver'], mode='w').delete as r:
        assert r.mode == 'w'
    assert not os.path.exists(path)

    # Test error - open
    with pytest.raises(RuntimeError, match='Could not open'):
        ds.aopen_raster(path, driver=meta['driver'])

    # Test error - bad sr
    meta = meta_file.copy()
    meta['sr'] = "ca n'existe pas"
    with pytest.raises(ValueError, match='wkt'):
        ds.acreate_raster(path, **meta)

    # Test error - failed file deletion for overwrite
    if meta['driver'].lower() == 'gtiff':
        try:
            os.mkdir(path)
            os.chmod(path, int('000', 8))
            meta = meta_file.copy()
            meta['ow'] = True
            with pytest.raises(RuntimeError, match='Could not delete'):
                ds.acreate_raster(path, **meta)
        finally:
            os.chmod(path, int('777', 8))
            os.rmdir(path)

def test_mem(meta_mem):
    meta = meta_mem
    fp = meta['fp']

    ds = Dataset()
    arr = np.add(*fp.meshgrid_raster)
    arr = np.repeat(arr[..., np.newaxis], meta['channel_count'], -1)

    with ds.acreate_raster(**meta).close as r:
        r.set_data(arr)

        assert r.wkt_stored == meta['sr']
        assert r.wkt_virtual == meta['sr']
        for k, v in meta['channels_schema'].items():
            assert r.channels_schema[k] == v
        assert r.dtype == np.dtype(meta['dtype'])
        assert r.fp_stored == fp
        if 'nodata' in meta['channels_schema']:
            assert r.nodata == meta['channels_schema']['nodata'][0]
        else:
            assert r.nodata == None
        assert len(r) == meta['channel_count']
        assert r.fp == fp
        assert r.mode == 'w'
        assert np.all(r.get_data(channels=slice(None)) == arr)

def test_numpy(meta_numpy):
    meta = meta_numpy
    arrptr = meta_numpy['array'].__array_interface__['data'][0]

    fp = meta['fp']

    ds = Dataset()
    arr = np.add(*fp.meshgrid_raster)
    arr = np.repeat(arr[..., np.newaxis], meta['array'].shape[-1], -1)

    with ds.awrap_numpy_raster(**meta).close as r:
        r.set_data(arr)

        assert r.wkt_stored == meta['sr']
        assert r.wkt_virtual == meta['sr']
        for k, v in meta['channels_schema'].items():
            assert r.channels_schema[k] == tuple(v)
        assert r.dtype == meta['array'].dtype
        assert r.fp_stored == fp
        if 'nodata' in meta['channels_schema']:
            assert r.nodata == meta['channels_schema']['nodata'][0]
        else:
            assert r.nodata == None
        assert len(r) == meta['array'].shape[-1]
        assert r.fp == fp
        assert r.mode == 'w'
        assert np.all(r.get_data(channels=slice(None)) == arr)
        r.array
        assert (
            meta_numpy['array'].__array_interface__['data'][0] ==
            r.array.__array_interface__['data'][0]
        )
