"""Tests for DataSource class"""

# pylint: disable=redefined-outer-name

from __future__ import division, print_function
import logging
import os
import tempfile
import uuid

import numpy as np
from osgeo import gdal
import pytest
import shapely.geometry as sg

import buzzard as buzz
from .tools import SRS

LOGGER = logging.getLogger('buzzard')

FLOAT32_RASTER_STRANGE = ('float32', 1, {
    'nodata': [42.0],
    'interpretation': ['greenband'],
    'offset': [0.0],
    'scale': [1.0],
    'mask': ['nodata']
})
FLOAT32_RASTER_NODATA0 = ('float32', 1, {
    'nodata': [0.0],
    'interpretation': ['undefined'],
    'offset': [0.0],
    'scale': [1.0],
    'mask': ['nodata']
})
FLOAT32_RASTER_NODATA32768 = ('float32', 1, {
    'nodata': [-32768.0],
    'interpretation': ['undefined'],
    'offset': [0.0],
    'scale': [1.0],
    'mask': ['nodata']
})
FLOAT32_RASTER_NODATA32768_GRAY = ('float32', 1, {
    'nodata': [-32768.0],
    'interpretation': ['grayindex'],
    'offset': [0.0],
    'scale': [1.0],
    'mask': ['nodata']
})

FLOAT64_RASTER_STRANGE = ('float64', 1, {
    'nodata': [42.0],
    'interpretation': ['greenband'],
    'offset': [0.0],
    'scale': [1.0],
    'mask': ['nodata']
})
FLOAT64_RASTER_NODATA0 = ('float64', 1, {
    'nodata': [0.0],
    'interpretation': ['undefined'],
    'offset': [0.0],
    'scale': [1.0],
    'mask': ['nodata']
})

BYTE_4RASTER_STRANGE = ('uint8', 4, {
    'nodata': [None, None, None, None],
    'interpretation': ['hueband', 'greenband', 'blueband', 'cyanband'],
    'offset': [0.0, 0.0, 0.0, 0.0],
    'scale': [1.0, 1.0, 1.0, 1.0],
    'mask': ['alpha', 'all_valid', 'alpha', 'all_valid'],
})

BYTE_3RASTER_STRANGE = ('uint8', 3, {
    'nodata': [None, None, None],
    'interpretation': ['hueband', 'greenband', 'blueband'],
    'offset': [0.0, 0.0, 0.0],
    'scale': [1.0, 1.0, 1.0],
    'mask': ['alpha', 'all_valid', 'alpha'],
})
BYTE_3RASTER_NORMAL = ('uint8', 3, {
    'nodata': [None, None, None],
    'interpretation': ['redband', 'greenband', 'blueband'],
    'offset': [0.0, 0.0, 0.0],
    'scale': [1.0, 1.0, 1.0],
    'mask': ['all_valid', 'all_valid', 'all_valid'],
})
BYTE_3RASTER_NODATA0 = ('uint8', 3, {
    'nodata': [0., 0., 0.],
    'interpretation': ['redband', 'greenband', 'blueband'],
    'offset': [0.0, 0.0, 0.0],
    'scale': [1.0, 1.0, 1.0],
    'mask': ['nodata', 'nodata', 'nodata'],
})

BYTE_1RASTER_ALPHA_UNDEF = ('uint8', 1, {
    'nodata': [255.],
    'interpretation': ['undefined'],
    'offset': [0.0],
    'scale': [1.0],
    'mask': ['alpha'],
})


def pytest_generate_tests(metafunc):
    if 'test_fields' in metafunc.fixturenames:
        metafunc.parametrize(
            argnames='driver,suffix,test_fields',
            argvalues=[
                ('ESRI Shapefile', '.shp', True),
                ('GeoJson', '.json', True),

                ('BNA', '.bna', False), # custom fields
                ('DGN', '.dgn', False), # custom fields
                ('DXF', '.dxf', False), # custom fields

                # ('GMT', '.gmt', True), # can't create
                # ('PGDump', '.sql', True), # can't create
                # ('MapInfo File', '.MIF', True), # can't create
                # ('WAsP', '.map', True), # can't create

                # ('Selafin', '.loool', True), # only points
                # ('GPSTrackMaker', '.gtm', True), # no polygon

                # ('Geoconcept', '.txt', True), # only strings
            ],
        )
    elif 'save_proj' in metafunc.fixturenames:
        metafunc.parametrize(
            argnames='driver,suffix,options,save_proj,band_details',
            argvalues=[
                ('GTiff', '.tif', [], True, FLOAT32_RASTER_STRANGE),
                ('GTiff', '.tif', [], True, FLOAT64_RASTER_STRANGE),
                ('GTiff', '.tif', [], True, BYTE_3RASTER_NORMAL),
                ('RMF', '.rsw', ['MTW=OFF'], False, BYTE_3RASTER_NODATA0),
                ('RMF', '.mtw', ['MTW=ON'], False, FLOAT64_RASTER_NODATA0),
                ('BMP', '.bmp', [], True, BYTE_3RASTER_NORMAL),
                ('BMP', '.bmp', [], True, BYTE_3RASTER_NODATA0),
                ('ERS', '.ers', [], False, FLOAT32_RASTER_NODATA0),
                ('ERS', '.ers', [], False, FLOAT64_RASTER_NODATA0),
                ('BT', '.bt', [], True, FLOAT32_RASTER_NODATA32768),
                ('RST', '.rst', [], False, FLOAT32_RASTER_NODATA32768_GRAY),
                ('GTX', '.gtx', [], False, FLOAT32_RASTER_NODATA32768),
                # ('IDA', '.ida', [], False, BYTE_1RASTER_ALPHA_UNDEF),
            ],
        )


@pytest.fixture()
def path(suffix, driver):
    """Create a temporary path, and take care of cleanup afterward"""
    path = '{}/{}{}'.format(tempfile.gettempdir(), uuid.uuid4(), suffix)
    yield path
    if os.path.isfile(path):
        try:
            gdal.GetDriverByName(driver).Delete(path)
        except:
            os.remove(path)


def test_raster(path, driver, band_details, options, save_proj):
    dtype, band_count, band_schema = band_details
    ds = buzz.DataSource()
    fp = buzz.Footprint(
        tl=(0, 10), size=(10, 10), rsize=(30, 30)
    )
    write = ds.create_araster(
        path, fp, dtype, band_count, band_schema, driver, options, SRS[0]['wkt'],
    )
    array = np.repeat(np.sum(fp.meshgrid_raster, 0)[:, :, np.newaxis], band_count, -1)
    write.set_data(array, band=np.arange(band_count) + 1)
    write.close()

    # open again and check
    read = ds.open_raster('read', path, driver=driver)
    array2 = read.get_data(band=np.arange(band_count) + 1)
    assert np.all(array == array2)
    assert len(read) == band_count
    assert read.fp == fp
    assert read.band_schema == band_schema
    if save_proj:
        assert buzz.srs.wkt_same(SRS[0]['wkt'], read.wkt_origin)

def test_vector(path, driver, test_fields):
    fields = [
        dict(name='name', type=str),
        dict(name='area', type='float32'),
        dict(name='count', type='int32'),
    ]
    ds = buzz.DataSource()
    if test_fields:
        out = ds.create_avector(path, 'polygon', fields, driver=driver, sr=SRS[0]['wkt'])
    else:
        out = ds.create_avector(path, 'polygon', [], driver=driver, sr=SRS[0]['wkt'])

    # insert features
    features = [
        [sg.box(0, 0, 1, 1), None, None, None, ],
        [sg.box(0, 0, 2, 2), 'ruelle', 42.5, 10, ],
        [sg.box(0, 0, 3, 3), 'avenue', None, 9, ],
    ]
    if test_fields:
        out.insert_data(features[0][0])
        out.insert_data(features[1][0], features[1][1:])
        out.insert_data(features[2][0], {
            defn['name']: val
            for (defn, val) in zip(fields, features[2][1:])
        })
    else:
        for data in features:
            out.insert_data(data[0])
    out.close()

    # close/open and check
    out = ds.open_vector('out', path, mode='r', driver=driver)
    for input, output in zip(features, out.iter_data()):
        if test_fields:
            assert all(v1 == v2 for (v1, v2) in zip(input[1:], output[1:]))
