"""Test:
- Vector opening
- Vector creation
- Attributes validity
- Vector deletion
"""

# pylint: disable=redefined-outer-name

from __future__ import division, print_function
import itertools
import os
import uuid
import tempfile
import operator
from pprint import pprint

import numpy as np
import pytest
from osgeo import gdal
import shapely.geometry as sg

from .tools import get_srs_by_name, eq
from buzzard import Footprint, DataSource, srs

# SR1 = get_srs_by_name('EPSG:2154')
SR1 = dict(wkt="""GEOGCS["WGS 84",DATUM["WGS_1984",SPHEROID["WGS 84",6378137,298.257223563,AUTHORITY["EPSG","7030"]],AUTHORITY["EPSG","6326"]],PRIMEM["Greenwich",0,AUTHORITY["EPSG","8901"]],UNIT["degree",0.01745329251994328,AUTHORITY["EPSG","9122"]],AUTHORITY["EPSG","4326"]]""")

DRIVER_LIST = [
    'ESRI Shapefile',
    'GeoJson',
    'DGN',
    'DXF',
    'Memory',
]

DRIVER_STORES_SRS = {
    'ESRI Shapefile': True,
    'GeoJson': False,
    'DGN': False,
    'DXF': False,
    'Memory': True,
}

DRIVER_STORES_GTYPE = {
    'ESRI Shapefile': True,
    'GeoJson': True,
    'DGN': False,
    'DXF': False,
    'Memory': True,
}

DRIVER_SUPPORT_EMPTY_COLLECTION = {
    'ESRI Shapefile': True,
    'GeoJson': True,
    'DGN': False,
    'DXF': False,
    'Memory': True,
}

EXTENSION_OF_DRIVER = {
    'ESRI Shapefile': '.shp',
    'GeoJson': '.json',
    'DGN': '.dgn',
    'DXF': '.dxf',
}

GTYPES_OF_DRIVER = {
    'ESRI Shapefile': {
        'GeometryCollection',
        'GeometryCollection25D',
        'LinearRing',
        'LineString',
        'LineString25D',
        'MultiLineString',
        'MultiLineString25D',
        'MultiPoint',
        'MultiPoint25D',
        'MultiPolygon',
        'MultiPolygon25D',
        'Point',
        'Point25D',
        'Polygon',
        'Polygon25D',
    },

    'GeoJson': {
        'GeometryCollection',
        'GeometryCollection25D',
        'LinearRing',
        'LineString',
        'LineString25D',
        'MultiLineString',
        'MultiLineString25D',
        'MultiPoint',
        'MultiPoint25D',
        'MultiPolygon',
        'MultiPolygon25D',
        'Point',
        'Point25D',
        'Polygon',
        'Polygon25D',
    },

    'DGN': {
        'GeometryCollection',
        # 'GeometryCollection25D',
        'LinearRing',
        'LineString',
        # 'LineString25D',
        'MultiLineString',
        # 'MultiLineString25D',
        # 'MultiPoint',
        # 'MultiPoint25D',
        # 'MultiPolygon',
        # 'MultiPolygon25D',
        # 'Point',
        # 'Point25D',
        # 'Polygon',
        # 'Polygon25D',
    },

    'DXF': {
        'GeometryCollection',
        'GeometryCollection25D',
        'LinearRing',
        'LineString',
        'LineString25D',
        'MultiLineString',
        'MultiLineString25D',
        # 'MultiPoint',
        # 'MultiPoint25D',
        'MultiPolygon',
        'MultiPolygon25D',
        'Point',
        'Point25D',
        'Polygon',
        'Polygon25D',
    },

    'Memory': {
        'GeometryCollection',
        'GeometryCollection25D',
        'LinearRing',
        'LineString',
        'LineString25D',
        'MultiLineString',
        'MultiLineString25D',
        'MultiPoint',
        'MultiPoint25D',
        'MultiPolygon',
        'MultiPolygon25D',
        'Point',
        'Point25D',
        'Polygon',
        'Polygon25D',
    },
}


FTYPES_OF_DRIVER = {
    'ESRI Shapefile': {
       'integer64',
       'real',
       'string',
       # 'integer64list',
       # 'reallist',
       # 'stringlist',
    },

    'GeoJson': {
       'integer64',
       'real',
       'string',
       'integer64list',
       'reallist',
       'stringlist',
    },

    'DGN': {
       # 'integer64',
       # 'real',
       # 'string',
       # 'integer64list',
       # 'reallist',
       # 'stringlist',
    },

    'DXF': {
       # 'integer64',
       # 'real',
       # 'string',
       # 'integer64list',
       # 'reallist',
       # 'stringlist',
    },

    'Memory': {
       'integer64',
       'real',
       'string',
       'integer64list',
       'reallist',
       'stringlist',
    },

}

pt0 = sg.Point(0, 0)
mpt0 = sg.MultiPoint([])
mpt1 = sg.MultiPoint([pt0])

ptz0 = sg.Point(0, 0, 0)
mptz0 = sg.MultiPoint([])
mptz1 = sg.MultiPoint([ptz0])

ls0 = sg.LineString([[0, 0], [1, 1]])
mls0 = sg.MultiLineString([])
mls1 = sg.MultiLineString([ls0])

lsz0 = sg.LineString([[0, 0, 0], [1, 1, 1]])
mlsz0 = sg.MultiLineString([])
mlsz1 = sg.MultiLineString([lsz0])

p0 = sg.Polygon(
	([13, 10], [10, 10], [10, 13], [13, 13], [13, 10]),
	[([12, 11], [12, 12], [11, 12], [11, 11], [12, 11])],
)
mp0 = sg.MultiPolygon([])
mp1 = sg.MultiPolygon([p0])

pz0 = sg.Polygon(
	([13, 10, 42], [10, 10, 42], [10, 13, 42], [13, 13, 42], [13, 10, 42]),
	[([12, 11, 42], [12, 12, 42], [11, 12, 42], [11, 11, 42], [12, 11, 42])],
)
mpz0 = sg.MultiPolygon([])
mpz1 = sg.MultiPolygon([pz0])

GEOM_TESTS = [
    ['Point', []],
    ['Point', [pt0]],
    ['MultiPoint', []],
    ['MultiPoint', [mpt0, mpt1]],
    ['Point25D', []],
    ['Point25D', [ptz0]],
    ['MultiPoint25D', [mptz0, mptz1]],

	['LineString', []],
	['LineString', [ls0]],
	['MultiLineString', []],
	['MultiLineString', [mls0, mls1]],
	['LineString25D', []],
	['LineString25D', [lsz0]],
	['MultiLineString25D', []],
	['MultiLineString25D', [mlsz0, mlsz1]],

    ['Polygon', []],
    ['Polygon', [p0]],
    ['MultiPolygon', []],
    ['MultiPolygon', [mp0, mp1]],
    ['Polygon25D', []],
    ['Polygon25D', [pz0]],
    ['MultiPolygon25D', []],
    ['MultiPolygon25D', [mpz0, mpz1]],

    # 'GeometryCollection', 'GeometryCollection25D', 'LinearRing', 'LineString25D', 'MultiLineString25D', 'Point25D', 'MultiPoint25D', 'Polygon25D', 'MultiPolygon25D', 'LineString', 'MultiLineString', 'Point', 'MultiPoint', 'Polygon', 'MultiPolygon'
# ])
]

FIELD_TESTS = [[],
               [
    [dict(name='i', type=int), 42],
    [dict(name='f', type=float), 42.42],
    [dict(name='s', type=str), 'ft'],
    [dict(name='is1', type='int list'), ()],
    [dict(name='is2', type='int list'), (4, 2)],
    [dict(name='fs1', type='float list'), ()],
    [dict(name='fs2', type='float list'), (4.2, 4.2)],
    [dict(name='ss1', type='str list'), ()],
    [dict(name='ss2', type='str list'), ('f', 't')],
]
]

int, float

def pytest_generate_tests(metafunc):
    tests = []

    if 'driver_file' in metafunc.fixturenames:
        driver_list = [d for d in DRIVER_LIST if d != 'Memory']
    else:
        driver_list = [d for d in DRIVER_LIST if d == 'Memory']

    for driver in driver_list:
        for gtype, geoms in GEOM_TESTS:
            if gtype not in GTYPES_OF_DRIVER[driver]:
                continue

            mask = [
                not g.is_empty or DRIVER_SUPPORT_EMPTY_COLLECTION[driver]
                for g in geoms
            ]
            geoms = list(itertools.compress(geoms, mask))

            for field_test in FIELD_TESTS:
                ftype = list(map(operator.itemgetter(0), field_test))
                fields = list(map(operator.itemgetter(1), field_test))
                mask = [
                    d['type'] in FTYPES_OF_DRIVER[driver]
                    for d in ftype
                ]
                ftype = list(itertools.compress(ftype, mask))
                fields = list(itertools.compress(fields, mask))
                tests.append((
                    driver, gtype, geoms, ftype, fields
                ))
    if 'driver_file' in metafunc.fixturenames:
        metafunc.parametrize(
            argnames='driver_file,gtype,geoms,ftypes,fields',
            argvalues=tests,
        )
    else:
        metafunc.parametrize(
            argnames='driver_mem,gtype,geoms,ftypes,fields',
            argvalues=tests,
        )

@pytest.fixture
def path(driver_file):
    ext = EXTENSION_OF_DRIVER[driver_file]
    path = '{}/{}{}'.format(tempfile.gettempdir(), uuid.uuid4(), ext)
    yield path
    if os.path.isfile(path):
        try:
            gdal.GetDriverByName(driver_file).Delete(path)
        except:
            os.remove(path)

def test_file(path, driver_file, gtype, geoms, ftypes, fields):
    driver = driver_file
    ds = DataSource(allow_none_geometry=1)

    with ds.acreate_vector(path, gtype, ftypes, driver=driver, options=[], sr=SR1['wkt']).close as v:
        for geom in geoms:
            v.insert_data(geom, fields)

        # TESTS 0
        if DRIVER_STORES_SRS[driver]:
            assert srs.wkt_same(v.wkt_stored, SR1['wkt'])
            assert srs.wkt_same(v.wkt_virtual, SR1['wkt'])
        for ftype, ftype_stored in zip(ftypes, v.fields):
            for key, value in ftype.items():
                assert ftype_stored[key] == value
        assert v.mode == 'w'
        if DRIVER_STORES_GTYPE[driver]:
            assert v.type.lower() == gtype.lower()

    assert os.path.isfile(path)
    with ds.aopen_vector(path, driver=driver, options=[]).close as v:

        # TESTS 0
        if DRIVER_STORES_SRS[driver]:
            assert srs.wkt_same(v.wkt_stored, SR1['wkt'])
            assert srs.wkt_same(v.wkt_virtual, SR1['wkt'])
        for ftype, ftype_stored in zip(ftypes, v.fields):
            for key, value in ftype.items():
                assert ftype_stored[key] == value
        assert v.mode == 'r'
        # if DRIVER_STORES_GTYPE[driver]:
        #     assert v.type.lower() == gtype.lower()

        # TESTS 1
        datas = list(v.iter_data())
        assert len(datas) == len(geoms)
        assert len(v) == len(geoms)
        # assert v.layer in {0, ''}

        if not sg.GeometryCollection(geoms).is_empty:
            assert eq(
                v.bounds,
                v.bounds_stored,
                v.extent[[0, 2, 1, 3]],
                v.extent_stored[[0, 2, 1, 3]],
                sg.GeometryCollection(geoms).bounds
            )

        for geom, data in zip(geoms, datas):
            if not isinstance(data, tuple):
                data = (data,)
            read_geom, *read_fields = data
            if read_geom is None or read_geom.is_empty:
                assert geom.is_empty
            else:
                assert (geom ^ read_geom).is_empty, (
                    geom.wkt,
                    read_geom.wkt,
                )
        with pytest.raises(RuntimeError):
            v.delete()

    with ds.aopen_vector(path, driver=driver, options=[], mode='w').delete as v:
        assert v.mode == 'w'
    assert not os.path.isfile(path)


def test_mem(driver_mem, gtype, geoms, ftypes, fields):
    driver = driver_mem
    ds = DataSource(allow_none_geometry=1)

    with ds.acreate_vector('', gtype, ftypes, driver=driver, options=[], sr=SR1['wkt']).close as v:
        for geom in geoms:
            v.insert_data(geom, fields)

        # TESTS 0
        assert srs.wkt_same(v.wkt_stored, SR1['wkt'])
        assert srs.wkt_same(v.wkt_virtual, SR1['wkt'])
        for ftype, ftype_stored in zip(ftypes, v.fields):
            for key, value in ftype.items():
                assert ftype_stored[key] == value
        assert v.mode == 'w'
        assert v.type.lower() == gtype.lower()

        # TESTS 1
        datas = list(v.iter_data())
        assert len(datas) == len(geoms)
        assert len(v) == len(geoms)
        # assert v.layer in {0, ''}

        if not sg.GeometryCollection(geoms).is_empty:
            assert eq(
                v.bounds,
                v.bounds_stored,
                v.extent[[0, 2, 1, 3]],
                v.extent_stored[[0, 2, 1, 3]],
                sg.GeometryCollection(geoms).bounds
            )

        for geom, data in zip(geoms, datas):
            if not isinstance(data, tuple):
                data = (data,)
            read_geom, *read_fields = data
            if read_geom is None or read_geom.is_empty:
                assert geom.is_empty
            else:
                assert (geom ^ read_geom).is_empty, (
                    geom.wkt,
                    read_geom.wkt,
                )
