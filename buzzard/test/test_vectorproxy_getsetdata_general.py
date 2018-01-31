# pylint: disable=redefined-outer-name

from __future__ import division, print_function
import logging
import collections
import itertools
import tempfile
import os
import uuid

import numpy as np
import pytest
import shapely.geometry as sg
from osgeo import gdal

import buzzard as buzz
from buzzard.test import make_tile_set
from buzzard.test.tools import eqall
from .tools import SRS

LOGGER = logging.getLogger('buzzard')

@pytest.fixture(scope='module')
def fps():
    """
    len(fps) = 225
    See make_tile_set
    A B C D E
    F G H I J
    K L M N O
    P Q R S T
    U V W X Y
    """
    return make_tile_set.make_tile_set(5, [0.1, -0.1])

def pytest_generate_tests(metafunc):
    if 'test_fields' in metafunc.fixturenames:
        metafunc.parametrize(
            argnames='driver,suffix,test_fields,test_coords_insertion',
            argvalues=[
                ('ESRI Shapefile', '.shp', True, True), # 145% time
                ('GeoJson', '.json', True, True), # 100% time
                # ('BNA', '.bna', False, True), # 350% time
                # ('DGN', '.dgn', False, False), # 190% time
                # ('DXF', '.dxf', False, False), # 490% time
            ],
        )
    else:
        metafunc.parametrize(
            argnames='driver,suffix',
            argvalues=[
                ('ESRI Shapefile', '.shp'),
            ],
        )

@pytest.fixture()
def path(suffix, driver):
    """Create a temporary path, and take care of cleanup afterward"""
    path = '{}/{}{}'.format(tempfile.gettempdir(), uuid.uuid4(), suffix)
    yield path
    if os.path.isfile(path):
        try:
            dr = gdal.GetDriverByName(driver)
            dr.Delete(path)
        except:
            os.remove(path)


def test_run(path, driver, fps, test_fields, test_coords_insertion):
    ds = buzz.DataSource()

    if test_fields:
        fields = FIELDS
    else:
        fields = []

    geom_type = 'polygon'
    v = ds.create_avector(path, geom_type, fields, driver=driver, sr=SRS[0]['wkt'])

    def _build_data():
        rng = np.random.RandomState(42)
        for fpname, fp in fps.items():
            geom = _geom_of_fp(rng, fp, test_coords_insertion)
            fields = _fields_of_fp(rng, fp, fpname) # Keep invocation before `if`
            if test_fields:
                yield geom, fields
            else:
                yield geom,

    data = list(_build_data())
    for dat in data:
        v.insert_data(*dat)
    v.close()
    del ds

    _test_geom_read(path, driver, fps, data)
    if test_fields:
        _test_fields_read(path, driver, data)

# Test write slaves ***************************************************************************** **
FIELDS = [
    {'name': 'rarea', 'type': int},
    {'name': 'fpname', 'type': str},
    {'name': 'sqrtarea', 'type': float},
]

def _geom_of_fp(rng, fp, test_coords_insertion):
    poly = fp.poly
    i = rng.randint(2)
    if not test_coords_insertion:
        return poly
    if i:
        return poly
    else:
        coords = sg.mapping(poly)['coordinates']
        if rng.randint(2):
            coords = np.asarray(coords)
        return coords

def _fields_of_fp(rng, fp, fpname):
    full = [
        ('rarea', fp.rarea),
        ('fpname', fpname),
        ('sqrtarea', fp.area ** .5),
    ]

    if rng.randint(2):
        # Dict
        i = rng.randint(4)
        if i == 0:
            # Dict - All present
            return dict(full)
        elif i == 1:
            # Dict - All absent
            return {}
        elif i == 2:
            # Dict - All None
            return {field_def['name']: None for field_def in FIELDS}
        else:
            # Dict - Per field
            assert i == 3
            d = {}
            for k, v in full:
                i = rng.randint(3)
                if i == 0:
                    d[k] = v
                elif i == 1:
                    d[k] = None
                else:
                    assert i == 2
            return d
    else:
        # List
        i = rng.randint(4)
        if i == 0:
            # List - All present
            return [v for (_, v) in full]
        elif i == 1:
            # List - All absent
            return []
        elif i == 2:
            # List - All None
            return [None] * len(full)
        else:
            # List - Per field
            assert i == 3
            l = []
            for _, v in full:
                if rng.randint(2):
                    l.append(v)
                else:
                    l.append(None)
            return l

# Test read slaves 1 **************************************************************************** **
def _test_fields_read(path, driver, data):
    """Test fields reading with iter_data

    Not testing geojson, no testing get_* functions
    Not actually testing fields value,
    - testing that all test yield the same thing
    - testing that all lengths are ok
    """

    query_waysssss = [
        [[], '', None],
        [[0], 'rarea', ['rarea']],
        [[1], 'fpname', ['fpname']],
        [[2], 'sqrtarea', ['sqrtarea']],
        [[0, 1], 'rarea,fpname', ['rarea', 'fpname'], [0, 'fpname'], ['rarea', 1]],
        [[0, 2], 'rarea,sqrtarea', ['rarea', 'sqrtarea'], [0, 'sqrtarea'], ['rarea', 2]],
        [[1, 0], 'fpname,rarea', ['fpname', 'rarea'], [1, 'rarea'], ['fpname', 0]],
        [[1, 2], 'fpname,sqrtarea', ['fpname', 'sqrtarea'], [1, 'sqrtarea'], ['fpname', 2]],
        [[2, 1], 'sqrtarea,fpname', ['sqrtarea', 'fpname'], [2, 'fpname'], ['sqrtarea', 1]],
        [[2, 0], 'sqrtarea,rarea', ['sqrtarea', 'rarea'], [2, 'rarea'], ['sqrtarea', 0]],
        [[0, 1, 2], -1, ['rarea', 'fpname', 'sqrtarea'], [0, 'fpname', 'sqrtarea'], ['rarea', 1, 2]],
    ]

    ds = buzz.DataSource()
    v = ds.open_vector('v', path, driver=driver)

    for query_ways in query_waysssss:
        # All queries in `query_ways` request the same thing in a different way
        indices = query_ways[0]

        queries_results = []
        for query in query_ways:
            features = list(v.iter_data(query))
            assert len(features) == len(data)

            query_result = []
            for feature in features:
                # For each feature in the polygon
                if len(indices) == 0:
                    feature = [feature]

                fields = feature[1:]
                assert len(fields) == len(indices)

                query_result.append(fields) # Build list of fields
            queries_results.append(query_result) # Build list of list of fields
            del query_result

        assert len(queries_results) == len(query_ways)
        _assert_all_list_of_fields_same(queries_results)

def _test_geom_read(path, driver, fps, data):
    ds = buzz.DataSource()
    v = ds.open_vector('v', path, driver=driver)

    queries = _build_geom_read_queries(v, fps)
    for gen, slicing, _, mask, clip in queries:
        # Normalize input mask
        if mask is None:
            pass
        elif isinstance(mask, buzz.Footprint):
            mask = mask.poly
        elif isinstance(mask, sg.Polygon):
            pass
        elif isinstance(mask, (tuple, np.ndarray)):
            mask = sg.box(*np.asarray(mask)[[0, 2, 1, 3]])
        else:
            print('fail with type', type(mask))
            assert False

        # Normalize input geometry
        geoms_ref = [dat[0] for dat in data]
        geoms_ref = [_any_geom_to_shapely(geom) for geom in geoms_ref]
        if mask is not None:
            geoms_ref = [
                geom for geom in geoms_ref
                if not geom.disjoint(mask)
            ]
        if clip:
            geoms_ref = [
                geom & mask
                for geom in geoms_ref
            ]
        geoms_ref = geoms_ref[slicing]

        # Compare
        geoms = list(gen)
        geoms = [_any_geom_to_shapely(geom) for geom in geoms]
        geoms_ref = [_any_geom_to_shapely(geom) for geom in geoms_ref]
        assert len(geoms) == len(geoms_ref)
        for geom, geom_ref in zip(geoms, geoms_ref):
            assert (geom ^ geom_ref).is_empty

# Test read slaves 2 **************************************************************************** **
def _build_geom_read_queries(v, fps):
    slicings = [
        slice(None),
        slice(1, None, 1),
        slice(None, 5, 1),
        slice(1, 5, 1),
        slice(1, None, 2),
        slice(None, 5, 2),
        slice(1, 5, 2),
    ]
    geom_types = ['shapely', 'coordinates']
    masks = [
        None,
        fps.GS,
        fps.GS.poly,
        fps.GS.extent,
    ]
    clips = [True, False]

    def _iter_by_get_data(geom_type, slicing, mask, clip):
        try:
            for i in range(*slicing.indices(len(v))):
                yield v.get_data(i, None, geom_type, mask, clip)
        except IndexError:
            pass

    def _iter_by_get_geojson(slicing, mask, clip):
        try:
            for i in range(*slicing.indices(len(v))):
                yield v.get_geojson(i, mask, clip)
        except IndexError:
            pass

    prod = itertools.product
    queries = itertools.chain(
        ((v.iter_data(None, geom_type, mask, clip, slicing), slicing, geom_type, mask, clip)
         for geom_type, slicing, mask, clip in prod(geom_types, slicings, masks, clips)
         if not (clip is True and mask is None)
        ),

        ((v.iter_geojson(mask, clip, slicing), slicing, 'geojson', mask, clip)
         for slicing, mask, clip in prod(slicings, masks, clips)
         if not (clip is True and mask is None)
        ),

        ((_iter_by_get_data(geom_type, slicing, mask, clip), slicing, geom_type, mask, clip)
         for geom_type, slicing, mask, clip in prod(geom_types, slicings, masks, clips)
         if not (clip is True and mask is None)
        ),

        ((_iter_by_get_geojson(slicing, mask, clip), slicing, 'geojson', mask, clip)
         for slicing, mask, clip in prod(slicings, masks, clips)
         if not (clip is True and mask is None)
        ),
    )
    return queries

def _any_geom_to_shapely(geom):
    """Any geom to shapely object. Points should have homogeneous dimensions size."""
    if isinstance(geom, (sg.LineString, sg.Point, sg.Polygon, sg.MultiPolygon)):
        return geom
    if isinstance(geom, dict):
        return sg.shape(geom['geometry'])
    if isinstance(geom, collections.Container):
        geom = np.asarray(geom)
        if geom.ndim == 1:
            return sg.Point(geom.tolist())
        elif geom.ndim == 2:
            return sg.LineString(geom.tolist())
        elif geom.ndim == 3:
            return sg.Polygon(*geom.tolist())
        elif geom.ndim == 4:
            return sg.MultiPolygon([
                sg.Polygon(*poly)
                for poly in geom.tolist()
            ])
    assert False

def _assert_all_list_of_fields_same(llf):
    """Check that all queries in query_ways yielded the same result"""
    fields_count = len(llf[0])

    assert eqall([len(lf) for lf in llf])

    for i in range(fields_count):
        val = llf[0][i]
        assert all(val == lf[i] for lf in llf)

def test_value_error(path):
    ds = buzz.DataSource()
    v = ds.create_avector(path, 'polygon')

    with pytest.raises(ValueError, match='geom_type'):
        list(v.iter_data(geom_type=''))
    with pytest.raises(TypeError, match='slicing'):
        list(v.iter_data(slicing=0))
    with pytest.raises(ValueError, match='clip'):
        list(v.iter_data(clip=True))
    with pytest.raises(TypeError, match='slicing'):
        list(v.iter_geojson(slicing=0))
    with pytest.raises(ValueError, match='clip'):
        list(v.iter_geojson(clip=True))
    with pytest.raises(TypeError, match='a'):
        v.insert_data(42)
