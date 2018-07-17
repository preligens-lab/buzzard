# pylint: disable=redefined-outer-name, unused-argument

from __future__ import division, print_function

import resource
import uuid
import os
import sys

import numpy as np
import pytest
import dask.distributed as dd

import buzzard as buzz
from buzzard.test.tools import fpeq

MEMV_META = dict(
    driver='MEMORY',
    path='',
    geometry='point',
    fields=[{'name': 'region', 'type': str}],
)

V_META = dict(
    geometry='point',
    fields=[{'name': 'region', 'type': str}],
)

def test_vector_nofifo():
    ds = buzz.DataSource(max_activated=np.inf)

    # Test with a shapefile
    assert (ds._queued_count, ds._locked_count) == (0, 0)
    with ds.acreate_vector('/tmp/v1.shp', **V_META).delete as v1:
        assert (ds._queued_count, ds._locked_count, v1.activated) == (0, 0, True)

        v1.insert_data((42, 43), ['fra'])
        assert (ds._queued_count, ds._locked_count, v1.activated) == (0, 0, True)

        v1.deactivate()
        assert (ds._queued_count, ds._locked_count, v1.activated) == (0, 0, False)

        v1.activate()
        assert (ds._queued_count, ds._locked_count, v1.activated) == (0, 0, True)

        assert (len(v1), v1.get_data(0, geom_type='coordinates')) == (1, ((42, 43), 'fra'))
        assert (ds._queued_count, ds._locked_count, v1.activated) == (0, 0, True)

        # Iteration until the end
        it = v1.iter_data()
        assert (ds._queued_count, ds._locked_count, v1.activated) == (0, 0, True), 'Generator is instanciated by has not yet started'
        for _ in it:
            assert (ds._queued_count, ds._locked_count, v1.activated) == (0, 1, True)
        assert (ds._queued_count, ds._locked_count, v1.activated) == (0, 0, True)
        del it
        assert (ds._queued_count, ds._locked_count, v1.activated) == (0, 0, True)

        # Do not start iteration
        it = v1.iter_data()
        assert (ds._queued_count, ds._locked_count, v1.activated) == (0, 0, True), 'Generator is instanciated by has not yet started'
        del it
        assert (ds._queued_count, ds._locked_count, v1.activated) == (0, 0, True)

        # Do not finish iteration
        it = v1.iter_data()
        assert (ds._queued_count, ds._locked_count, v1.activated) == (0, 0, True), 'Generator is instanciated by has not yet started'
        for _ in it:
            assert (ds._queued_count, ds._locked_count, v1.activated) == (0, 1, True)
            break
        del it
        assert (ds._queued_count, ds._locked_count, v1.activated) == (0, 0, True)

        # Start iteration before being loaded
        v1.deactivate()
        assert (ds._queued_count, ds._locked_count, v1.activated) == (0, 0, False)
        it = v1.iter_data()
        assert (ds._queued_count, ds._locked_count, v1.activated) == (0, 0, False), 'Generator is instanciated by has not yet started'
        for _ in it:
            assert (ds._queued_count, ds._locked_count, v1.activated) == (0, 1, True)
        assert (ds._queued_count, ds._locked_count, v1.activated) == (0, 0, True)
        del it
        assert (ds._queued_count, ds._locked_count, v1.activated) == (0, 0, True)

        assert (len(v1), v1.get_data(0, geom_type='coordinates')) == (1, ((42, 43), 'fra'))
    assert (ds._queued_count, ds._locked_count) == (0, 0)

    # Test with a memory vector
    with ds.acreate_vector(**MEMV_META).close as v1:
        assert (ds._queued_count, ds._locked_count, v1.activated) == (0, 0, True)

        v1.insert_data((42, 43), ['fra'])
        assert (ds._queued_count, ds._locked_count, v1.activated) == (0, 0, True)

        v1.deactivate()
        assert (ds._queued_count, ds._locked_count, v1.activated) == (0, 0, True), "can't deactivated a memory dataset"

        v1.activate()
        assert (ds._queued_count, ds._locked_count, v1.activated) == (0, 0, True)

        assert (len(v1), v1.get_data(0, geom_type='coordinates')) == (1, ((42, 43), 'fra'))
        assert (ds._queued_count, ds._locked_count, v1.activated) == (0, 0, True)

        # Iteration until the end
        it = v1.iter_data()
        assert (ds._queued_count, ds._locked_count, v1.activated) == (0, 0, True), 'Generator is instanciated by has not yet started'
        for _ in it:
            assert (ds._queued_count, ds._locked_count, v1.activated) == (0, 0, True), 'no need to lock a memory dataset'
        assert (ds._queued_count, ds._locked_count, v1.activated) == (0, 0, True)
        del it
        assert (ds._queued_count, ds._locked_count, v1.activated) == (0, 0, True)
    assert (ds._queued_count, ds._locked_count) == (0, 0)

    # Close while being activated
    with ds.acreate_vector('/tmp/v1.shp', **V_META).close as v1:
        assert (ds._queued_count, ds._locked_count, v1.activated) == (0, 0, True)
    assert (ds._queued_count, ds._locked_count) == (0, 0)

    # Delete while being activated
    with ds.acreate_vector('/tmp/v1.shp', **V_META).delete as v1:
        assert (ds._queued_count, ds._locked_count, v1.activated) == (0, 0, True)
    assert (ds._queued_count, ds._locked_count) == (0, 0)

    # Close while being deactivated
    with ds.acreate_vector('/tmp/v1.shp', **V_META).close as v1:
        assert (ds._queued_count, ds._locked_count, v1.activated) == (0, 0, True)
        v1.deactivate()
    assert (ds._queued_count, ds._locked_count) == (0, 0)

    # Delete while being deactivated
    with ds.acreate_vector('/tmp/v1.shp', **V_META).delete as v1:
        assert (ds._queued_count, ds._locked_count, v1.activated) == (0, 0, True)
        v1.deactivate()
    assert (ds._queued_count, ds._locked_count) == (0, 0)

def test_vector_fifo1_1file():
    ds = buzz.DataSource(max_activated=1)

    # Test with a shapefile
    assert (ds._queued_count, ds._locked_count) == (0, 0)
    with ds.acreate_vector('/tmp/v1.shp', **V_META).delete as v1:
        assert (ds._queued_count, ds._locked_count, v1.activated) == (1, 0, True)

        v1.insert_data((42, 43), ['fra'])
        assert (ds._queued_count, ds._locked_count, v1.activated) == (1, 0, True)

        v1.deactivate()
        assert (ds._queued_count, ds._locked_count, v1.activated) == (0, 0, False)

        v1.activate()
        assert (ds._queued_count, ds._locked_count, v1.activated) == (1, 0, True)

        assert (len(v1), v1.get_data(0, geom_type='coordinates')) == (1, ((42, 43), 'fra'))
        assert (ds._queued_count, ds._locked_count, v1.activated) == (1, 0, True)

        # Iteration until the end
        it = v1.iter_data()
        assert (ds._queued_count, ds._locked_count, v1.activated) == (1, 0, True), 'Generator is instanciated by has not yet started'
        for _ in it:
            assert (ds._queued_count, ds._locked_count, v1.activated) == (0, 1, True)
        assert (ds._queued_count, ds._locked_count, v1.activated) == (1, 0, True)
        del it
        assert (ds._queued_count, ds._locked_count, v1.activated) == (1, 0, True)

        # Do not start iteration
        it = v1.iter_data()
        assert (ds._queued_count, ds._locked_count, v1.activated) == (1, 0, True), 'Generator is instanciated by has not yet started'
        del it
        assert (ds._queued_count, ds._locked_count, v1.activated) == (1, 0, True)

        # Do not finish iteration
        it = v1.iter_data()
        assert (ds._queued_count, ds._locked_count, v1.activated) == (1, 0, True), 'Generator is instanciated by has not yet started'
        for _ in it:
            assert (ds._queued_count, ds._locked_count, v1.activated) == (0, 1, True)
            break
        del it
        assert (ds._queued_count, ds._locked_count, v1.activated) == (1, 0, True)

        # Start iteration before being loaded
        v1.deactivate()
        assert (ds._queued_count, ds._locked_count, v1.activated) == (0, 0, False)
        it = v1.iter_data()
        assert (ds._queued_count, ds._locked_count, v1.activated) == (0, 0, False), 'Generator is instanciated by has not yet started'
        for _ in it:
            assert (ds._queued_count, ds._locked_count, v1.activated) == (0, 1, True)
        assert (ds._queued_count, ds._locked_count, v1.activated) == (1, 0, True)
        del it
        assert (ds._queued_count, ds._locked_count, v1.activated) == (1, 0, True)

        assert (len(v1), v1.get_data(0, geom_type='coordinates')) == (1, ((42, 43), 'fra'))
    assert (ds._queued_count, ds._locked_count) == (0, 0)

    # Test with a memory vector
    with ds.acreate_vector(**MEMV_META).close as v1:
        assert (ds._queued_count, ds._locked_count, v1.activated) == (0, 0, True)

        v1.insert_data((42, 43), ['fra'])
        assert (ds._queued_count, ds._locked_count, v1.activated) == (0, 0, True)

        v1.deactivate()
        assert (ds._queued_count, ds._locked_count, v1.activated) == (0, 0, True), "can't deactivated a memory dataset"

        v1.activate()
        assert (ds._queued_count, ds._locked_count, v1.activated) == (0, 0, True)

        assert (len(v1), v1.get_data(0, geom_type='coordinates')) == (1, ((42, 43), 'fra'))
        assert (ds._queued_count, ds._locked_count, v1.activated) == (0, 0, True)

        # Iteration until the end
        it = v1.iter_data()
        assert (ds._queued_count, ds._locked_count, v1.activated) == (0, 0, True), 'Generator is instanciated by has not yet started'
        for _ in it:
            assert (ds._queued_count, ds._locked_count, v1.activated) == (0, 0, True), 'no need to lock a memory dataset'
        assert (ds._queued_count, ds._locked_count, v1.activated) == (0, 0, True)
        del it
        assert (ds._queued_count, ds._locked_count, v1.activated) == (0, 0, True)
    assert (ds._queued_count, ds._locked_count) == (0, 0)

    # Close while being activated
    with ds.acreate_vector('/tmp/v1.shp', **V_META).close as v1:
        assert (ds._queued_count, ds._locked_count, v1.activated) == (1, 0, True)
    assert (ds._queued_count, ds._locked_count) == (0, 0)

    # Delete while being activated
    with ds.acreate_vector('/tmp/v1.shp', **V_META).delete as v1:
        assert (ds._queued_count, ds._locked_count, v1.activated) == (1, 0, True)
    assert (ds._queued_count, ds._locked_count) == (0, 0)

    # Close while being deactivated
    with ds.acreate_vector('/tmp/v1.shp', **V_META).close as v1:
        assert (ds._queued_count, ds._locked_count, v1.activated) == (1, 0, True)
        v1.deactivate()
    assert (ds._queued_count, ds._locked_count) == (0, 0)

    # Delete while being deactivated
    with ds.acreate_vector('/tmp/v1.shp', **V_META).delete as v1:
        assert (ds._queued_count, ds._locked_count, v1.activated) == (1, 0, True)
        v1.deactivate()
    assert (ds._queued_count, ds._locked_count) == (0, 0)

def test_vector_fifo1_2files():
    ds = buzz.DataSource(max_activated=1)

    # Test with a shapefile
    assert (ds._queued_count, ds._locked_count) == (0, 0)
    with ds.acreate_vector('/tmp/v1.shp', **V_META).delete as v1:
        assert (ds._queued_count, ds._locked_count, v1.activated) == (1, 0, True)

        with ds.acreate_vector('/tmp/v2.shp', **V_META).delete as v2:
            assert (ds._queued_count, ds._locked_count, v1.activated, v2.activated) == (1, 0, False, True)

            v1.insert_data((42, 43), ['fra'])
            assert (ds._queued_count, ds._locked_count, v1.activated, v2.activated) == (1, 0, True, False)

            v2.insert_data((44, 45), ['ger'])
            assert (ds._queued_count, ds._locked_count, v1.activated, v2.activated) == (1, 0, False, True)

            assert (len(v1), v1.get_data(0, geom_type='coordinates')) == (1, ((42, 43), 'fra'))
            assert (len(v2), v2.get_data(0, geom_type='coordinates')) == (1, ((44, 45), 'ger'))

            # Attempt to iterate on both
            it1 = v1.iter_data()
            next(it1)
            assert (ds._queued_count, ds._locked_count, v1.activated, v2.activated) == (0, 1, True, False)
            it2 = v2.iter_data()
            assert (ds._queued_count, ds._locked_count, v1.activated, v2.activated) == (0, 1, True, False)
            with pytest.raises(RuntimeError, match='simultaneous'):
                next(it2)
            del it2, it1

def test_vector_fifo2_2files():
    ds = buzz.DataSource(max_activated=2)

    # Test with a shapefile
    assert (ds._queued_count, ds._locked_count) == (0, 0)
    with ds.acreate_vector('/tmp/v1.shp', **V_META).delete as v1:
        assert (ds._queued_count, ds._locked_count, v1.activated) == (1, 0, True)

        with ds.acreate_vector('/tmp/v2.shp', **V_META).delete as v2:
            assert (ds._queued_count, ds._locked_count, v1.activated, v2.activated) == (2, 0, True, True)

            v1.insert_data((42, 43), ['fra'])
            assert (ds._queued_count, ds._locked_count, v1.activated, v2.activated) == (2, 0, True, True)

            v2.insert_data((44, 45), ['ger'])
            assert (ds._queued_count, ds._locked_count, v1.activated, v2.activated) == (2, 0, True, True)

            assert (len(v1), v1.get_data(0, geom_type='coordinates')) == (1, ((42, 43), 'fra'))
            assert (len(v2), v2.get_data(0, geom_type='coordinates')) == (1, ((44, 45), 'ger'))

            # Attempt to iterate on both
            it1 = v1.iter_data()
            next(it1)
            assert (ds._queued_count, ds._locked_count, v1.activated, v2.activated) == (1, 1, True, True)
            it2 = v2.iter_data()
            next(it2)
            assert (ds._queued_count, ds._locked_count, v1.activated, v2.activated) == (0, 2, True, True)
            del it2, it1
            assert (ds._queued_count, ds._locked_count, v1.activated, v2.activated) == (2, 0, True, True)

def test_vector_end_while_iterating():
    ds = buzz.DataSource()
    v1 = ds.acreate_vector('/tmp/v1.shp', **V_META)
    v1.insert_data((42, 43), ['fra'])
    it = v1.iter_data()
    next(it)
    with pytest.raises(RuntimeError, match='in progress'):
        v1.close()
    with pytest.raises(RuntimeError, match='in progress'):
        v1.delete()
    with pytest.raises(RuntimeError, match='in progress'):
        v1.delete_layer()

def test_raster():
    ds = buzz.DataSource(max_activated=2)
    fp = buzz.Footprint(
        tl=(1, 1),
        size=(10, 10),
        rsize=(10, 10),
    )
    with ds.acreate_raster('/tmp/t1.tif', fp, float, 1).delete as r1:
        assert (ds._queued_count, ds._locked_count, r1.activated) == (1, 0, True)

        with ds.acreate_raster('/tmp/t2.tif', fp, float, 1).delete as r2:
            assert (ds._queued_count, ds._locked_count, r1.activated, r2.activated) == (2, 0, True, True)

            with ds.acreate_raster('/tmp/t3.tif', fp, float, 1).delete as r3:
                assert (ds._queued_count, ds._locked_count, r1.activated, r2.activated, r3.activated) == (2, 0, False, True, True)

                # Test lru policy
                r1.fill(1)
                assert (ds._queued_count, ds._locked_count, r1.activated, r2.activated, r3.activated) == (2, 0, True, False, True)
                r2.fill(2)
                assert (ds._queued_count, ds._locked_count, r1.activated, r2.activated, r3.activated) == (2, 0, True, True, False)
                r3.fill(3)
                assert (ds._queued_count, ds._locked_count, r1.activated, r2.activated, r3.activated) == (2, 0, False, True, True)

                # Test MEM raster (should behave like raster proxy in this case)
                with ds.acreate_raster('', fp, float, 1, driver='MEM').close as r4:
                    assert (ds._queued_count, ds._locked_count, r1.activated, r2.activated, r3.activated, r4.activated) == (2, 0, False, True, True, True)
                    r4.deactivate()
                    assert (ds._queued_count, ds._locked_count, r1.activated, r2.activated, r3.activated, r4.activated) == (2, 0, False, True, True, True)
                    r4.activate()
                    assert (ds._queued_count, ds._locked_count, r1.activated, r2.activated, r3.activated, r4.activated) == (2, 0, False, True, True, True)
                    r4.fill(42)
                    assert (r4.get_data() == 42).all()
                assert (ds._queued_count, ds._locked_count, r1.activated, r2.activated, r3.activated) == (2, 0, False, True, True)

                # Test full activations / deactivations
                with pytest.raises(RuntimeError, match='max_activated'):
                    ds.activate_all()

                ds.deactivate_all()
                assert (ds._queued_count, ds._locked_count, r1.activated, r2.activated, r3.activated) == (0, 0, False, False, False)

            assert (ds._queued_count, ds._locked_count, r1.activated, r2.activated) == (0, 0, False, False)
            ds.activate_all()
            assert (ds._queued_count, ds._locked_count, r1.activated, r2.activated) == (2, 0, True, True)

def test_maxfd():
    cap, _ = resource.getrlimit(resource.RLIMIT_NOFILE)

    if cap > 2049:
        pytest.skip('file descriptors cap is too high to be tested: {}'.format(cap))

    # Test 1 without fifo
    ds = buzz.DataSource(max_activated=np.inf)
    suff = str(uuid.uuid4())
    try:
        with pytest.raises(Exception):
            for i in range(cap + 1):
                ds.create_vector(i, '/tmp/f{:04d}_{}.shp'.format(i, suff), 'point', [{'name': 'Hello', 'type': int}])
                ds[i].insert_data((i, i + 1))
            pytest.skip("Reached {} opened files but didn't crash, whatever...".format(i))
    finally:
        file_count = i
        for i in range(file_count):
            ds[i].delete()

    # Test 2 with fifo
    ds = buzz.DataSource(max_activated=5)
    suff = str(uuid.uuid4())
    for i in range(cap + 1):
        ds.create_vector(i, '/tmp/f{:04d}_{}.shp'.format(i, suff), 'point', [{'name': 'Hello', 'type': int}])
        ds[i].insert_data((i, i + 1))

    assert (ds._queued_count, ds._locked_count) == (5, 0)
    for i in range(cap + 1):
        v = ds[i]
        assert (len(v), v.get_data(0, geom_type='coordinates')) == (1, ((i, i + 1), None))

    for i in range(cap + 1):
        ds[i].delete()

@pytest.mark.skipif(sys.version_info.major == 2, reason='python 2.7')
def test_pickling():

    def pxfn(fp):
        """Pixel function for recipe. `ds` lives in the closure"""
        return np.ones(fp.shape) * id(ds)

    def slave():
        """Slave process routine. `ds` and `oldid` live in the closure and are pickled by cloudpickle in Client.sumbit"""
        # At this point the `ds` was cloudpickled/uncloudpickled by dask through dask.distributed
        local_ds = ds

        # Perform an additional copy to unit test this feature too

        local_ds = local_ds.copy()

        assert id(local_ds) != oldid, 'this test makes sense if `local_ds` was pickled'
        assert 'v1' in local_ds
        assert 'v2' not in local_ds
        assert 'r1' in local_ds
        assert 'r2' not in local_ds
        assert (local_ds._queued_count, local_ds._locked_count, local_ds.v1.activated, local_ds.r1.activated) == (0, 0, False, False)
        assert local_ds.v1.get_data(0)[1] == str(oldid)
        assert (local_ds.r1.get_data() == oldid).all()

        local_ds.v1.insert_data((0, 1), ['42'])
        local_ds.r1.fill(42)
        assert local_ds.v1.get_data(1)[1] == '42'
        assert (local_ds.r1.get_data() == 42).all()

        local_ds.deactivate_all()

    ds = buzz.DataSource(max_activated=2)
    oldid = id(ds)
    fp = buzz.Footprint(
        tl=(1, 1),
        size=(10, 10),
        rsize=(10, 10),
    )
    clust = dd.LocalCluster(n_workers=1, threads_per_worker=1, scheduler_port=0)
    print()
    print(clust)
    cl = dd.Client(clust)
    print(cl)

    with ds.create_vector('v1', '/tmp/v1.shp', **V_META).delete:
        with ds.create_raster('r1', '/tmp/t1.shp', fp, float, 1).delete:
            ds.create_raster('r2', '', fp, float, 1, driver='MEM')
            ds.create_vector('v2',**MEMV_META)

            ds.v1.insert_data((0, 1), [str(oldid)])
            ds.v2.insert_data((0, 1), [str(oldid)])
            ds.r1.fill(oldid)
            ds.r2.fill(oldid)

            ds.deactivate_all()
            cl.submit(slave).result()
            assert ds.v1.get_data(1)[1] == '42'
            assert (ds.r1.get_data() == 42).all()

def test_file_changed():
    ds = buzz.DataSource(max_activated=2)
    fp = buzz.Footprint(
        tl=(1, 1),
        size=(10, 10),
        rsize=(10, 10),
    )

    with ds.acreate_raster('/tmp/t1.tif', fp, float, 1).delete as r1:
        r1.fill(1)
        assert (r1.get_data() == 1).all()
        assert r1.fp == fp
        assert len(r1) == 1
        r1.deactivate()

        with ds.aopen_raster('/tmp/t1.tif').close as r2:
            assert (r2.get_data() == 1).all()
            assert r2.fp == fp
            assert len(r2) == 1

        with ds.acreate_raster('/tmp/t1.tif', fp, float, 2).close as r2:
            r2.fill(2)
            assert (r2.get_data() == 2).all()
            assert r2.fp == fp
            assert len(r2) == 2

        with ds.aopen_raster('/tmp/t1.tif').close as r2:
            assert (r2.get_data() == 2).all()
            assert r2.fp == fp
            assert len(r2) == 2

        with pytest.raises(RuntimeError, match='changed'):
            r1.activate()

    with ds.acreate_vector('/tmp/v1.shp', 'Point', [{'name': 'area', 'type': float}]).delete as v1:
        v1.insert_data((0, 0), [42])
        assert v1.get_data(0, geom_type='coordinates') == ((0, 0), 42)
        assert v1.type == 'Point'
        assert len(v1) == 1
        v1.deactivate()

        with ds.aopen_vector('/tmp/v1.shp').close as v2:
            assert v2.get_data(0, geom_type='coordinates') == ((0, 0), 42)
            assert v2.type == 'Point'
            assert len(v2) == 1

        with ds.acreate_vector('/tmp/v1.shp', 'LineString', [{'name': 'area', 'type': float}]).close as v2:
            v2.insert_data(((0, 0), (1, 1)), [42])
            v2.insert_data(((0, 0), (1, 1)), [42])
            assert v2.get_data(0, geom_type='coordinates') == (((0, 0), (1, 1)), 42)
            assert v2.type == 'LineString'
            assert len(v2) == 2


        with ds.aopen_vector('/tmp/v1.shp').close as v2:
            assert v2.get_data(0, geom_type='coordinates') == (((0, 0), (1, 1)), 42)
            assert v2.type == 'LineString'
            assert len(v2) == 2

        with pytest.raises(RuntimeError, match='changed'):
            v1.activate()
