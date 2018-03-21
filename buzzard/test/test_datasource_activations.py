

# pylint: disable=redefined-outer-name, unused-argument

from __future__ import division, print_function

import resource
import uuid

import numpy as np
import pytest

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
    with ds.create_avector('/tmp/v1.shp', **V_META).delete as v1:
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
    with ds.create_avector(**MEMV_META).close as v1:
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
    with ds.create_avector('/tmp/v1.shp', **V_META).close as v1:
        assert (ds._queued_count, ds._locked_count, v1.activated) == (0, 0, True)
    assert (ds._queued_count, ds._locked_count) == (0, 0)

    # Delete while being activated
    with ds.create_avector('/tmp/v1.shp', **V_META).delete as v1:
        assert (ds._queued_count, ds._locked_count, v1.activated) == (0, 0, True)
    assert (ds._queued_count, ds._locked_count) == (0, 0)

    # Close while being deactivated
    with ds.create_avector('/tmp/v1.shp', **V_META).close as v1:
        assert (ds._queued_count, ds._locked_count, v1.activated) == (0, 0, True)
        v1.deactivate()
    assert (ds._queued_count, ds._locked_count) == (0, 0)

    # Delete while being deactivated
    with ds.create_avector('/tmp/v1.shp', **V_META).delete as v1:
        assert (ds._queued_count, ds._locked_count, v1.activated) == (0, 0, True)
        v1.deactivate()
    assert (ds._queued_count, ds._locked_count) == (0, 0)

def test_vector_fifo1_1file():
    ds = buzz.DataSource(max_activated=1)

    # Test with a shapefile
    assert (ds._queued_count, ds._locked_count) == (0, 0)
    with ds.create_avector('/tmp/v1.shp', **V_META).delete as v1:
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
    with ds.create_avector(**MEMV_META).close as v1:
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
    with ds.create_avector('/tmp/v1.shp', **V_META).close as v1:
        assert (ds._queued_count, ds._locked_count, v1.activated) == (1, 0, True)
    assert (ds._queued_count, ds._locked_count) == (0, 0)

    # Delete while being activated
    with ds.create_avector('/tmp/v1.shp', **V_META).delete as v1:
        assert (ds._queued_count, ds._locked_count, v1.activated) == (1, 0, True)
    assert (ds._queued_count, ds._locked_count) == (0, 0)

    # Close while being deactivated
    with ds.create_avector('/tmp/v1.shp', **V_META).close as v1:
        assert (ds._queued_count, ds._locked_count, v1.activated) == (1, 0, True)
        v1.deactivate()
    assert (ds._queued_count, ds._locked_count) == (0, 0)

    # Delete while being deactivated
    with ds.create_avector('/tmp/v1.shp', **V_META).delete as v1:
        assert (ds._queued_count, ds._locked_count, v1.activated) == (1, 0, True)
        v1.deactivate()
    assert (ds._queued_count, ds._locked_count) == (0, 0)

def test_vector_fifo1_2files():
    ds = buzz.DataSource(max_activated=1)

    # Test with a shapefile
    assert (ds._queued_count, ds._locked_count) == (0, 0)
    with ds.create_avector('/tmp/v1.shp', **V_META).delete as v1:
        assert (ds._queued_count, ds._locked_count, v1.activated) == (1, 0, True)

        with ds.create_avector('/tmp/v2.shp', **V_META).delete as v2:
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
    with ds.create_avector('/tmp/v1.shp', **V_META).delete as v1:
        assert (ds._queued_count, ds._locked_count, v1.activated) == (1, 0, True)

        with ds.create_avector('/tmp/v2.shp', **V_META).delete as v2:
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
    v1 = ds.create_avector('/tmp/v1.shp', **V_META)
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
        tl=(0, 0),
        size=(10, 10),
        rsize=(10, 10),
    )
    with ds.create_araster('/tmp/t1.shp', fp, float, 1).delete as r1:
        assert (ds._queued_count, ds._locked_count, r1.activated) == (1, 0, True)

        with ds.create_araster('/tmp/t2.shp', fp, float, 1).delete as r2:
            assert (ds._queued_count, ds._locked_count, r1.activated, r2.activated) == (2, 0, True, True)

            with ds.create_araster('/tmp/t3.shp', fp, float, 1).delete as r3:
                assert (ds._queued_count, ds._locked_count, r1.activated, r2.activated, r3.activated) == (2, 0, False, True, True)

                # Test lru policy
                r1.fill(1)
                assert (ds._queued_count, ds._locked_count, r1.activated, r2.activated, r3.activated) == (2, 0, True, False, True)
                r2.fill(2)
                assert (ds._queued_count, ds._locked_count, r1.activated, r2.activated, r3.activated) == (2, 0, True, True, False)
                r3.fill(3)
                assert (ds._queued_count, ds._locked_count, r1.activated, r2.activated, r3.activated) == (2, 0, False, True, True)

                # Test raster proxy
                def pxfn(fp):
                    return np.ones(fp.shape) * 42

                with ds.create_recipe_araster(pxfn, fp, 'float32').close as r4:
                    assert (ds._queued_count, ds._locked_count, r1.activated, r2.activated, r3.activated, r4.activated) == (2, 0, False, True, True, True)
                    r4.deactivate()
                    assert (ds._queued_count, ds._locked_count, r1.activated, r2.activated, r3.activated, r4.activated) == (2, 0, False, True, True, True)
                    r4.activate()
                    assert (ds._queued_count, ds._locked_count, r1.activated, r2.activated, r3.activated, r4.activated) == (2, 0, False, True, True, True)
                    assert (r4.get_data() == 42).all()
                assert (ds._queued_count, ds._locked_count, r1.activated, r2.activated, r3.activated) == (2, 0, False, True, True)

                # Test MEM raster (should behave like raster proxy in this case)
                with ds.create_araster('', fp, float, 1, driver='MEM').close as r4:
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
                ds.create_vector(i, '/tmp/f{:04d}_{}.json'.format(i, suff), 'point', driver='GeoJSON')
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
        ds.create_vector(i, '/tmp/f{:04d}_{}.json'.format(i, suff), 'point', driver='GeoJSON')
        ds[i].insert_data((i, i + 1))

    assert (ds._queued_count, ds._locked_count) == (5, 0)
    for i in range(cap + 1):
        v = ds[i]
        assert (len(v), v.get_data(0, geom_type='coordinates')) == (1, (i, i + 1))

    for i in range(cap + 1):
        ds[i].delete()
