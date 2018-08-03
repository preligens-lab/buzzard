# pylint: disable=redefined-outer-name, unused-argument

from __future__ import division, print_function

import resource
import uuid
import os
import sys
import multiprocessing as mp
import multiprocessing.pool

import numpy as np
import pytest
import shapely.geometry as sg

import buzzard as buzz
from buzzard.test.tools import fpeq


def test_vector():
    ds = buzz.DataSource(max_active=2)
    meta = dict(
        geometry='point',
    )

    def statuses(*args):
        l = tuple([
            (
                ds._back.idle_count(prox._back.uid),
                ds._back.used_count(prox._back.uid),
                prox.active_count,
                prox.active
            )
            for prox in args
        ])
        return l

    assert (ds._back.idle_count(), ds._back.used_count(), ds.active_count) == (0, 0, 0)
    with ds.acreate_vector('/tmp/v1.shp', **meta).delete as r1:
        r1.insert_data([0, 0])

        assert (ds._back.idle_count(), ds._back.used_count(), ds.active_count) == (1, 0, 1)
        assert (ds._back.idle_count(r1._back.uid), ds._back.used_count(r1._back.uid), r1.active_count, r1.active) == (1, 0, 1, True)

        # Iteration 1 - exn
        it = r1.iter_data()
        next(it)
        assert (ds._back.idle_count(), ds._back.used_count(), ds.active_count) == (0, 1, 1)
        assert (ds._back.idle_count(r1._back.uid), ds._back.used_count(r1._back.uid), r1.active_count, r1.active) == (0, 1, 1, True)

        try:
            next(it)
        except StopIteration:
            pass
        assert (ds._back.idle_count(), ds._back.used_count(), ds.active_count) == (1, 0, 1)
        assert (ds._back.idle_count(r1._back.uid), ds._back.used_count(r1._back.uid), r1.active_count, r1.active) == (1, 0, 1, True)

        # Iteration 2 - close
        it = r1.iter_data()
        next(it)
        assert (ds._back.idle_count(), ds._back.used_count(), ds.active_count) == (0, 1, 1)
        assert (ds._back.idle_count(r1._back.uid), ds._back.used_count(r1._back.uid), r1.active_count, r1.active) == (0, 1, 1, True)

        it.close()
        assert (ds._back.idle_count(r1._back.uid), ds._back.used_count(r1._back.uid), r1.active_count, r1.active) == (1, 0, 1, True)

        # Iteration 3 - del
        it = r1.iter_data()
        next(it)
        assert (ds._back.idle_count(), ds._back.used_count(), ds.active_count) == (0, 1, 1)
        assert (ds._back.idle_count(r1._back.uid), ds._back.used_count(r1._back.uid), r1.active_count, r1.active) == (0, 1, 1, True)

        del it
        assert (ds._back.idle_count(r1._back.uid), ds._back.used_count(r1._back.uid), r1.active_count, r1.active) == (1, 0, 1, True)

        # Iteration 4 - try proxy.close
        it = r1.iter_data()
        next(it)
        assert (ds._back.idle_count(), ds._back.used_count(), ds.active_count) == (0, 1, 1)
        assert (ds._back.idle_count(r1._back.uid), ds._back.used_count(r1._back.uid), r1.active_count, r1.active) == (0, 1, 1, True)

        with pytest.raises(Exception, match='deactivate'):
            r1.close()
        assert (ds._back.idle_count(), ds._back.used_count(), ds.active_count) == (0, 1, 1)
        assert (ds._back.idle_count(r1._back.uid), ds._back.used_count(r1._back.uid), r1.active_count, r1.active) == (0, 1, 1, True)

        del it
        assert (ds._back.idle_count(), ds._back.used_count(), ds.active_count) == (1, 0, 1)
        assert (ds._back.idle_count(r1._back.uid), ds._back.used_count(r1._back.uid), r1.active_count, r1.active) == (1, 0, 1, True)

        # Iteration 5 - multi
        it0 = r1.iter_data()
        next(it0)
        it1 = r1.iter_data()
        next(it1)
        assert (ds._back.idle_count(), ds._back.used_count(), ds.active_count) == (0, 2, 2)
        assert (ds._back.idle_count(r1._back.uid), ds._back.used_count(r1._back.uid), r1.active_count, r1.active) == (0, 2, 2, True)

        it2 = r1.iter_data()
        with pytest.raises(RuntimeError, match='simultaneous'):
            next(it2)

        assert (ds._back.idle_count(), ds._back.used_count(), ds.active_count) == (0, 2, 2)
        assert (ds._back.idle_count(r1._back.uid), ds._back.used_count(r1._back.uid), r1.active_count, r1.active) == (0, 2, 2, True)

        r1.activate() # no effect
        assert (ds._back.idle_count(), ds._back.used_count(), ds.active_count) == (0, 2, 2)
        assert (ds._back.idle_count(r1._back.uid), ds._back.used_count(r1._back.uid), r1.active_count, r1.active) == (0, 2, 2, True)

        del it0
        assert (ds._back.idle_count(), ds._back.used_count(), ds.active_count) == (1, 1, 2)
        assert (ds._back.idle_count(r1._back.uid), ds._back.used_count(r1._back.uid), r1.active_count, r1.active) == (1, 1, 2, True)

        del it1
        assert (ds._back.idle_count(), ds._back.used_count(), ds.active_count) == (2, 0, 2)
        assert (ds._back.idle_count(r1._back.uid), ds._back.used_count(r1._back.uid), r1.active_count, r1.active) == (2, 0, 2, True)

def test_vector_concurrent():

    def _work(i):
        point, = r1.iter_data(None)
        return point

    ds = buzz.DataSource(max_active=4)
    meta = dict(
        geometry='point',
    )

    p = mp.pool.ThreadPool(4)
    with ds.acreate_vector('/tmp/v1.shp', **meta).delete as r1:
        pt = sg.Point([42, 45])
        r1.insert_data(pt)
        r1.deactivate()

        points = list(p.map(_work, range(1000)))
        assert all(p == pt for p in points)
        assert (ds._back.idle_count(), ds._back.used_count(), ds.active_count) == (4, 0, 4)
        assert (ds._back.idle_count(r1._back.uid), ds._back.used_count(r1._back.uid), r1.active_count, r1.active) == (4, 0, 4, True)

    p.terminate()

def test_raster():
    ds = buzz.DataSource(max_activated=2)
    meta = dict(
        fp = buzz.Footprint(
            tl=(1, 1),
            size=(10, 10),
            rsize=(10, 10),
        ),
        dtype=float,
        band_count=1,
    )

    def statuses(*args):
        l = tuple([
            (
                ds._back.idle_count(prox._back.uid),
                ds._back.used_count(prox._back.uid),
                prox.active_count,
                prox.active
            )
            for prox in args
        ])
        return l

    assert (ds._back.idle_count(), ds._back.used_count(), ds.active_count) == (0, 0, 0)
    with ds.acreate_raster('/tmp/t1.tif', **meta).delete as r1:
        assert (ds._back.idle_count(), ds._back.used_count(), ds.active_count) == (1, 0, 1)
        assert (ds._back.idle_count(r1._back.uid), ds._back.used_count(r1._back.uid), r1.active_count, r1.active) == (1, 0, 1, True)
        with ds.acreate_raster('/tmp/t2.tif', **meta).delete as r2:
            assert (ds._back.idle_count(), ds._back.used_count(), ds.active_count) == (2, 0, 2)
            assert statuses(r1, r2) == ((1, 0, 1, True), (1, 0, 1, True))

            with ds.acreate_raster('/tmp/t3.tif', **meta).delete as r3:
                assert (ds._back.idle_count(), ds._back.used_count(), ds.active_count) == (2, 0, 2)
                assert statuses(r1, r2, r3) == ((0, 0, 0, False), (1, 0, 1, True), (1, 0, 1, True))

                r1.activate()
                assert (ds._back.idle_count(), ds._back.used_count(), ds.active_count) == (2, 0, 2)
                assert statuses(r1, r2, r3) == ((1, 0, 1, True), (0, 0, 0, False), (1, 0, 1, True))

                r1.deactivate()
                r2.activate()
                assert (ds._back.idle_count(), ds._back.used_count(), ds.active_count) == (2, 0, 2)
                assert statuses(r1, r2, r3) == ((0, 0, 0, False), (1, 0, 1, True), (1, 0, 1, True))

                with pytest.raises(RuntimeError, match='max_activated'):
                    ds.activate_all()
                assert (ds._back.idle_count(), ds._back.used_count(), ds.active_count) == (2, 0, 2)
                assert statuses(r1, r2, r3) == ((0, 0, 0, False), (1, 0, 1, True), (1, 0, 1, True))

                ds.deactivate_all()
                assert (ds._back.idle_count(), ds._back.used_count(), ds.active_count) == (0, 0, 0)
                assert statuses(r1, r2, r3) == ((0, 0, 0, False), (0, 0, 0, False), (0, 0, 0, False))

                r1.fill(42)
                r2.fill(42)
                r3.fill(42)
                assert (ds._back.idle_count(), ds._back.used_count(), ds.active_count) == (2, 0, 2)
                assert statuses(r1, r2, r3) == ((0, 0, 0, False), (1, 0, 1, True), (1, 0, 1, True))

            assert (ds._back.idle_count(), ds._back.used_count(), ds.active_count) == (1, 0, 1)
            assert statuses(r1, r2) == ((0, 0, 0, False), (1, 0, 1, True))
            ds.deactivate_all()
            ds.activate_all()
            assert (ds._back.idle_count(), ds._back.used_count(), ds.active_count) == (2, 0, 2)
            assert statuses(r1, r2) == ((1, 0, 1, True), (1, 0, 1, True))

            return

    assert (ds._back.idle_count(), ds._back.used_count(), ds.active_count) == (0, 0, 0)
