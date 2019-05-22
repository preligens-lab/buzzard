"""Tests for *Raster.get_data, *Raster.set_data and *Raster.fill methods (not testing resampling)"""

# pylint: disable=redefined-outer-name

from __future__ import division, print_function
import itertools
import os
import uuid
import tempfile

import numpy as np
import pytest

from buzzard import Footprint, Dataset

@pytest.fixture(scope='module')
def ds():
    return Dataset(allow_interpolation=0)

@pytest.fixture(scope='module', params=['GTiff', 'MEM', 'numpy'])
def driver(request):
    return request.param

@pytest.fixture(scope='module', params=[1, 3])
def channel_count(request):
    return request.param

@pytest.fixture(scope='module', params=['float32', 'uint8'])
def dtype(request):
    return request.param

@pytest.fixture(scope='module', params=[200, None])
def src_nodata(request):
    return request.param

@pytest.fixture(scope='module', params=[200, 250])
def dst_nodata(request):
    return request.param

@pytest.fixture(scope='module')
def rast(ds, driver, channel_count, dtype, src_nodata):
    """Fixture for the dataset creation"""
    fp = Footprint(
        tl=(100, 110), size=(10, 10), rsize=(10, 10)
    )
    if driver == 'numpy':
        rast = ds.awrap_numpy_raster(
            fp,
            np.empty(np.r_[fp.shape, channel_count], dtype=dtype),
            channels_schema=dict(nodata=src_nodata),
            sr=None,
            mode='w',
        )
    elif driver == 'MEM':
        rast = ds.acreate_raster(
            '', fp, dtype, channel_count, channels_schema=dict(nodata=src_nodata), driver='MEM',
        )
    else:
        path = '{}/{}.tif'.format(tempfile.gettempdir(), uuid.uuid4())
        rast = ds.acreate_raster(
            path, fp, dtype, channel_count, channels_schema=dict(nodata=src_nodata), driver=driver
        )
    yield rast
    if driver in {'numpy', 'MEM'}:
        rast.close()
    else:
        rast.delete()

@pytest.fixture
def dst_arr(rast):
    """Array to write in raster and to compare against"""
    arr = np.dstack([
        np.add(*rast.fp.meshgrid_raster) + i
        for i in range(len(rast))
    ]).astype(rast.dtype)
    if rast.nodata is not None:
        arr[np.diag_indices(arr.shape[0])] = rast.nodata
    return arr

def test_fill(rast):
    for band_id in range(1, len(rast) + 1):
        rast.fill(band=band_id, value=band_id)
    for band_id in range(1, len(rast) + 1):
        assert np.all(
            rast.get_data(band=band_id) == band_id
        )

def test_set_data_whole(rast, dst_arr):
    rast.set_data(dst_arr, channels=slice(None))
    arr = rast.get_data(band=[-1])
    assert np.all(
        arr == dst_arr
    )

def test_get_data_dst_nodata(rast, dst_nodata, dst_arr):
    fp = rast.fp.dilate(1)
    inner_slice = rast.fp.slice_in(fp)
    rast.set_data(dst_arr, channels=None)

    arr = rast.get_data(band=[-1], dst_nodata=dst_nodata, fp=fp)

    arr2 = rast.get_data(band=[-1], dst_nodata=dst_nodata, fp=fp.erode(1))
    assert np.all(arr2 == arr[inner_slice])

    if rast.nodata is not None:
        assert np.all(arr[inner_slice][np.diag_indices(dst_arr.shape[0])] == dst_nodata)
        arr[inner_slice][np.diag_indices(dst_arr.shape[0])] = rast.nodata
    assert np.all(arr[inner_slice] == dst_arr)

    outer_mask = np.ones(fp.shape, bool)
    outer_mask[inner_slice] = False
    assert np.all(arr[outer_mask] == dst_nodata)

def test_get_data_rect(rast, dst_arr):
    rast.set_data(dst_arr, band=-1)

    rect_sizes = [
        1, 10, 15
    ]
    locations = [
        (-10, -10),
        (-9, -9),
        (-1, -1),
        (0, 0),
        (5, 5),
        (9, 9),
        (10, 10),
        (11, 11),
    ]
    dst_nodata = rast.nodata or 200
    for offset_px, rsize1 in itertools.product(locations, rect_sizes):
        fp = rast.fp
        fp = fp.move(fp.tl + offset_px * fp.pxvec)
        fp = fp.clip(0, 0, rsize1, rsize1)
        slice_of_file = rast.fp.slice_in(fp, clip=True)
        slice_in_file = fp.slice_in(rast.fp, clip=True)
        arr = rast.get_data(fp=fp, band=[-1], dst_nodata=dst_nodata).copy()
        if rast.fp.share_area(fp):
            assert np.all(
                dst_arr[slice_in_file] == arr[slice_of_file]
            )
            arr[slice_of_file] = dst_nodata
        assert np.all(arr == dst_nodata)

def test_set_data_rect(rast, dst_arr):
    rect_sizes = [
        1, 10, 15
    ]
    locations = [
        (-10, -10),
        (-9, -9),
        (-1, -1),
        (0, 0),
        (5, 5),
        (9, 9),
        (10, 10),
        (11, 11),
    ]
    dst_nodata = rast.nodata or 200
    for offset_px, rsize1 in itertools.product(locations, rect_sizes):
        rast.set_data(dst_arr, band=-1) # reset

        fp = rast.fp
        fp = fp.move(fp.tl + offset_px * fp.pxvec)
        fp = fp.clip(0, 0, rsize1, rsize1)
        slice_of_file = rast.fp.slice_in(fp, clip=True)
        slice_in_file = fp.slice_in(rast.fp, clip=True)

        arr1 = np.full(np.r_[fp.shape, len(rast)], 201, dtype=rast.dtype)
        rast.set_data(arr1, fp=fp, band=-1)
        arr2 = rast.get_data(band=[-1], dst_nodata=dst_nodata).copy()

        assert np.all(arr2[slice_in_file] == arr1[slice_of_file])
        arr2[slice_in_file] = dst_nodata
        assert np.all(arr2 != 201)

def test_set_data_mask(rast, dst_arr):
    fp = rast.fp.erode(rast.fp.rsemiminoraxis - 3)
    mask = np.zeros(rast.fp.shape, bool)
    mask[fp.slice_in(rast.fp)] = 1

    for mask_param in [fp.poly, mask]:
        rast.fill(band=-1, value=0)
        rast.set_data(dst_arr, band=-1, mask=mask_param)
        arr = rast.get_data(band=[-1])
        assert np.all(arr[mask] == dst_arr[mask])
        assert np.all(arr[~mask] == 0)

def test_get_data_channel_behavior(rast):
    c = len(rast)
    if c == 1:
        assert rast.get_data(band=-1).shape[2:] == ()
    else:
        assert rast.get_data(band=-1).shape[2:] == (c,)
    assert rast.get_data(band=[-1]).shape[2:] == (c,)
    assert rast.get_data(band=[1]).shape[2:] == (1,)
    assert rast.get_data(band=1).shape[2:] == ()

    if len(rast) == 3:
        for i in range(len(rast)):
            rast.fill(i * 10, i)
        assert np.all(rast.get_data(channels=[0, 1, 2]) == [[[0, 10, 20]]])
        assert np.all(rast.get_data(channels=[2, 1, 0]) == [[[20, 10, 0]]])
        assert np.all(rast.get_data(channels=[2, 1, 0, 1, 2]) == [[[20, 10, 0, 10, 20]]])
