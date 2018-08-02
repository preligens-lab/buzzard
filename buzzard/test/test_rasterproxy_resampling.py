"""Resampling tests for *Raster.get_data() methods (not testing rotations)

TODO: Unit test set_data with resampling
TODO: Unit test `remap` mixin on one side and `getsetdata_remap` elsewhere
"""

# pylint: disable=redefined-outer-name

from __future__ import division, print_function
import itertools
import os
import uuid
import tempfile

import numpy as np
import pytest

from buzzard import Footprint, DataSource

# CONSTANTS - INTERPOLATIONS ******************************************************************** **
INTERPOLATIONS = [
    'cv_area',
    'cv_linear',
    'cv_nearest',
    'cv_cubic',
    'cv_lanczos4',
]
INTERPOLATIONS_MAX_VALUE_ERROR = {
    'cv_area': 0.023,
    'cv_linear': 0.023,
    'cv_nearest': 1.,
    'cv_cubic': 0.11,
    'cv_lanczos4': 0.23,
}
INTERPOLATIONS_INSIDE_UNCERTAINTY_BORDER = {
    'cv_area': 0,
    'cv_linear': 0,
    'cv_nearest': 0,
    'cv_cubic': 1, # Massive spikes near borders. Nodata holes near topleft borders.
    'cv_lanczos4': 3, # Nodata holes near topleft borders.
}
INTERPOLATIONS_OUTSIDE_UNCERTAINTY_BORDER = {
    'cv_area': 1,
    'cv_linear': 1,
    'cv_nearest': 2,
    'cv_cubic': 2,
    'cv_lanczos4': 2,
}

# CONSTANTS - TIF GENERATION ******************************************************************** **
TIF_NODATA = 200
TIF_FP = Footprint(
    tl=(100, 110), size=(10, 10), rsize=(10, 10)
)
TIF_VALUES = np.add(*TIF_FP.meshgrid_raster).astype('float32')
TIF_VALUES[:1] = TIF_NODATA
TIF_VALUES[:, :2] = TIF_NODATA

XS, YS = TIF_FP.meshgrid_spatial
TIF_DATAMASK = TIF_VALUES != TIF_NODATA
TIF_DATA_MAXY = YS[TIF_DATAMASK].max()
TIF_DATA_MINY = YS[TIF_DATAMASK].min()
TIF_DATA_MAXX = XS[TIF_DATAMASK].max()
TIF_DATA_MINX = XS[TIF_DATAMASK].min()
TIF_MAXY = YS.max()
TIF_MINY = YS.min()
TIF_MAXX = XS.max()
TIF_MINX = XS.min()
del XS, YS

# CONSTANTS - SCENARIOS ************************************************************************* **
# All tested sizes should be multiples of all resolutions tested ******************************** **
DATA_FULL_LOADING = {
    'size': [12],
    'scale': [3/1, 3/2, 3/3, 3/4, 3/6, 3/7],
    'offset': [0., 0.5, np.sqrt(2), 2., np.sqrt(2) + 1],
}

ZONES_EDGES_TESTS = {
    'size': [1, 9, 10, 14],
    'scale': [1.],
    'offset': [-15, -4, -0.5, 0., 1.5, 2., 2.5],
}

SCENARIOS = [DATA_FULL_LOADING, ZONES_EDGES_TESTS]

# FIXTURES ************************************************************************************** **
@pytest.fixture(scope='module')
def ds():
    return DataSource(allow_interpolation=1)

@pytest.fixture(
    scope='module',
    params=[
        ('GTiff', 1, 'float32', TIF_NODATA),
        ('GTiff', 3, 'uint8', None),
        ('MEM', 1, 'float32', TIF_NODATA),
        ('MEM', 3, 'uint8', None),
        ('numpy', 1, 'float32', None),
        ('numpy', 3, 'uint8', TIF_NODATA),
    ],
)
def rast(request, ds):
    """Fixture for the datasource creation"""
    fp = TIF_FP
    driver, band_count, dtype, nodata = request.param
    if driver == 'numpy':
        rast = ds.awrap_numpy_raster(
            fp,
            np.dstack([TIF_VALUES.copy().astype(dtype=dtype)] * band_count),
            band_schema=dict(nodata=nodata),
            sr=None,
            mode='r',
        )
    elif driver == 'MEM':
        rast = ds.acreate_raster(
            '', fp, dtype, band_count, band_schema=dict(nodata=nodata), driver='MEM',
        )
        for band_id in range(1, len(rast) + 1):
            rast.set_data(TIF_VALUES, band=band_id)
    else:
        path = '{}/{}.tif'.format(tempfile.gettempdir(), uuid.uuid4())
        rast = ds.acreate_raster(
            path, fp, dtype, band_count, band_schema=dict(nodata=nodata), driver=driver
        )
        for band_id in range(1, len(rast) + 1):
            rast.set_data(TIF_VALUES, band=band_id)
    yield rast
    if driver in {'numpy', 'MEM'}:
        rast.close()
    else:
        rast.delete()

def pytest_generate_tests(metafunc):
    values = []
    for scenar in SCENARIOS:
        it = itertools.product(
            scenar['size'], scenar['scale'], scenar['offset'], [[1, 1], [-1, 1], [1, -1], [-1, -1]], INTERPOLATIONS,
        )
        values += it
    metafunc.parametrize(
        argnames='size1,scale1,offset1,offset_factor2,interpolation',
        argvalues=values
    )

@pytest.fixture()
def fp(size1, scale1, offset1, offset_factor2):
    tl = TIF_FP.tl + [offset1, offset1] * np.asarray(offset_factor2)
    size = [size1, size1]
    rsize = (size / np.asarray(scale1)).astype(int)
    assert (size == rsize * scale1).all()
    fp = Footprint(tl=tl, size=size, rsize=np.abs(rsize))
    return fp

# TESTS ***************************************************************************************** **
def test_getdata(rast, fp, interpolation):
    if rast.nodata is None:
        all_res = rast.get_data(band=-1, fp=fp, interpolation=interpolation, dst_nodata=TIF_NODATA)
    else:
        all_res = rast.get_data(band=-1, fp=fp, interpolation=interpolation)

    all_res = np.atleast_3d(all_res)

    for i in range(len(rast)):
        res = all_res[..., i]

        # 1 - Assert absent nodata within input pixels (pixels being points not areas)
        xs, ys = fp.meshgrid_spatial
        b = INTERPOLATIONS_INSIDE_UNCERTAINTY_BORDER[interpolation]
        deep_inside_data_mask = (
            (xs > TIF_DATA_MINX + TIF_FP.pxsizex * b) &
            (xs < TIF_DATA_MAXX - TIF_FP.pxsizex * b) &
            (ys > TIF_DATA_MINX + TIF_FP.pxsizex * b) &
            (ys < TIF_DATA_MAXX - TIF_FP.pxsizex * b)
        )
        assert np.all(
            res[deep_inside_data_mask] != TIF_NODATA
        )

        # 2 - Test the difference of values between two neighboring columns/rows
        if deep_inside_data_mask.any():
            xs, ys = fp.meshgrid_raster
            data_slice = (
                slice(ys[deep_inside_data_mask].min(), ys[deep_inside_data_mask].max() + 1),
                slice(xs[deep_inside_data_mask].min(), xs[deep_inside_data_mask].max() + 1),
            )
            below_minus_above = np.diff(res[data_slice], axis=0)
            right_minus_left = np.diff(res[data_slice], axis=1)

            vertical_errors = np.abs(below_minus_above - fp.pxsizex)
            horizontal_errors = np.abs(right_minus_left - fp.pxsizex)
            if np.issubdtype(rast.dtype, np.integer):
                maxerr = np.ceil(INTERPOLATIONS_MAX_VALUE_ERROR[interpolation])
            else:
                maxerr = INTERPOLATIONS_MAX_VALUE_ERROR[interpolation]
            if vertical_errors.size:
                assert vertical_errors.max() <= maxerr
            if horizontal_errors.size:
                assert horizontal_errors.max() <= maxerr

        # 3 - Assert only nodata far from data
        xs, ys = fp.meshgrid_spatial
        b = INTERPOLATIONS_OUTSIDE_UNCERTAINTY_BORDER[interpolation]
        if rast.nodata is not None:
            far_outside_data_mask = (
                (xs < TIF_DATA_MINX - TIF_FP.pxsizex * b) |
                (xs > TIF_DATA_MAXX + TIF_FP.pxsizex * b) |
                (ys < TIF_DATA_MINX - TIF_FP.pxsizex * b) |
                (ys > TIF_DATA_MAXX + TIF_FP.pxsizex * b)
            )
        else:
            far_outside_data_mask = (
                (xs < TIF_MINX - TIF_FP.pxsizex * b) |
                (xs > TIF_MAXX + TIF_FP.pxsizex * b) |
                (ys < TIF_MINX - TIF_FP.pxsizex * b) |
                (ys > TIF_MAXX + TIF_FP.pxsizex * b)
            )
        assert np.all(
            res[far_outside_data_mask] == TIF_NODATA
        )
