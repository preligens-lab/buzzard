
import multiprocessing as mp
import multiprocessing
import shutil
import uuid
import tempfile
import functools
import os
import glob

import numpy as np
import pytest

import buzzard as buzz

def pytest_generate_tests(metafunc):
    if 'pools' in metafunc.fixturenames:
        argvalues = []
        for pval in [
                None,
                'lol', mp.pool.ThreadPool(2),
                mp.pool.Pool(2),
        ]:
            argvalues.append(dict(
                io={'io_pool': pval},
                computation={'computation_pool': pval},
                merge={'merge_pool': pval},
                resample={'resample_pool': pval},
            ))

        metafunc.parametrize(
            argnames='pools',
            argvalues=argvalues,
        )

@pytest.fixture
def test_prefix():
    path = os.path.join(tempfile.gettempdir(), 'buzz-ut-' + str(uuid.uuid4()))
    os.makedirs(path)
    yield path
    shutil.rmtree(path)

def test_(pools, test_prefix):
    def _open(**kwargs):
        d = dict(
            fp=fp, dtype='float32', band_count=2,
            compute_array=functools.partial(_meshgrid_raster_in, reffp=fp),
            cache_dir=test_prefix,
            **pools['merge'],
            **pools['resample'],
            **pools['computation'],
            **pools['io'],
        )
        d.update(kwargs)
        return ds.acreate_cached_raster_recipe(**d)

    def _test_get():
        arrs = r.get_data(band=-1)
        assert arrs.shape == tuple(np.r_[fp.shape, 2])
        x, y = arrs[..., 0], arrs[..., 1]
        xref, yref = fp.meshgrid_raster
        assert np.all(x == xref)
        assert np.all(y == yref)

    def _test_resampling(fp):
        print(fp)
        arr = r.get_data(band=-1, fp=fp)
        ref = npr.get_data(band=-1, fp=fp)
        assert np.allclose(arr, ref)

    print() # debug line
    fp = buzz.Footprint(
        rsize=(100,100),
        size=(100,100),
        tl=(1000, 1100),
    )
    print(fp)

    # Create a numpy array with the same data
    with buzz.DataSource(allow_interpolation=1).close as ds:

        npr = ds.awrap_numpy_raster(fp, np.stack(fp.meshgrid_raster, axis=2).astype('float32'))

        # Test lazyness of cache
        r = _open()
        files = glob.glob(os.path.join(test_prefix, '*.tif'))
        assert len(files) == 0

        # Test get_data results
        _test_get()
        files = glob.glob(os.path.join(test_prefix, '*.tif'))
        assert len(files) > 0
        mtimes0 = {f: os.stat(f).st_mtime for f in files}

        # Test persistence of cache
        # Test get_data results
        r.close()
        r = _open(compute_array=_should_not_be_called)
        _test_get()
        files = glob.glob(os.path.join(test_prefix, '*.tif'))
        assert len(files) > 0
        mtimes1 = {f: os.stat(f).st_mtime for f in files}
        assert mtimes0 == mtimes1

        # Test overwrite parameter
        # Test get_data results
        r.close()
        r = _open(o=True)
        _test_get()
        files = glob.glob(os.path.join(test_prefix, '*.tif'))
        assert len(files) > 0
        mtimes1 = {f: os.stat(f).st_mtime for f in files}
        assert mtimes0.keys() == mtimes1.keys()
        for k, t0 in mtimes0.items():
            t1 = mtimes1[k]
            assert t0 < t1

        # Test remapping #1 - Interpolation - Fully Inside
        fp_within_upscaled = fp.intersection(fp, scale=fp.scale / 2) & fp.erode(fp.rsemiminoraxis // 4)
        _test_resampling(fp_within_upscaled)

        # Test remapping #2 - Interpolation - Fully Outside
        _test_resampling(fp_within_upscaled.move(fp.br + fp.diagvec))

        # Test remapping #3 - No Interpolation - Fully Outside
        _test_resampling(fp.move(fp.br + fp.diagvec))

        # Test remapping #4 - Interpolation - Both in and out
        _test_resampling(fp_within_upscaled.move(fp.br - fp_within_upscaled.diagvec / 2))

        # Test remapping #5 - No Interpolation - Both in and out
        _test_resampling(fp.move(fp.br - fp.pxvec * fp.rsemiminoraxis))

        # Concurrent queries that need a cache file checksum
        r.close()
        r = _open()
        for it in [r.iter_data(fps=[fp], band=-1) for _ in range(10)]:
            next(it)

        # Concurrent queries that need a cache file missing, all but one computation aborted
        # because already launched
        r.close()
        r = _open(o=True)
        for it in [r.iter_data(fps=[fp], band=-1) for _ in range(10)]:
            next(it)



        # Corrupted cache file
        # iter_data of several items, more than cache_max, test backpressure with time.sleep
        # max resampling size
        # derived raster
        # in iter_data, the first one(s) don't need cache, the next ones need cache file checking
        # computation tiles

        # query garbage collected
        # raster closed during query


# Tools ***************************************************************************************** **
def _meshgrid_raster_in(fp, primitive_fps, primtive_arrays, raster, reffp):
    if raster is not None:
        assert raster.fp == reffp
    x, y = fp.meshgrid_raster_in(reffp)
    return np.stack([x, y], axis=2).astype('float32')

def _should_not_be_called(*args):
    assert False, _should_not_be_called
