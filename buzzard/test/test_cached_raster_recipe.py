import multiprocessing as mp
import multiprocessing.pool
import shutil
import uuid
import tempfile
import functools
import os
import glob
import time
import gc
import threading
import itertools

import numpy as np
import pytest

import buzzard as buzz

def pytest_generate_tests(metafunc):
    if 'pools' in metafunc.fixturenames:
        argvalues = []
        for pval in [
                None,
                'lol',
                mp.pool.ThreadPool(2),
                mp.pool.Pool(2),
        ]:
            # TODO: test with different pools
            # TODO: test with spawn/forks
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

test_prefix2 = test_prefix

@pytest.fixture(params=[
    (100, 100),
    (99, 99),
    (26, 26),
])
def cache_tiles(request):
    return request.param

def test_(pools, test_prefix, cache_tiles, test_prefix2):
    def _open(**kwargs):
        d = dict(
            fp=fp, dtype='float32', channel_count=2,
            compute_array=functools.partial(_meshgrid_raster_in, reffp=fp),
            cache_dir=test_prefix,
            cache_tiles=cache_tiles,
            **dict(itertools.chain(
                pools['merge'].items(),
                pools['resample'].items(),
                pools['computation'].items(),
                pools['io'].items(),
            ))
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
        arr = r.get_data(band=-1, fp=fp)
        ref = npr.get_data(band=-1, fp=fp)
        assert np.allclose(arr, ref)

    def _corrupt_files(files):
        for path in files:
            with open(path, 'wb') as stream:
                stream.write(b'42')

    print() # debug line
    fp = buzz.Footprint(
        rsize=(100, 100),
        size=(100, 100),
        tl=(1000, 1100),
    )
    compute_same_address_space = (
        type(pools['computation']['computation_pool']) in {str, mp.pool.ThreadPool, type(None)}
    )

    with buzz.Dataset(allow_interpolation=1).close as ds:
        # Create a numpy raster with the same data, useful to compare resampling
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
        r = _open(ow=True)
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

        # Test remapping #6 - Interpolation - Fully Inside - Tiled
        r.close()
        r = _open(max_resampling_size=20)
        _test_resampling(fp_within_upscaled)

        # Concurrent queries that need a cache file checksum
        r.close()
        r = _open()
        for it in [r.iter_data(fps=[fp], band=-1) for _ in range(10)]:
            next(it)

        # Concurrent queries that need a cache file missing, all but one computation aborted
        # because already launched
        r.close()
        r = _open(ow=True)
        for it in [r.iter_data(fps=[fp], band=-1) for _ in range(10)]:
            next(it)

        # Query garbage collected
        it1 = r.iter_data(fps=[fp] * 2, max_queue_size=1) # 2/2 ready, 1/2 sinked
        it2 = r.iter_data(fps=[fp] * 1, max_queue_size=1) # 1/1 ready, 0/1 sinked
        it3 = r.iter_data(fps=[fp] * 2, max_queue_size=1) # 1/2 ready, 0/2 sinked
        next(it1)
        time.sleep(1/2)

        del it1, it2, it3
        gc.collect()
        time.sleep(1 / 2)
        r.get_data() # This line will reraise any exception from scheduler

        # Raster closing during query
        it1 = r.iter_data(fps=[fp] * 2, max_queue_size=1) # 2/2 ready, 1/2 sinked
        it2 = r.iter_data(fps=[fp] * 1, max_queue_size=1) # 1/1 ready, 0/1 sinked
        it3 = r.iter_data(fps=[fp] * 2, max_queue_size=1) # 1/2 ready, 0/2 sinked
        next(it1)
        time.sleep(1/2)
        # Close Dataset instead of Raster, because Dataset.close is currently blocking

    with buzz.Dataset(allow_interpolation=1).close as ds:
        npr = ds.awrap_numpy_raster(fp, np.stack(fp.meshgrid_raster, axis=2).astype('float32'))

        # Corrupted cache file
        files = glob.glob(os.path.join(test_prefix, '*.tif'))
        mtimes0 = {f: os.stat(f).st_mtime for f in files}
        corrupted_path = files[0]
        _corrupt_files([corrupted_path])
        r = _open()

        r.get_data()
        mtimes1 = {f: os.stat(f).st_mtime for f in files}
        assert mtimes0.keys() == mtimes1.keys()
        for path in files:
            if path == corrupted_path:
                assert mtimes0[path] != mtimes1[path]
            else:
                assert mtimes0[path] == mtimes1[path]

    with buzz.Dataset(allow_interpolation=1).close as ds:
        npr = ds.awrap_numpy_raster(fp, np.stack(fp.meshgrid_raster, axis=2).astype('float32'))

        # In iter_data, the first one(s) don't need cache, the next ones need cache file checking and then recomputation
        _corrupt_files(glob.glob(os.path.join(test_prefix, '*.tif')))
        r = _open()
        fps = [
            fp.move(fp.br + fp.diagvec), # Outside
        ] + [fp] * 12
        arrs = list(r.iter_data(band=-1, fps=fps))
        assert len(arrs) == 13
        for tile, arr in zip(fps, arrs):
            assert np.all(arr == npr.get_data(band=-1, fp=tile))

    with buzz.Dataset(allow_interpolation=1).close as ds:
        npr = ds.awrap_numpy_raster(fp, np.stack(fp.meshgrid_raster, axis=2).astype('float32'))

        # Test channels order versus numpy raster
        r = _open()
        for channels in [
                0, 1, None, slice(None), [0, 1], [1, 0], [1, 0, 1],
        ]:
            assert np.all(r.get_data(channels=channels) == npr.get_data(channels=channels))

    with buzz.Dataset(allow_interpolation=1).close as ds:
        # Derived and primitive rasters not computed
        if compute_same_address_space:
            ac0, ac1 = _AreaCounter(fp), _AreaCounter(fp)
        else:
            ac0, ac1 = None, None
        r0 = _open(
            compute_array=functools.partial(_base_computation, area_counter=ac0, reffp=fp),
            ow=True,
        )
        r1 = _open(
            compute_array=functools.partial(_derived_computation, area_counter=ac1, reffp=fp),
            queue_data_per_primitive={'prim': functools.partial(r0.queue_data, band=-1)},
            cache_dir=test_prefix2,
            ow=True,
        )
        assert len(r0.primitives) == 0
        assert len(r1.primitives) == 1
        assert r1.primitives['prim'] is r0

        r1.get_data()
        if compute_same_address_space:
            ac0.check_done()
            ac1.check_done()
        r0.close()
        r1.close()

        # Derived raster not computed
        if compute_same_address_space:
            ac0, ac1 = _AreaCounter(fp), _AreaCounter(fp)
        else:
            ac0, ac1 = None, None
        r0 = _open(
            compute_array=functools.partial(_base_computation, area_counter=ac0, reffp=fp),
            ow=False,
        )
        r1 = _open(
            compute_array=functools.partial(_derived_computation, area_counter=ac1, reffp=fp),
            queue_data_per_primitive={'prim': functools.partial(r0.queue_data, band=-1)},
            cache_dir=test_prefix2,
            ow=True,
        )
        r1.get_data()
        if compute_same_address_space:
            ac0.check_not_done()
            ac1.check_done()
        r0.close()
        r1.close()

        # Primitive raster not computed
        if compute_same_address_space:
            ac0, ac1 = _AreaCounter(fp), _AreaCounter(fp)
        else:
            ac0, ac1 = None, None
        r0 = _open(
            compute_array=functools.partial(_base_computation, area_counter=ac0, reffp=fp),
            ow=True,
        )
        r1 = _open(
            compute_array=functools.partial(_derived_computation, area_counter=ac1, reffp=fp),
            queue_data_per_primitive={'prim': functools.partial(r0.queue_data, band=-1)},
            cache_dir=test_prefix2,
            ow=False,
        )
        r1.get_data()
        if compute_same_address_space:
            ac0.check_not_done()
            ac1.check_not_done()
        r0.close()
        r1.close()

        # Test computation tiles
        if compute_same_address_space:
            ac0, ac1 = _AreaCounter(fp), _AreaCounter(fp)
        else:
            ac0, ac1 = None, None
        r0 = _open(
            compute_array=functools.partial(_base_computation, area_counter=ac0, reffp=fp),
            computation_tiles=(11, 11),
            ow=True,
        )
        r1 = _open(
            compute_array=functools.partial(_derived_computation, area_counter=ac1, reffp=fp),
            queue_data_per_primitive={'prim': functools.partial(r0.queue_data, band=-1)},
            cache_dir=test_prefix2,
            computation_tiles=(22, 22),
            ow=True,
        )
        r1.get_data()
        if compute_same_address_space:
            ac0.check_done()
            ac1.check_done()
        r0.close()
        r1.close()

        # Several queries, one is dropped, the rest is still working
        r0 = _open(
            compute_array=functools.partial(_base_computation, reffp=fp),
            ow=True,
        )
        r1 = _open(
            compute_array=functools.partial(_derived_computation, reffp=fp),
            queue_data_per_primitive={'prim': functools.partial(r0.queue_data, band=-1)},
            cache_dir=test_prefix2,
            ow=True,
        )
        t = r1.cache_tiles.flatten()
        fps0 = t.tolist() * 2
        fps1 = fps0[::-1]
        fps2 = np.roll(t, t.size // 2).tolist() * 2
        fps3 = fps2[::-1]

        it0 = r1.iter_data(fps=fps0)
        it1 = r1.iter_data(fps=fps1)
        it2 = r1.iter_data(fps=fps2)
        it3 = r1.iter_data(fps=fps3)
        del it1

        assert len(list(it3)) == t.size * 2
        assert len(list(it0)) == t.size * 2
        assert len(list(it2)) == t.size * 2

        r0.close()
        r1.close()

        # Computation function crashes, we catch error in main thread
        r = _open(ow=True, compute_array=_please_crash)
        with pytest.raises(NecessaryCrash):
            r.get_data()

# Tools ***************************************************************************************** **
class _AreaCounter(object):
    def __init__(self, fp):
        self._lock = threading.Lock()
        self._fp = fp
        self._mask = np.zeros(fp.shape, 'uint8')

    def increment(self, fp):
        with self._lock:
            self._mask[fp.slice_in(self._fp)] += 1

    def check_not_done(self):
        assert np.all(self._mask == 0)

    def check_done(self):
        assert np.all(self._mask == 1)

def _base_computation(fp, primitive_fps, primtive_arrays, raster, reffp, area_counter=None):
    if area_counter is not None:
        area_counter.increment(fp)
    x, y = fp.meshgrid_raster_in(reffp)
    return np.stack([x, y], axis=2).astype('float32')

def _derived_computation(fp, primitive_fps, primtive_arrays, raster, reffp, area_counter=None):
    if area_counter is not None:
        area_counter.increment(fp)
    assert fp == primitive_fps['prim']
    x, y = fp.meshgrid_raster_in(reffp)
    return np.stack([x, y], axis=2).astype('float32') * primtive_arrays['prim']

def _meshgrid_raster_in(fp, primitive_fps, primtive_arrays, raster, reffp):
    if raster is not None:
        assert raster.fp == reffp
    x, y = fp.meshgrid_raster_in(reffp)
    return np.stack([x, y], axis=2).astype('float32')

class NecessaryCrash(Exception):
    pass

def _please_crash(fp, primitive_fps, primtive_arrays, raster):
    raise NecessaryCrash()

def _should_not_be_called(*args):
    assert False, _should_not_be_called
