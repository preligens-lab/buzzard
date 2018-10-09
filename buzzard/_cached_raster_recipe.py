import collections
import queue
import weakref

import numpy as np
import rtree.index # TODO: add rtree to deps

from buzzard._actors.message import Msg
from buzzard import _tools
from buzzard._footprint import Footprint
from buzzard._a_raster_recipe import ARasterRecipe, ABackRasterRecipe

class CachedRasterRecipe(ARasterRecipe):
    """TODO: docstring"""
    def __init__(
        self, ds,
        fp, dtype, band_count, band_schema, sr,
        compute_array, merge_array,
        cache_dir, primitives_back, primitives_kwargs, convert_footprint_per_primitive,
        computation_pool, merge_pool, io_pool, resample_pool,
        cache_tiles,computation_tiles,
        max_resampling_size
    ):
        print('//////////////////////////////////////////////////')
        print('CachedRasterRecipe')
        print(band_schema)
        print('//////////////////////////////////////////////////')

        back = BackCachedRasterRecipe(
            ds._back,
            weakref.proxy(self),
            fp, dtype, band_count, band_schema, sr,
            compute_array, merge_array,
            cache_dir, primitives_back, primitives_kwargs, convert_footprint_per_primitive,
            computation_pool, merge_pool, io_pool, resample_pool,
            cache_tiles,computation_tiles,
            max_resampling_size
        )
        super().__init__(ds=ds, back=back)

class BackCachedRasterRecipe(ABackRasterRecipe):
    """TODO: docstring"""

    def __init__(
        self, back_ds, facade_proxy,
        fp, dtype, band_count, band_schema, sr,
        compute_array, merge_array,
        cache_dir, primitives_back, primitives_kwargs, convert_footprint_per_primitive,
        computation_pool, merge_pool, io_pool, resample_pool,
        cache_tiles,computation_tiles,
        max_resampling_size
    ):
        super().__init__(
            # Proxy
            back_ds=back_ds,
            wkt_stored=sr,

            # RasterProxy
            band_schema=band_schema,
            dtype=dtype,
            fp_stored=fp,
            band_count=band_count,

            # Recipe
            facade_proxy=facade_proxy,
            computation_pool=computation_pool,
            merge_pool=merge_pool,
            compute_array=compute_array,
            merge_array=merge_array,
            primitives_back=primitives_back,
            primitives_kwargs=primitives_kwargs,
            convert_footprint_per_primitive=convert_footprint_per_primitive,

            # Scheduled
            resample_pool=resample_pool,
            max_resampling_size=max_resampling_size,
        )
        self.io_pool = io_pool
        self._cache_tiles = cache_tiles

        # Tilings shortcuts ****************************************************
        self._cache_footprint_index = self._build_cache_fps_index(
            cache_tiles,
        )
        self.cache_fps_of_compute_fp = {
            compute_fp: self.cache_fps_of_fp(compute_fp)
            for compute_fp in computation_tiles.flat
        }
        self.compute_fps_of_cache_fp = collections.defaultdict(list)
        for compute_fp, cache_fps in self.cache_fps_of_compute_fp.items():
            for cache_fp in cache_fps:
                self.compute_fps_of_cache_fp[cache_fp].append(compute_fp)
        self.indices_of_cache_fp = {
            cache_fp: indices
            for indices, cache_fp in np.ndenumerate(cache_tiles)
        }

        # Scheduler notification ***********************************************
        if not back_ds.started:
            back_ds.start_scheduler()
        back_ds.put_message(Msg(
            '/Global/TopLevel', 'new_raster', self,
        ))

    def queue_data(self, fps, band_ids, dst_nodata, interpolation, max_queue_size, is_flat,
                   parent_uid, key_in_parent):
        q = queue.Queue(max_queue_size)
        back_ds.put_message(Msg(
            '/Raster{}/QueriesHandler'.format(id(self)),
            'new_query',
            weakref.ref(q),
            is_flat,
            dst_nodata,
            interpolation,
            max_queue_size,
            parent_uid,
            key_in_parent
        ))
        return q

    def get_data(self, fp, band_ids, dst_nodata, interpolation):
        q = self.queue_data(
            [fp], band_ids, dst_nodata, interpolation, 1,
            False, # `is_flat` is not important since caller reshapes output
            None, None,
        )
        return q.get()

    # ******************************************************************************************* **
    def cache_fps_of_fp(self, fp):
        assert fp.same_grid(self.fp)
        rtl = self.fp.spatial_to_raster(fp.tl, dtype=float)
        bounds = np.r_[rtl, rtl + fp.rsize]# + bounds_inset
        return [
            self._cache_tiles.flat[i]
            for i in list(self._cache_footprint_index.intersection(bounds))
        ]

    def fname_prefix_of_cache_fp(self, cache_fp):
        y, x = self.indices_of_cache_fp[cache_fp]
        params = np.r_[
            x,
            y,
            self.fp.spatial_to_raster(cache_fp.tl),
        ]
        return "tile_x{:03d}-y{:03d}_px_x{:05d}-y{:05d}".format(*params) # TODO: better file name


    # ******************************************************************************************* **
    def _build_cache_fps_index(self, cache_fps):
        idx = rtree.index.Index()
        bounds_inset = np.asarray([
            + 1 / 4,
            + 1 / 4,
            - 1 / 4,
            - 1 / 4,
        ])
        for i, fp in enumerate(cache_fps.flat):
            rtl = self.fp.spatial_to_raster(fp.tl, dtype=float)
            bounds = np.r_[rtl, rtl + fp.rsize] + bounds_inset
            idx.insert(i, bounds)
        return idx
