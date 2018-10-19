import collections
import weakref

import numpy as np
import rtree.index # TODO: add rtree to deps

from buzzard._actors.message import Msg
from buzzard import _tools
from buzzard._footprint import Footprint
from buzzard._a_raster_recipe import ARasterRecipe, ABackRasterRecipe

from buzzard._actors.cached.cache_extractor import ActorCacheExtractor
from buzzard._actors.cached.cache_supervisor import ActorCacheSupervisor
from buzzard._actors.cached.file_checker import ActorFileChecker
from buzzard._actors.cached.merger import ActorMerger
from buzzard._actors.cached.producer import ActorProducer
from buzzard._actors.cached.queries_handler import ActorQueriesHandler
from buzzard._actors.cached.reader import ActorReader
from buzzard._actors.cached.writer import ActorWriter
from buzzard._actors.computation_accumulator import ActorComputationAccumulator
from buzzard._actors.computation_gate1 import ActorComputationGate1
from buzzard._actors.computation_gate2 import ActorComputationGate2
from buzzard._actors.computer import ActorComputer
from buzzard._actors.production_gate import ActorProductionGate
from buzzard._actors.resampler import ActorResampler

class CachedRasterRecipe(ARasterRecipe):
    """TODO: docstring"""
    def __init__(
        self, ds,
        fp, dtype, band_count, band_schema, sr,
        compute_array, merge_arrays,
        cache_dir, primitives_back, primitives_kwargs, convert_footprint_per_primitive,
        computation_pool, merge_pool, io_pool, resample_pool,
        cache_tiles, computation_tiles,
        max_resampling_size,
        debug_observers,
    ):
        back = BackCachedRasterRecipe(
            ds._back,
            weakref.proxy(self),
            fp, dtype, band_count, band_schema, sr,
            compute_array, merge_arrays,
            cache_dir, primitives_back, primitives_kwargs, convert_footprint_per_primitive,
            computation_pool, merge_pool, io_pool, resample_pool,
            cache_tiles, computation_tiles,
            max_resampling_size,
            debug_observers,
        )
        super().__init__(ds=ds, back=back)

    @property
    def cache_tiles(self):
        return self._back.cache_fps.copy()

class BackCachedRasterRecipe(ABackRasterRecipe):
    """TODO: docstring"""

    def __init__(
        self, back_ds, facade_proxy,
        fp, dtype, band_count, band_schema, sr,
        compute_array, merge_arrays,
        cache_dir, primitives_back, primitives_kwargs, convert_footprint_per_primitive,
        computation_pool, merge_pool, io_pool, resample_pool,
        cache_tiles, computation_tiles,
        max_resampling_size,
        debug_observers,
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
            merge_arrays=merge_arrays,
            primitives_back=primitives_back,
            primitives_kwargs=primitives_kwargs,
            convert_footprint_per_primitive=convert_footprint_per_primitive,

            # Scheduled
            resample_pool=resample_pool,
            max_resampling_size=max_resampling_size,
            debug_observers=debug_observers,
        )
        self.io_pool = io_pool
        self.cache_fps = cache_tiles
        self.cache_dir = cache_dir

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
        self.back_ds.put_message(Msg(
            '/Global/TopLevel', 'new_raster', self,
        ))

    # ******************************************************************************************* **
    def cache_fps_of_fp(self, fp):
        assert fp.same_grid(self.fp)
        rtl = self.fp.spatial_to_raster(fp.tl, dtype=float)
        bounds = np.r_[rtl, rtl + fp.rsize]# + bounds_inset
        return [
            self.cache_fps.flat[i]
            for i in list(self._cache_footprint_index.intersection(bounds))
        ]

    def fname_prefix_of_cache_fp(self, cache_fp):
        y, x = self.indices_of_cache_fp[cache_fp]
        params = np.r_[
            x,
            y,
            self.fp.spatial_to_raster(cache_fp.tl),
        ]
        return "x{:03d}-y{:03d}_x{:05d}-y{:05d}".format(*params) # TODO: better file name

    def create_actors(self):
        actors = [
            ActorCacheExtractor(self),
            ActorCacheSupervisor(self),
            ActorFileChecker(self),
            ActorMerger(self),
            ActorProducer(self),
            ActorQueriesHandler(self),
            ActorReader(self),
            ActorWriter(self),
            ActorComputationAccumulator(self),
            ActorComputationGate1(self),
            ActorComputationGate2(self),
            ActorComputer(self),
            ActorProductionGate(self),
            ActorResampler(self),
        ]
        return actors

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
