import collections
import weakref
import glob
import os

import numpy as np
import rtree.index

from buzzard._actors.message import Msg
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
    """Concrete class defining the behavior of a raster computed on the fly and fills a cache to
    avoid subsequent computations.

    >>> help(Dataset.create_cached_raster_recipe)

    """
    def __init__(
        self, ds,
        fp, dtype, channel_count, channels_schema, sr,
        compute_array, merge_arrays,
        cache_dir, overwrite,
        primitives_back, primitives_kwargs, convert_footprint_per_primitive,
        computation_pool, merge_pool, io_pool, resample_pool,
        cache_tiles, computation_tiles,
        max_resampling_size,
        debug_observers,
    ):
        back = BackCachedRasterRecipe(
            ds._back,
            weakref.proxy(self),
            fp, dtype, channel_count, channels_schema, sr,
            compute_array, merge_arrays,
            cache_dir, overwrite,
            primitives_back, primitives_kwargs, convert_footprint_per_primitive,
            computation_pool, merge_pool, io_pool, resample_pool,
            cache_tiles, computation_tiles,
            max_resampling_size,
            debug_observers,
        )
        super().__init__(ds=ds, back=back)

    @property
    def cache_tiles(self):
        """Cache tiles provided or created at construction"""
        return self._back.cache_fps.copy()

    @property
    def cache_dir(self):
        """Cache directory path provided at construction"""
        return self._back.cache_dir

class BackCachedRasterRecipe(ABackRasterRecipe):
    """Implementation of CachedRasterRecipe's specifications"""

    def __init__(
        self, back_ds, facade_proxy,
        fp, dtype, channel_count, channels_schema, sr,
        compute_array, merge_arrays,
        cache_dir, overwrite,
        primitives_back, primitives_kwargs, convert_footprint_per_primitive,
        computation_pool, merge_pool, io_pool, resample_pool,
        cache_tiles, computation_tiles,
        max_resampling_size,
        debug_observers,
    ):
        super().__init__(
            # Source
            back_ds=back_ds,
            wkt_stored=sr,

            # RasterSource
            channels_schema=channels_schema,
            dtype=dtype,
            fp_stored=fp,
            channel_count=channel_count,

            # Recipe
            facade_proxy=facade_proxy,
            computation_pool=computation_pool,
            merge_pool=merge_pool,
            compute_array=compute_array,
            merge_arrays=merge_arrays,
            primitives_back=primitives_back,
            primitives_kwargs=primitives_kwargs,
            convert_footprint_per_primitive=convert_footprint_per_primitive,

            # Async
            resample_pool=resample_pool,
            max_resampling_size=max_resampling_size,
            debug_observers=debug_observers,
        )
        self.io_pool = io_pool
        self.cache_fps = cache_tiles
        self.cache_dir = cache_dir
        self.overwrite = overwrite

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
        bounds = np.r_[rtl, rtl + fp.rsize]
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
        return "buzz_x{:03d}-y{:03d}_x{:05d}-y{:05d}".format(*params)

    def list_cache_path_candidates(self, cache_fp=None):
        if cache_fp is not None:
            prefix = self.fname_prefix_of_cache_fp(cache_fp)
            s = os.path.join(self.cache_dir, prefix + '_[0123456789abcdef]*.tif') # TODO: Use regex
            return glob.glob(s)
        else:
            s = os.path.join(
                self.cache_dir,
                 # TODO: Use regex
                'buzz_x[0-9]*-y[0-9]*_x[0-9]*-y[0-9]*_[0123456789abcdef]*.tif',
            )
            return glob.glob(s)

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
        for a in actors:
            self.debug_mngr.event('object_allocated', a)
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
