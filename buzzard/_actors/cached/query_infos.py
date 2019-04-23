from typing import (
    Set, Dict, List, Sequence, Union, cast, NamedTuple, FrozenSet, Tuple, Mapping, AbstractSet
)
import collections
import queue # Should be imported for `mypy`
from types import MappingProxyType
import itertools

import numpy as np
from buzzard._footprint import Footprint

class ComputationFootprint(Footprint):
    """The Footprint that is passed to the user's computation function along with the
    dict of `collected arrays` and the dict of `CollectionFootprint` to compute the
    `computed array`.
    """

class CacheFootprint(Footprint):
    """The Footprint of a cache file. The matrix of `CacheFootprints` is always provided by the
    user when creating a recipe.

    A `cache array` is created by merging one or more `computation array` together. The merging
    function is provided by user, it might just be a 2d-concatenation.
    """

class SampleFootprint(Footprint):
    """A Footprint that has to be sampled from one or more cache files. This Footprint is aligned
    and contained in the raster.

    A `sample array` is allocated uninitiallized and cache files are read to this array.
    """

class ResampleFootprint(Footprint):
    """A Footprint that is part of a tiling of a `ProductionFootprint`. Most of the time this is a 1x1
    tiling, so a `ResampleFootprint` is usually the same as the `ProductionFootprint`.

    A `resample array` is created by transforming 0 or 1 2d-slice of a `sample array`. Usually this
    transformation is trivial (identity/np.full/copy/nodata-conversion), but if the
    `ResampleFootprint` shares area with the raster and is not on the same grid as the raster,
    a costly resampling will be performed on a pool using the `interpolation` algorithm chosen by
    the user.
    """

class ProductionFootprint(Footprint):
    """A Footprint that is requested by user

    A `production array` is created by 2d-concatenation of `resample arrays`
    """

class CacheProduceInfos(NamedTuple(
    'CacheProduceInfos', [
        ('fp', ProductionFootprint),
        ('same_grid', bool),
        ('share_area', bool),
        ('sample_fp', Union[None, SampleFootprint]),
        ('cache_fps', FrozenSet[CacheFootprint]),
        ('resample_fps', Tuple[ResampleFootprint, ...]),
        ('resample_cache_deps_fps', Mapping[ResampleFootprint, FrozenSet[CacheFootprint]]),
        ('resample_sample_dep_fp', Mapping[ResampleFootprint, Union[None, SampleFootprint]]),
    ],
)):
    """Object that stores many informations about an array to produce"""

class CachedQueryInfos(object):
    """Object that stores many informations about a query. Most attributes are immutable.
    An instance of this class identifies a query among the actors, hence the
    `__hash__` implementation.

    Classes' attributes are typed for documentation purposes and for validation with `mypy`
    """

    def __init__(self, raster, list_of_prod_fp,
                 channel_ids, is_flat, dst_nodata, interpolation,
                 max_queue_size,
                 parent_uid, key_in_parent):
        # Mutable attributes ******************************************************************** **
        # Attributes that relates a query to a single optional computation phase
        self.cache_computation = None # type: Union[None, CacheComputationInfos]

        # Immutable attributes ****************************************************************** **
        self.parent_uid = parent_uid
        self.key_in_parent = key_in_parent

        # The parameters given by user in invocation
        self.channel_ids = channel_ids # type: Sequence[int]
        self.is_flat = is_flat # type: bool
        self.unique_channel_ids = []
        for bi in channel_ids:
            if bi not in self.unique_channel_ids:
                self.unique_channel_ids.append(bi)
        self.unique_channel_ids = tuple(self.unique_channel_ids)

        self.dst_nodata = dst_nodata # type: Union[int, float]
        self.interpolation = interpolation # type: str

        # Output max queue size (Parameter given to queue.Queue)
        self.max_queue_size = max_queue_size # type: int

        # How many arrays are requested
        self.produce_count = len(list_of_prod_fp) # type: int

        # Build CacheProduceInfos objects **************************************
        to_zip = []

        # The list of Footprints requested
        list_of_prod_fp = list_of_prod_fp # type: List[ProductionFootprint]
        to_zip.append(list_of_prod_fp)

        # Boolean attribute of each `prod_fp`
        # If `True` the resampling phase has to be performed on a Pool
        list_of_prod_same_grid = [
            fp.same_grid(raster.fp)
            for fp in list_of_prod_fp
        ] # type: List[bool]
        to_zip.append(list_of_prod_same_grid)

        # Boolean attribute of each `prod_fp`
        # If `False` the queried footprint is outside of raster's footprint. It means that no
        # sampling is necessary and the outputed array will be full of `dst_nodata`
        list_of_prod_share_area = [
            fp.share_area(raster.fp)
            for fp in list_of_prod_fp
        ] # type: List[bool]
        to_zip.append(list_of_prod_share_area)

        # The full Footprint that needs to be sampled for each `prod_fp`
        # Is `None` if `prod_fp` is fully outside of raster
        list_of_prod_sample_fp = [] # type: List[Union[None, SampleFootprint]]
        to_zip.append(list_of_prod_sample_fp)

        # The set of cache Footprints that are needed for each `prod_fp`
        list_of_prod_cache_fps = [] # type: List[FrozenSet[CacheFootprint]]
        to_zip.append(list_of_prod_cache_fps)

        # The list of resamplings to perform for each `prod_fp`
        # Always at least 1 resampling per `prod_fp`
        list_of_prod_resample_fps = [] # type: List[Tuple[ResampleFootprint, ...]]
        to_zip.append(list_of_prod_resample_fps)

        # The set of `cache_fp` necessary per `resample_fp` for each `prod_fp`
        list_of_prod_resample_cache_deps_fps = [] # type: List[Mapping[ResampleFootprint, FrozenSet[CacheFootprint]]]
        to_zip.append(list_of_prod_resample_cache_deps_fps)

        # The full Footprint that needs to be sampled par `resample_fp` for each `prod_fp`
        # Might be `None` if `prod_fp` is fully outside of raster
        list_of_prod_resample_sample_dep_fp = [] # type: List[Mapping[ResampleFootprint, Union[None, SampleFootprint]]]
        to_zip.append(list_of_prod_resample_sample_dep_fp)

        it = zip(list_of_prod_fp, list_of_prod_same_grid, list_of_prod_share_area)
        # TODO: Speed up that piece of code
        # - Code footprint with lower level code
        # - Spawn a ProcessPoolExecutor when >100 prod. (The same could be done for fp.tile).
        # - What about a global process pool executor in `buzz.env`?
        for prod_fp, same_grid, share_area in it:
            if not share_area:
                # Resampling will be performed in one pass, on the scheduler
                list_of_prod_sample_fp.append(None)
                list_of_prod_cache_fps.append(frozenset())
                resample_fp = cast(ResampleFootprint, prod_fp)
                list_of_prod_resample_fps.append((resample_fp,))
                list_of_prod_resample_cache_deps_fps.append(MappingProxyType({resample_fp: frozenset()}))
                list_of_prod_resample_sample_dep_fp.append(MappingProxyType({resample_fp: None}))
            else:
                if same_grid:
                    # Remapping will be performed in one pass, on the scheduler
                    sample_fp = raster.fp & prod_fp
                    resample_fps = [cast(ResampleFootprint, prod_fp)]
                    sample_dep_fp = {
                        resample_fps[0]: sample_fp
                    }
                else:
                    sample_fp = raster.build_sampling_footprint_to_remap_interpolate(prod_fp, interpolation)

                    if raster.max_resampling_size is None:
                        # Remapping will be performed in one pass, on a Pool
                        resample_fps = [cast(ResampleFootprint, prod_fp)]
                        sample_dep_fp = {
                            resample_fps[0]: sample_fp
                        }
                    else:
                        # Resampling will be performed in several passes, on a Pool
                        rsize = np.maximum(prod_fp.rsize, sample_fp.rsize)
                        countx, county = np.ceil(rsize / raster.max_resampling_size).astype(int)
                        resample_fps = prod_fp.tile_count(
                            countx, county, boundary_effect='shrink'
                        ).flatten().tolist()
                        sample_dep_fp = {
                            resample_fp: (
                                raster.build_sampling_footprint_to_remap_interpolate(resample_fp, interpolation)
                                if resample_fp.share_area(raster.fp) else
                                None
                            )
                            for resample_fp in resample_fps
                        }

                resample_cache_deps_fps = MappingProxyType({
                    resample_fp: frozenset(raster.cache_fps_of_fp(sample_subfp))
                    for resample_fp in resample_fps
                    for sample_subfp in [sample_dep_fp[resample_fp]]
                    if sample_subfp is not None
                })
                for s in resample_cache_deps_fps.items():
                    assert len(s) > 0

                # The `intersection of the cache_fps with sample_fp` might not be the same as the
                # the `intersection of the cache_fps with resample_fps`!
                cache_fps = frozenset(itertools.chain.from_iterable(
                    resample_cache_deps_fps.values()
                ))
                assert len(cache_fps) > 0

                list_of_prod_cache_fps.append(cache_fps)
                list_of_prod_sample_fp.append(sample_fp)
                list_of_prod_resample_fps.append(tuple(resample_fps))
                list_of_prod_resample_cache_deps_fps.append(resample_cache_deps_fps)
                list_of_prod_resample_sample_dep_fp.append(MappingProxyType(sample_dep_fp))

        self.prod = tuple([
            CacheProduceInfos(*args)
            for args in zip(*to_zip)
        ]) # type: Tuple[CacheProduceInfos, ...]

        # Misc *****************************************************************
        # The list of all cache Footprints needed, ordered by priority
        self.list_of_cache_fp = [] # type: Sequence[CacheFootprint]
        seen = set()
        for fps in list_of_prod_cache_fps:
            for fp in fps:
                if fp not in seen:
                    seen.add(fp)
                    self.list_of_cache_fp.append(fp)
        self.list_of_cache_fp = tuple(self.list_of_cache_fp)
        del seen

        # The dict of cache Footprint to set of production idxs
        # For each `cache_fp`, the set of prod_idx that need this cache tile
        self.dict_of_prod_idxs_per_cache_fp = collections.defaultdict(set) # type: Mapping[CacheFootprint, AbstractSet[int]]
        for i, (prod_fp, cache_fps) in enumerate(zip(list_of_prod_fp, list_of_prod_cache_fps)):
            for cache_fp in cache_fps:
                self.dict_of_prod_idxs_per_cache_fp[cache_fp].add(i)
        for k, v in self.dict_of_prod_idxs_per_cache_fp.items():
            self.dict_of_prod_idxs_per_cache_fp[k] = frozenset(v)
        self.dict_of_prod_idxs_per_cache_fp = MappingProxyType(self.dict_of_prod_idxs_per_cache_fp)

        # The dict of cache Footprint to production_idx
        # For each `cache_fp`, the minimum prod_idx that need this cache tile
        self.dict_of_min_prod_idx_per_cache_fp = {} # type: Mapping[CacheFootprint, int]
        for k, v in self.dict_of_prod_idxs_per_cache_fp.items():
            self.dict_of_min_prod_idx_per_cache_fp[k] = min(v)
        self.dict_of_min_prod_idx_per_cache_fp = MappingProxyType(self.dict_of_min_prod_idx_per_cache_fp)

        # *************************************************************************************** **
    def __hash__(self):
        return id(self)

    def __eq__(self, other):
        return self is other

class CacheComputationInfos(object):
    """Object that store informations about a computation phase of a query.
    Instanciating this object also starts the primitives collection from the list of the cache
    footprints missing. Primitive collection consists of creating new raster queries to
    primitive rasters.
    (e.g. to compute all the missing `slopes` cache files required by a query, a
    single query to `dsm` will be opened)

    This object is instanciated for each query that requires missing cache file
    """

    def __init__(self, qi, raster, list_of_cache_fp):
        """
        Parameters
        ----------
        raster: _a_recipe_raster.ABackRecipeRaster
        list_of_cache_fp: sequence of CacheFootprint
            The subset of raster's cache footprints that are missing for a particular query
        """

        # Mutable **************************************************************
        self.collected_count = 0 # type: int

        # Immutable ************************************************************
        self.list_of_cache_fp = tuple(list_of_cache_fp) # type: Tuple[CacheFootprint, ...]

        # Step 1 - List compute Footprints sorted by priority
        l = []
        seen = set()
        prev_prod_idx = 0
        self.dict_of_min_prod_idx_per_compute_fp = {}
        for cache_fp in self.list_of_cache_fp:
            prod_idx = qi.dict_of_min_prod_idx_per_cache_fp[cache_fp]
            assert prod_idx >= prev_prod_idx
            prev_prod_idx = prod_idx
            for compute_fp in raster.compute_fps_of_cache_fp[cache_fp]:
                if compute_fp not in seen:
                    seen.add(compute_fp)
                    l.append(compute_fp)
                    self.dict_of_min_prod_idx_per_compute_fp[compute_fp] = prod_idx

        # Sort those tiles by using the same scheme as the WaitingRoom does
        l = sorted(l, key=lambda fp: (self.dict_of_min_prod_idx_per_compute_fp[fp], -fp.cy, +fp.cx))
        self.list_of_compute_fp = tuple(l) # type: Tuple[ComputationFootprint, ...]
        self.to_collect_count = len(self.list_of_compute_fp) # type: int
        del l, seen

        # Step 2 - List primtive Footprints
        self.primitive_fps_per_primitive = {
            name: tuple([func(fp) for fp in self.list_of_compute_fp])
            for name, func in raster.convert_footprint_per_primitive.items()
        }

        # Step 3 - Start collection phase
        self.primitive_queue_per_primitive = {
            name: prim_back.queue_data(
                self.primitive_fps_per_primitive[name],
                parent_uid=raster.uid,
                key_in_parent=(qi, name),
                **raster.primitives_kwargs[name]
            )
            for name, prim_back in raster.primitives_back.items()
        }
