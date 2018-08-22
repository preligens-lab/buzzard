from typing import Set, Dict, List, Sequence, Union, cast, NamedTuple, FrozenSet, Tuple, Mapping, AbstractSet
import collections
import queue # Should be imported for `mypy`
from types import MappingProxyType

import numpy as np
from buzzard._footprint import Footprint

class ProdFootprint(Footprint):
    """A Footprint that is requested by user"""

class CacheFootprint(Footprint):
    """The Footprint of a cache file"""

class SampleFootprint(Footprint):
    """A Footprint that has to be sampled from cache files. This Footprint is on the same grid and
    contained in the raster.
    """

class ResampleFootprint(Footprint):
    """A Footprint that is part of a tiling of a `ProdFootprint`. Most of the time this is a 1x1
    tiling, so a `ResampleFootprint` is usually the same as the `ProdFootprint`.
    If the `ResampleFootprint` shares area with the raster and is not on the same grid as the
    raster, a costly resampling will be performed on a pool using the algorithm given by
    an `interpolation` parameter.
    """

class CacheProduceInfos(NamedTuple(
    'CacheProduceInfos', [
        ('fp', ProdFootprint),
        ('same_grid', bool),
        ('share_area', bool),
        ('sample_fp', SampleFootprint),
        ('cache_fps', FrozenSet[CacheFootprint]),
        ('resample_fps', Tuple[ResampleFootprint]),
        ('resample_cache_deps_fps', Mapping[ResampleFootprint, FrozenSet[CacheFootprint]]),
        ('resample_sample_dep_fp', Mapping[ResampleFootprint, Union[None, SampleFootprint]]),
    ],
)):
    """Object that stored many informations about an array to produce"""

class CachedQueryInfos(object):
    """Object that stores many informations about a query. Most attributes are immutable.
    An instance of this class identifies a query among the actors, hence the
    `__hash__` implementation.

    Classes' attributes are typed for documentation purposes and for validation with `mypy`
    """

    def __init__(self, raster, list_of_prod_fp,
                 band_ids, dst_nodata, interpolation,
                 max_queue_size):
        # Mutable attributes ******************************************************************** **
        # Since a Query might require missing cache files, some other queries might need to be
        # opened to primitive arrays
        # (e.g. to compute all the missing `slopes` cache files required by a query, a
        # single query to `dsm` will be opened)
        self.primitive_queues = None # type: Union[None, Dict[str, queue.Queue]]

        # Immutable attributes ****************************************************************** **
        # The parameters given by user in invocation
        self.band_ids = band_ids # type: Sequence[int]
        self.dst_nodata = dst_nodata # type: Union[int, float]
        self.interpolation = interpolation # type: str

        # Output max queue size (Parameter given to queue.Queue)
        self.max_queue_size = max_queue_size # type: int

        # How many arrays are requested
        self.produce_count = len(list_of_prod_fp) # type: int

        # Build CacheProduceInfos objects **************************************
        to_zip = []

        # The list of Footprints requested
        list_of_prod_fp = list_of_prod_fp # type: List[ProdFootprint]
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
        list_of_prod_resample_fps = [] # type: List[Tuple[ResampleFootprint]]
        to_zip.append(list_of_prod_resample_fps)

        # The set of `cache_fp` necessary per `resample_fp` for each `prod_fp`
        list_of_prod_resample_cache_deps_fps = [] # type: List[Mapping[ResampleFootprint, FrozenSet[CacheFootprint]]]
        to_zip.append(list_of_prod_resample_cache_deps_fps)

        # The full Footprint that needs to be sampled par `resample_fp` for each `prod_fp`
        # Might be `None` if `prod_fp` is fully outside of raster
        list_of_prod_resample_sample_dep_fp = [] # type: List[Mapping[ResampleFootprint, Union[None, SampleFootprint]]]
        to_zip.append(list_of_prod_resample_sample_dep_fp)

        it = zip(list_of_prod_fp, list_of_prod_sample_fp, list_of_prod_share_area)
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
                    # Resampling will be performed in one pass, on the scheduler
                    sample_fp = raster.fp & prod_fp
                    resample_fps = [cast(ResampleFootprint, prod_fp)]
                    sample_dep_fp = {
                        resample_fps[0]: sample_fp
                    }
                else:
                    sample_fp = raster.build_sampling_footprint_to_remap(prod_fp, interpolation)

                    if raster.max_resampling_size is None:
                        # Resampling will be performed in one pass, on a Pool
                        resample_fps = [cast(ResampleFootprint, prod_fp)]
                        sample_dep_fp = {
                            resample_fps[0]: sample_fp
                        }
                    else:
                        # Resampling will be performed in several passes, on a Pool
                        rsize = np.maximum(prod_fp.rsize, sample_fp.rsize)
                        countx, county = np.ceil(rsize / raster.max_resampling_size).astype(int)
                        resample_fps = sample_fp.tile_count(
                            (countx, county), boundary_effect='shrink'
                        ).flatten().tolist()
                        sample_dep_fp = {
                            resample_fp: raster.build_sampling_footprint_to_remap(resample_fp, interpolation)
                            for resample_fp in resample_fps
                        }

                list_of_prod_sample_fp.append(sample_fp)
                list_of_prod_cache_fps.append(
                    frozenset(raster.cache_fps_of_fp(sample_fp))
                )
                list_of_prod_resample_fps.append(tuple(resample_fps))
                list_of_prod_resample_cache_deps_fps.append(MappingProxyType({
                    resample_fp: frozenset(raster.cache_fps_of_fp(resample_fp))
                    for resample_fp in resample_fps
                }))
                list_of_prod_resample_sample_dep_fp.append(MappingProxyType(sample_dep_fp))

        self.prod = tuple([
            CacheProduceInfos(*args)
            for args in zip(*to_zip)
        ]) # type: Tuple[CacheProduceInfos]

        # Misc *****************************************************************
        # The list of all cache Footprints needed, ordered by priority
        self.list_of_cache_fp = [] # type: Sequence[CacheFootprint]
        seen = set()
        for fps in self.list_of_prod_cache_fps:
            for fp in fps:
                if fp not in seen:
                    seen.add(fp)
                    self.list_of_cache_fp.append(fp)
        self.list_of_cache_fp = tuple(self.list_of_cache_fp)
        del seen

        # The dict of cache Footprint to set of production idxs
        self.dict_of_cache_prod_idxs = collections.defaultdict(set) # type: Mapping[CacheFootprint, AbstractSet[int]]
        for i, (prod_fp, cache_fps) in enumerate(zip(self.list_of_prod_fp, self.list_of_prod_cache_fps)):
            for cache_fp in cache_fps:
                self.dict_of_cache_prod_idxs[cache_fp].add(i)
        for k, v in self.dict_of_cache_prod_idxs.items():
            self.dict_of_cache_prod_idxs[k] = frozenset(v)
        self.dict_of_cache_prod_idxs = MappingProxyType(self.dict_of_cache_prod_idxs)

        # *************************************************************************************** **
    def __hash__(self):
        return id(self)

    def __eq__(self, other):
        return self is other
