from typing import Set, Dict, List, Sequence, Union, cast
import sys
import collections

import numpy as np
from buzzard._footprint import Footprint

# Declare supertypes for attributes typing
class ProdFootprint(Footprint): pass
class CacheFootprint(Footprint): pass
class SampleFootprint(Footprint): pass
class ResampleFootprint(Footprint): pass

class QueryInfos(object):
    """Immutable object that stores many informations about a query
    An instance of this class identifies a query among the actors, hence the
    `__hash__` implementation.

    Classes' attributes are typed for documentation purposes and for validation with `mypy`
    """

    def __init__(self, raster, list_of_prod_fp,
                 band_ids, dst_nodata, interpolation,
                 max_queue_size):

        # The parameters given by user in invocation
        self.band_ids = band_ids # type: Sequence[int]
        self.dst_nodata = dst_nodata # type: Sequence[Union[int, float]]
        self.interpolation = interpolation # type: str

        # Output max queue size
        self.max_queue_size = max_queue_size # type: int

        # How many arrays are requested
        self.produce_count = len(list_of_prod_fp) # type: int

        # The list of Footprints requested
        self.list_of_prod_fp = list_of_prod_fp # type: List[ProdFootprint]

        # Boolean attribute of each `prod_fp`
        # If `True` the resampling phase has to be performed on a Pool
        self.list_of_prod_same_grid = [
            fp.same_grid(raster.fp)
            for fp in list_of_prod_fp
        ] # type: List[bool]

        # Boolean attribute of each `prod_fp`
        # If `False` the queried footprint is outside of raster's footprint. It means that no
        # sampling is necessary and the outputed array will be full of `dst_nodata`
        self.list_of_prod_share_area = [
            fp.share_area(raster.fp)
            for fp in list_of_prod_fp
        ] # type: List[bool]

        # The full Footprint that needs to be sampled for each `prod_fp`
        # Is `None` if `prod_fp` is fully outside of raster
        self.list_of_prod_sample_fp = [] # type: List[Union[None, SampleFootprint]]

        # The set of cache Footprints that are needed for each `prod_fp`
        self.list_of_prod_cache_fps = [] # type: List[Set[CacheFootprint]]

        # The list of resamplings to perform for each `prod_fp`
        # Always at least 1 resampling per `prod_fp`
        self.list_of_prod_resample_fps = [] # type: List[List[ResampleFootprint]]

        # The set of `cache_fp` necessary per `resample_fp` for each `prod_fp`
        self.list_of_prod_resample_cache_deps_fps = [] # type: List[Dict[ResampleFootprint, Set[CacheFootprint]]]

        # The full Footprint that needs to be sampled par `resample_fp` for each `prod_fp`
        # Might be `None`
        self.list_of_prod_resample_sample_dep_fp = [] # type: List[Dict[ResampleFootprint, Union[None, SampleFootprint]]]

        it = zip(self.list_of_prod_fp, self.list_of_prod_sample_fp, self.list_of_prod_share_area)
        for prod_fp, same_grid, share_area in it:
            if not share_area:
                # Resampling will be performed in one pass, on the scheduler
                self.list_of_prod_sample_fp.append(None)
                self.list_of_prod_cache_fps.append(set())
                resample_fp = cast(ResampleFootprint, prod_fp)
                self.list_of_prod_resample_fps.append([resample_fp])
                self.list_of_prod_resample_cache_deps_fps.append({resample_fp: set()})
                self.list_of_prod_resample_sample_dep_fp.append({resample_fp: None})
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

                self.list_of_prod_sample_fp.append(sample_fp)
                self.list_of_prod_cache_fps.append(
                    set(raster.cache_fps_of_fp(sample_fp))
                )
                self.list_of_prod_resample_fps.append(resample_fps)
                self.list_of_prod_resample_cache_deps_fps.append({
                    resample_fp: set(raster.cache_fps_of_fp(resample_fp))
                    for resample_fp in resample_fps
                })
                self.list_of_prod_resample_sample_dep_fp.append(sample_dep_fp)

        # The list of all cache Footprints needed, ordered by priority
        self.list_of_cache_fp = [] # type: List[CacheFootprint]
        seen = set()
        for fps in self.list_of_prod_cache_fps:
            for fp in fps:
                if fp not in seen:
                    seen.add(fp)
                    self.list_of_cache_fp.append(fp)
        del seen

        # The dict of cache Footprint to set of production idxs
        self.dict_of_cache_prod_idxs = collections.defaultdict(set) # type: Dict[CacheFootprint, Set[int]]
        for i, (prod_fp, cache_fps) in enumerate(zip(self.list_of_prod_fp, self.list_of_prod_cache_fps)):
            for cache_fp in cache_fps:
                self.dict_of_cache_prod_idxs[cache_fp].add(i)

    def __hash__(self):
        return id(self)

    def __eq__(self, other):
        return self is other
