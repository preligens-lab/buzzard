
class QueryInfos(object):
    """Immutable object that stores many informations on a query"""

    def __init__(self, raster, list_of_prod_fp, band_ids, dst_nodata, interpolation,
                 max_queue_size):
        self.band_ids = band_ids
        self.dst_nodata = dst_nodata
        self.interpolation = interpolation
        self.max_queue_size = max_queue_size

        self.produce_count = len(list_of_prod_fp)

        # The list of Footprints required by user
        self.list_of_prod_fp = list_of_prod_fp

        # Boolean attribute of each `prod_fp`
        self.list_of_prod_same_grid = [
            fp.same_grid(raster.fp)
            for fp in list_of_prod_fp
        ]

        # Boolean attribute of each `prod_fp`
        self.list_of_prod_share_area = [
            fp.share_area(raster.fp)
            for fp in list_of_prod_fp
        ]

        # The full Footprint that needs to be sampled for each `prod_fp`
        self.list_of_prod_sample_fp = []

        # The set of cache Footprints that are needed for each `prod_fp`
        self.list_of_prod_cache_fps = []

        # The list of resamplings to perform for each `prod_fp`
        self.list_of_prod_resample_fps = []

        it = zip(self.list_of_prod_fp, self.list_of_prod_sample_fp, self.list_of_prod_share_area)
        for prod_fp, same_grid, share_area in zip(it):
            if not share_area:
                self.list_of_prod_sample_fp.append(None)
                self.list_of_prod_cache_fps.append(set())
                self.list_of_prod_resample_fps.append([prod_fp])
            else:
                if same_grid:
                    sample_fp = raster.fp & prod_fp
                    resample_fps = [prod_fp]
                else:
                    sample_fp = raster.build_sampling_footprint_to_remap(prod_fp, interpolation)

                    if raster.max_resampling_size is None:
                        resample_fps = [prod_fp]
                    else:
                        rsize = np.maximum(produce_fp.rsize, sample_fp.rsize)
                        countx, county = np.ceil(rsize / self._raster.max_resampling_size).astype(int)
                        resample_fps = sample_fp.tile_count(
                            (countx, county), boundary_effect='shrink'
                        ).flatten().tolist()

                self.list_of_prod_sample_fp.append(sample_fp)
                self.list_of_prod_cache_fps.append(
                    set(raster.cache_fps_of_fp(sample_fp))
                )
                self.list_of_prod_resample_fps.append(resample_fps)

        # The list of all cache Footprints needed, ordered by priority
        self.list_of_cache_fp = []
        seen = set()
        for fps in cache_fps_of_sample_fp:
            for fp in fps:
                if fp not in seen:
                    seen.add(fp)
                    self.list_of_cache_fp.append(fp)

        # The dict of cache Footprint to set of production ids
        self.dict_of_cache_prod_ids = collections.default_dict(set)
        for i, (prod_fp, cache_fps) in enumerate(zip(self.list_of_prod_fp, self.list_of_prod_cache_fps)):
            for cache_fp in cache_fps:
                self.dict_of_cache_prod_ids[cache_fp].add(i)
