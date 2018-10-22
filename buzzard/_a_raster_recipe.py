from buzzard._a_scheduled_raster import ABackScheduledRaster, AScheduledRaster

class ARasterRecipe(AScheduledRaster):
    """TODO: docstring"""

    @property
    def primitives(self):
        # TODO: differenciate _datasource_register and _datasource_back_register for this job
        # TODO: Make sure primitives come from the same datasource
        d = {}
        for key, back in self._back.primitives_back.items():
            for facade in self._ds._keys_of_proxy.keys():
                if facade._back is back:
                    d[key] = facade
                    break
            else:
                assert False
        return d

class ABackRasterRecipe(ABackScheduledRaster):
    """TODO: docstring"""
    def __init__(self,
                 band_schema,
                 band_count,
                 facade_proxy,
                 computation_pool,
                 merge_pool,
                 compute_array,
                 merge_arrays,
                 primitives_back,
                 primitives_kwargs,
                 convert_footprint_per_primitive,
                 **kwargs):
        self.facade_proxy = facade_proxy
        self.computation_pool = computation_pool
        self.merge_pool = merge_pool
        self.compute_array = compute_array
        self.merge_arrays = merge_arrays
        self.primitives_back = primitives_back
        self.primitives_kwargs = primitives_kwargs
        self.convert_footprint_per_primitive = convert_footprint_per_primitive

        if 'nodata' not in band_schema:
            band_schema['nodata'] = [None] * band_count

        if 'interpretation' not in band_schema:
            band_schema['interpretation'] = ['undefined'] * band_count

        if 'offset' not in band_schema:
            band_schema['offset'] = [0.] * band_count

        if 'scale' not in band_schema:
            band_schema['scale'] = [1.] * band_count

        if 'mask' not in band_schema:
            band_schema['mask'] = ['all_valid']

        super().__init__(band_schema=band_schema, **kwargs)
