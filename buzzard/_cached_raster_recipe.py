import numpy as np

from buzzard import _tools
from buzzard._footprint import Footprint
from buzzard._a_proxy_raster import AProxyRaster, ABackProxyRaster

class CachedRasterRecipe(AProxyRaster):

    def __init__(
        self,
        ds,
        fp,
        dtype,
        band_count,
        band_schema,
        sr,
        compute_array,
        merge_array,
        cache_dir,
        queue_data_per_primitive,
        convert_footprint_per_primitive,
        computation_pool,
        merge_pool,
        io_pool,
        resample_pool,
        computation_tiles,
        cache_tiles,
        max_resampling_size
    ):
        back = BackCachedRasterRecipe(
        )
        super().__init__(ds=ds, back=back)

    # def get_data(self, fp=None, band=1, dst_nodata=None, interpolation='cv_area', **kwargs):

    def parse_queue_data_parameters(self, band=1, dst_nodata=None, interpolation='cv_area',
                                    max_queue_size=5):

        # Normalize and check band parameter
        band_ids, is_flat = _tools.normalize_band_parameter(band, len(self), self.shared_band_id)
        del band

        # Normalize and check dst_nodata parameter
        if dst_nodata is not None:
            dst_nodata = self.dtype.type(dst_nodata)
        elif self.nodata is not None:
            dst_nodata = self.nodata
        else:
            dst_nodata = self.dtype.type(0)

        # Check interpolation parameter here
        if not (interpolation is None or interpolation in self._back.REMAP_INTERPOLATIONS): # pragma: no cover
            raise ValueError('`interpolation` should be None or one of {}'.format(
                set(self._back.REMAP_INTERPOLATIONS.keys())
            ))

        # Check max_queue_size
        max_queue_size = int(max_queue_size)
        if max_queue_size <= 0:
            raise ValueError('`max_queue_size` should be >0')

        return {
            fps=fps,
            band_ids=band_ids,
            dst_nodata=dst_nodata,
            interpolation=interpolation,
            max_queue_size=max_queue_size,
            is_flat=is_flat,
        }

    def queue_data(self, fps, band=1, dst_nodata=None, interpolation='cv_area', max_queue_size=5):
        """TODO: Docstring
        """
        # Normalize and check fps parameter
        for fp in fps:
            if not isinstance(fp, Footprint):
                raise ValueError('element of `fps` parameter should be a Footprint (not {})'.format(fp)) # pragma: no cover

        return self._back.queue_data(
            fps=fps,
            parent_uid=None,
            key_in_parent=None,
            **self._parse_queue_data_parameters(fps, band, dst_nodata, interpolation, max_queue_size)
        )

class BackCachedRasterRecipe(ABackProxyRaster):

    def __init__(
        self,
        back_ds,
        fp,
        dtype,
        band_count,
        band_schema,
        sr,
        compute_array,
        merge_array,
        cache_dir,
        queue_data_per_primitive,
        convert_footprint_per_primitive,
        computation_pool,
        merge_pool,
        io_pool,
        resample_pool,
        computation_tiles,
        cache_tiles,
        max_resampling_size
    ):
        super()(
            back_ds=back_ds,
            wkt_stored=sr,
            band_schema=band_schema,
            dtype=dtype,
            fp_stored=...,
        )
        if not back_ds.started:
            back_ds.start_scheduler()
        back_ds.put_message(Msg(
            '/Global/TopLevel', 'new_raster' self
        ))

    def queue_data(
            self,
            fps=fps,
            band_ids=band_ids,
            dst_nodata=dst_nodata,
            interpolation=interpolation,
            max_queue_size=max_queue_size,
            is_flat=is_flat,
            parent_uid=None,
            key_in_parent=None,



    def ext_receive_new_query(self, queue_wref, max_queue_size, produce_fps,
                              band_ids, is_flat, dst_nodata, interpolation,
                              max_queue_size,
                              parent_uid,
                              key_in_parent):



                   parent_uid, key_in_parent):
                   # self.primitive_fps_per_primitive[name],
                # *raster.primitives_args[name],
                # parent_uid=raster.uid,
                # key_in_parent=(qi, name),
                # **raster.primitives_kwargs[name],

    ):
