import queue
import weakref

import numpy as np

from buzzard import _tools
from buzzard._footprint import Footprint
from buzzard._a_proxy_raster import AProxyRaster, ABackProxyRaster
from buzzard._a_scheduled_raster import ABackScheduledRaster, ASchedulerRaster

class CachedRasterRecipe(AProxyRaster, ASchedulerRaster):

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
            **tools.parse_queue_data_parameters(self, band, dst_nodata, interpolation, max_queue_size)
        )

    def iter_data(self, fps, band=1, dst_nodata=None, interpolation='cv_area', max_queue_size=5):
        """TODO: Docstring
        """
        q = self.queue_data(fps, band, dst_nodata, interpolation, max_queue_size)
        def _iter_data_generator():
            for _ in fps:
                yield q.get()
        return _iter_data_generator

class BackCachedRasterRecipe(ABackProxyRaster, ABackSchedulerRaster):

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
            False, # `is_flat` is not importent since caller reshapes
            None, None,
        )
        return q.get()
