import uuid
import queue
import weakref

from buzzard._a_proxy_raster import AProxyRaster, ABackProxyRaster
from buzzard._footprint import Footprint
from buzzard import _tools
from buzzard._actors.message import Msg

class AScheduledRaster(AProxyRaster):
    """TODO: docstring"""

    def queue_data(self, fps, band=1, dst_nodata=None, interpolation='cv_area', max_queue_size=5):
        """TODO: Docstring
        """
        for fp in fps:
            if not isinstance(fp, Footprint):
                raise ValueError('element of `fps` parameter should be a Footprint (not {})'.format(fp)) # pragma: no cover

        return self._back.queue_data(
            fps=fps,
            parent_uid=None,
            key_in_parent=None,
            **_tools.parse_queue_data_parameters(self, band, dst_nodata, interpolation, max_queue_size)
        )

    def iter_data(self, fps, band=1, dst_nodata=None, interpolation='cv_area', max_queue_size=5):
        """TODO: Docstring
        """
        q = self.queue_data(fps, band, dst_nodata, interpolation, max_queue_size)
        def _iter_data_generator():
            i = 0
            n = len(fps)
            while True:
                try:
                    while i < len(fps):
                        arr = q.get(True, 1 / 10)
                        yield arr
                        i += 1
                    return
                except queue.Empty:
                    timeout = True
                else:
                    timeout = False
                if timeout:
                    self._back.back_ds.ensure_scheduler_still_alive()
        return _iter_data_generator()

class ABackScheduledRaster(ABackProxyRaster):
    """TODO: docstring"""

    def __init__(self, resample_pool, max_resampling_size, **kwargs):
        self.uid = uuid.uuid4()
        self.resample_pool = resample_pool
        self.max_resampling_size = max_resampling_size
        super().__init__(**kwargs)

    def queue_data(self, fps, band_ids, dst_nodata, interpolation, max_queue_size, is_flat,
                   parent_uid, key_in_parent):
        q = queue.Queue(max_queue_size)
        self.back_ds.put_message(Msg(
            '/Raster{}/QueriesHandler'.format(self.uid),
            'new_query',
            weakref.ref(q),
            max_queue_size,
            fps,
            band_ids,
            is_flat,
            dst_nodata,
            interpolation,
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

    def create_actors(self):
        raise NotImplementedError('ABackScheduledRaster.create_actors is virtual pure')
