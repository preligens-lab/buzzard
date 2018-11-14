import uuid
import queue
import weakref

from buzzard._a_proxy_raster import AProxyRaster, ABackProxyRaster
from buzzard._footprint import Footprint
from buzzard import _tools
from buzzard._actors.message import Msg
from buzzard._debug_observers_manager import DebugObserversManager

class AAsyncRaster(AProxyRaster):
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
        for fp in fps:
            if not isinstance(fp, Footprint):
                raise ValueError('element of `fps` parameter should be a Footprint (not {})'.format(fp)) # pragma: no cover

        return self._back.iter_data(
            fps=fps,
            **_tools.parse_queue_data_parameters(self, band, dst_nodata, interpolation, max_queue_size)
        )

class ABackAsyncRaster(ABackProxyRaster):
    """Implementation of AAsyncRaster's specifications"""

    def __init__(self, resample_pool, max_resampling_size, debug_observers, **kwargs):
        self.uid = uuid.uuid4()
        self.resample_pool = resample_pool
        self.max_resampling_size = max_resampling_size
        self.debug_mngr = DebugObserversManager(debug_observers)

        # Quick hack to share the dict of path to cache files with the ActorCacheSupervisor
        # This is useful to perform .close
        # This is clearly a violation of the separation of concerns
        self.async_dict_path_of_cache_fp = {}

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

    def iter_data(self, fps, band_ids, dst_nodata, interpolation, max_queue_size, is_flat):
        q = self.queue_data(fps, band_ids, dst_nodata, interpolation, max_queue_size, is_flat,
                            None, None)
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
                    self.back_ds.ensure_scheduler_still_alive()
        return _iter_data_generator()

    def get_data(self, fp, band_ids, dst_nodata, interpolation):
        it = self.iter_data(
            [fp], band_ids, dst_nodata, interpolation, 1,
            False, # `is_flat` is not important since caller reshapes output
        )
        return next(it)

    def create_actors(self): # pragma: no cover
        raise NotImplementedError('ABackAsyncRaster.create_actors is virtual pure')

    def close(self):
        """Virtual method:
        - May be overriden
        - Should always be called

        Should be called after scheduler's end
        """
        self.back_ds.put_message(Msg(
            '/Global/TopLevel', 'kill_raster', self,
        ), check_scheduler_status=False)
        # TODO: just sending a kill_raster message may not be enough. Need synchro?
        self.back_ds.deactivate_many(self.async_dict_path_of_cache_fp.values())
        super().close()
