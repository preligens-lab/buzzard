import uuid
import queue
import weakref

from buzzard._a_source_raster import ASourceRaster, ABackSourceRaster
from buzzard._footprint import Footprint
from buzzard import _tools
from buzzard._actors.message import Msg
from buzzard._debug_observers_manager import DebugObserversManager

QUEUE_POLL_DISTANCE = 0.1

class AAsyncRaster(ASourceRaster):
    """Base abstract class defining the common behavior of all rasters that are managed by the
    Dataset's scheduler.

    Features Defined
    ----------------
    - Has a `queue_data`, a low level method that can be used to query several arrays at once.
    - Has an `iter_data`, a higher level wrapper of `queue_data`.
    """

    def queue_data(self, fps, channels=None, dst_nodata=None, interpolation='cv_area',
                   max_queue_size=5, **kwargs):
        """Read several rectangles of data on several channels from the source raster.

        Using `queue_data` instead of multiple calls to `get_data` allows more parallelism.
        The `fps` parameter should contain a sequence of `Footprint` that will be mapped to
        `numpy.ndarray`. The first one will be computed with a higher priority than the later one.

        Calling this method sends an asynchronous message to the Dataset's scheduler with the
        input parameters and a queue. On the input side of the queue, the scheduler will call the
        `put` method with each array requested. On the output side of the queue, the `get` method
        should be called to retrieve the requested arrays.

        The output queue will be created with a max queue size of `max_queue_size`, the scheduler
        will be careful to prepare only the arrays that can fit in the output queue. Thanks to this
        feature: backpressure can be entirely avoided.

        If you wish to cancel your request, loose the reference to the queue and the scheduler will
        gracefuly cancel the query.

        In general you should use the `iter_data` method instead of the `queue_data` one, it is much
        safer to use. However you will need to pass the `queue_data` method of a raster, to create
        another raster (a recipe) that depends on the first raster.

        see rasters' `get_data` documentation, it shares most of the concepts

        Parameters
        ----------
        fps: sequence of Footprint
            The Footprints at which the raster should be sampled.
        channels:
            see `get_data` method
        dst_nodata:
            see `get_data` method
        interpolation:
            see `get_data` method
        max_queue_size: int
            Maximum number of arrays to prepare in advance in the underlying queue.

        Returns
        -------
        queue.Queue of ndarray
        The arrays are put into the queue in the same order as in the `fps` parameter.

        """
        for fp in fps:
            if not isinstance(fp, Footprint):
                msg = 'element of `fps` parameter should be a Footprint (not {})'.format(fp) # pragma: no cover
                raise ValueError(msg)

        return self._back.queue_data(
            fps=fps,
            parent_uid=None,
            key_in_parent=None,
            **_tools.parse_queue_data_parameters(
                'queue_data', self, channels, dst_nodata, interpolation, max_queue_size, **kwargs
            )
        )

    def iter_data(self, fps, channels=None, dst_nodata=None, interpolation='cv_area',
                  max_queue_size=5, **kwargs):
        """Read several rectangles of data on several channels from the source raster.

        The `iter_data` method is a higher level wrapper around the `queue_data` method. It
        returns a python generator and while waiting for data, it periodically probes the
        Dataset's scheduler to reraise an exception if it crashed.

        If you wish to cancel your request, loose the reference to the iterable and the scheduler
        will gracefully cancel the query.

        see rasters' `get_data` documentation, it shares most of the concepts
        see `queue_data` documentation, it is called from within the `iter_data` method

        Parameters
        ----------
        fps: sequence of Footprint
            The Footprints at which the raster should be sampled.
        channels:
            see `get_data` method
        dst_nodata:
            see `get_data` method
        interpolation:
            see `get_data` method
        max_queue_size: int
            Maximum number of arrays to prepare in advance in the underlying queue.

        Returns
        -------
        generator of ndarray
        The arrays are yielded into the generator in the same order as in the `fps` parameter.

        """
        for fp in fps:
            if not isinstance(fp, Footprint):
                raise ValueError('element of `fps` parameter should be a Footprint (not {})'.format(
                    fp
                )) # pragma: no cover

        return self._back.iter_data(
            fps=fps,
            **_tools.parse_queue_data_parameters(
                'iter_data', self, channels, dst_nodata, interpolation, max_queue_size, **kwargs
            )
        )

class ABackAsyncRaster(ABackSourceRaster):
    """Implementation of AAsyncRaster's specifications"""

    def __init__(self, resample_pool, max_resampling_size, debug_observers, **kwargs):
        self.uid = uuid.uuid4()
        self.resample_pool = resample_pool
        self.max_resampling_size = max_resampling_size
        self.debug_mngr = DebugObserversManager(debug_observers)

        # Quick hack to share the dict of path to cache files with the ActorCacheSupervisor
        # This is currently needed to perform the `.close` operation
        # This is a clear violation of the separation of concerns
        self.async_dict_path_of_cache_fp = {}

        super().__init__(**kwargs)

    def queue_data(self, fps, channel_ids, dst_nodata, interpolation, max_queue_size, is_flat,
                   parent_uid, key_in_parent):
        q = queue.Queue(max_queue_size)
        self.back_ds.put_message(Msg(
            '/Raster{}/QueriesHandler'.format(self.uid),
            'new_query',
            weakref.ref(q),
            max_queue_size,
            fps,
            channel_ids,
            is_flat,
            dst_nodata,
            interpolation,
            parent_uid,
            key_in_parent
        ))
        return q

    def iter_data(self, fps, channel_ids, dst_nodata, interpolation, max_queue_size, is_flat):
        q = self.queue_data(fps, channel_ids, dst_nodata, interpolation, max_queue_size, is_flat,
                            None, None)
        def _iter_data_generator():
            i = 0
            while True:
                try:
                    while i < len(fps):
                        arr = q.get(True, timeout=QUEUE_POLL_DISTANCE)
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

    def get_data(self, fp, channel_ids, dst_nodata, interpolation):
        it = self.iter_data(
            [fp], channel_ids, dst_nodata, interpolation, 1,
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
