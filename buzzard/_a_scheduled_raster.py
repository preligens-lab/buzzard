import uuid

class AScheduledRaster(object):
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

class ABackScheduledRaster(object):
    """TODO: docstring"""

    def __init__(self, resample_pool, max_resampling_size, **kwargs):
        self.uid = uuid.uuid4()
        self.resample_pool = resample_pool
        self.max_resampling_size = max_resampling_size
        super().__init__(**kwargs)

    def create_actors(self):
        raise NotImplementedError('ABackScheduledRaster.create_actors is virtual pure')

    def queue_data(self):
        raise NotImplementedError('ABackScheduledRaster.queue_data is virtual pure')
