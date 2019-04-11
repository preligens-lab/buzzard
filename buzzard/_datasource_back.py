
from buzzard._dataset_back_conversions import BackDatasetConversionsMixin
from buzzard._dataset_back_activation_pool import BackDatasetActivationPoolMixin
from buzzard._dataset_back_scheduler import BackDatasetSchedulerMixin
from buzzard._dataset_pools_container import PoolsContainer

class BackDataset(BackDatasetConversionsMixin,
                     BackDatasetActivationPoolMixin,
                     BackDatasetSchedulerMixin):
    """Backend of the Dataset, referenced by backend proxies
    Implements activation (pooling) and conversion methods"""

    def __init__(self, allow_none_geometry, allow_interpolation, **kwargs):
        self.allow_interpolation = allow_interpolation
        self.allow_none_geometry = allow_none_geometry
        self.pools_container = PoolsContainer()
        super(BackDataset, self).__init__(**kwargs)
