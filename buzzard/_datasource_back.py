from buzzard._datasource_back_conversions import BackDataSourceConversionsMixin
from buzzard._datasource_back_activation_pool import BackDataSourceActivationPoolMixin
from buzzard._datasource_back_scheduler import BackDataSourceSchedulerMixin

class BackDataSource(BackDataSourceConversionsMixin,
                     BackDataSourceActivationPoolMixin,
                     BackDataSourceSchedulerMixin):
    """Backend of the DataSource, referenced by backend proxies
    Implements activation (pooling) and conversion methods"""

    def __init__(self, allow_none_geometry, allow_interpolation, **kwargs):
        self.allow_interpolation = allow_interpolation
        self.allow_none_geometry = allow_none_geometry

        super(BackDataSource, self).__init__(**kwargs)
