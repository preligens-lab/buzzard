from osgeo import osr

from buzzard._datasource_back_conversions import *
from buzzard._datasource_back_register import *
from buzzard._datasource_back_activation_pool import *

class BackDataSource(BackDataSourceConversionsMixin, BackDataSourceRegisterMixin,
                     BackDataSourceActivationPoolMixin):

    def __init__(self, allow_none_geometry, allow_interpolation, **kwargs):
        self.allow_interpolation = allow_interpolation
        self.allow_none_geometry = allow_none_geometry

        super(BackDataSource, self).__init__(**kwargs)
