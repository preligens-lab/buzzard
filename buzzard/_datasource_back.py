from osgeo import osr

from buzzard._datasource_back_conversions import *
from buzzard._datasource_back_register import *

class BackDataSource(BackDataSourceConversionsMixin, BackDataSourceRegisterMixin):

    def __init__(self, wkt_work, wkt_fallback, wkt_forced,
                 analyse_transformation,
                 allow_none_geometry,
                 allow_interpolation,
                 max_activated):
        self.allow_interpolation = allow_interpolation
        self.allow_none_geometry = allow_none_geometry

        super(BackDataSource, self).__init__(
            wkt_work=wkt_work,
            wkt_fallback=wkt_fallback,
            wkt_forced=wkt_forced,
            analyse_transformation=analyse_transformation,
        )
