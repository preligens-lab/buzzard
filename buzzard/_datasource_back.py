
from osgeo import osr


class BackDataSource():


    def __init__(self, wkt_work, wkt_fallback, wkt_forced,
                 analyse_transformation,
                 allow_none_geometry,
                 allow_interpolation,
                 max_activated):


        # DataSourceConversionsMixin.__init__(
        #     self, sr_work, sr_fallback, sr_forced, analyse_transformation
        # )
        # _datasource_tools.DataSourceToolsMixin.__init__(self, max_activated)

        self.wkt_work = wkt_work
        self.wkt_fallback = wkt_fallback
        self.wkt_forced = wkt_forced
        self.allow_interpolation = allow_interpolation
        self.allow_none_geometry = allow_none_geometry
        self.assert_no_change_on_activation = assert_no_change_on_activation
        self.thread_pool_task_counter = collections.defaultdict(int)
