from buzzard._actors.message import Msg

class ActorResampler(object):
    """Actor that takes care of """

    def __init__(self, raster):
        self._raster = raster
        self._alive = True

    @property
    def address(self):
        return '/Raster{}/Resampler'.format(self._raster.uid)

    @property
    def alive(self):
        return self._alive

    # ******************************************************************************************* **
    def receive_(self):
        msgs = []

        return msgs

    def receive_cancel_this_query(self, qi):
        """Receive message: One query was dropped

        Parameters
        ----------
        qi: _actors.cached.query_infos.QueryInfos
        """

        return []

    def receive_die(self):
        """Receive message: The raster was killed"""
        assert self._alive
        self._alive = False

        return []

    # ******************************************************************************************* **
