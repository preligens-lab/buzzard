from buzzard._actors.message import Msg

class ActorProductionGate(object):
    """Actor that takes care of delaying the production of arrays until needed"""

    def __init__(self, raster):
        self._raster = raster
        self._queries = {}
        self._alive = True
        self.address = '/Raster{}/ProductionGate'.format(self._raster.uid)

    @property
    def alive(self):
        return self._alive

    # ******************************************************************************************* **
    def receive_make_those_arrays(self, qi):
        """Receive message: New query.

        Parameters
        ----------
        qi: _actors.cached.query_infos.QueryInfos
        """
        msgs = []
        assert qi not in self._queries

        q = _Query()
        self._queries[qi] = q
        msgs += self._allow(qi, q, 0)
        return msgs

    def receive_output_queue_update(self, qi, produced_count, queue_size):
        """Receive message: The output queue of a query changed in size.

        If necessary, allow some production array contruction to start

        Parameters
        ----------
        qi: _actors.cached.query_infos.QueryInfos
        produced_count: int
            How many arrays were pushed in the output queue
        queue_size: int
            How many arrays are currently available in the output queue
        """
        msgs = []

        q = self._queries[qi]
        if produced_count == qi.produce_count:
            assert q.allowed_count == produced_count
            del self._queries[qi]
        else:
            pulled_count = produced_count - queue_size
            msgs += self._allow(qi, q, pulled_count)

        return msgs

    def receive_cancel_this_query(self, qi):
        """Receive message: One query was dropped

        Parameters
        ----------
        qi: _actors.cached.query_infos.QueryInfos
        """
        del self._queries[qi]
        return []

    def receive_die(self):
        """Receive message: The raster was killed"""
        assert self._alive
        self._alive = False

        self._queries.clear()
        self._raster = None
        return []

    # ******************************************************************************************* **
    @staticmethod
    def _allow(qi, q, pulled_count):
        msgs = []

        while True:
            if q.allowed_count == qi.produce_count:
                # All productions started
                break
            if q.allowed_count == pulled_count + qi.max_queue_size:
                # Enough production started yet
                break
            msgs += [Msg(
                'Producer', 'make_this_array', qi, q.allowed_count
            )]
            q.allowed_count += 1

        return msgs


    # ******************************************************************************************* **

class _Query(object):

    def __init__(self):
        self.allowed_count = 0
