from buzzard._actors.message import Msg

class ComputationGate(object):
    """Actor that takes care of
    - Start the computation of those missing cache files.
    - Launch primitives collection.
    - Delay the computation until needed by the query.
    """

    def __init__(self, raster):
        self._raster = raster
        self._queries = {}
        self._alive = True

    @property
    def address(self):
        return '/Raster{}/ComputationGate'.format(self._raster.uid)

    @property
    def alive(self):
        return self._alive

    # ******************************************************************************************* **
    def receive_compute_those_cache_files(self, qi):
        msgs = []
        assert qi not in self._queries
        q = _Query()
        self._queries[qi] = q
        msgs += self._allow(qi, q, 0)
        return msgs

    def receive_output_queue_update(self, qi, produced_count, queue_size):

        msgs = []
        assert qi in self._queries
        q = self._queries[qi]
        if produced_count == qi.produce_count:
            assert qi.allowed_count == produced_count
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
        if qi in self._queries:
            del self._queries[qi]
        return []

    def receive_die(self):
        """Receive message: The raster was killed"""
        assert self._alive
        self._alive = False
        self._queries.clear()
        return []

    # ******************************************************************************************* **
    @staticmethod
    def _allow(qi, q, pulled_count):
        msgs = []

        max_prod_idx_allowed = pulled_count + qi.max_queue_size
        i = allowed_count
        while True:
            if i == len(qi.cache_fps):
                break
            cache_fp = qi.cache_fps[i]
            prod_idx = qi.dict_of_min_prod_idx_per_cache_fp[cache_fp]
            if prod_idx > max_prod_idx_allowed:
                break
            i += 1
            msgs += [Msg(
                'Computer', 'compute_this_array', cache_fp
            )]
            q.allowed_count = i

        return msgs

    # ******************************************************************************************* **

class _Query(object):

    def __init__(self):
        self.allowed_count = 0
