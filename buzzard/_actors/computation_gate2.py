import numpy as np

from buzzard._actors.message import Msg

class ActorComputationGate2(object):
    """Actor that takes care of delaying the computation of a cache file until inputs are ready."""

    def __init__(self, raster):
        self._raster = raster
        self._queries = {}
        self._alive = True

    @property
    def address(self):
        return '/Raster{}/ComputationGate2'.format(self._raster.uid)

    @property
    def alive(self):
        return self._alive

    # ******************************************************************************************* **
    def receive_compute_this_array(self, qi, compute_idx):
        """Receive message: Wait for the inputs of this computation to be ready

        Parameters
        ----------
        qi: _actors.cached.query_infos.QueryInfos
        compute_idx: int
        """
        msgs = []

        qicc = qi.cache_computation
        assert qicc is not None

        if qi in self._queries:
            q = self._queries[qi]
            assert q.max_compute_idx_allowed + 1 == compute_idx
            q.max_compute_idx_allowed = compute_idx
        else:
            q = _Query()
            self._queries[qi] = q

        msgs += self._allow(qi, q)
        return msgs

    def receive_input_queue_update(self, queue_key):
        """Receive message: One of the input queues of a query changed in size.

        Parameters
        ----------
        queue_key: (_actors.cached.query_infos.QueryInfos, str)
        """
        msgs = []

        qi, prim_name = queue_key
        if qi in self._queries:
            q = self._queries[qi]
            msgs += self._allow(qi, q)

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
    def _allow(qi, q):
        msgs = []
        qicc = qi.cache_computation

        if len(qicc.primitive_queue_per_primitive) == 0:
            queues_min_qsize = min(qicc.primitive_queue_per_primitive.values(), key=lambda v: v.qsize())
            max_compute_idx_ready = qicc.pulled_count + queues_min_qsize - 1
            assert q.max_compute_idx_allowed <= max_compute_idx_ready, 'allowed more than ready count'
        else:
            max_compute_idx_ready = np.inf

        i = q.allowed_count
        while i <= max_compute_idx_ready and i <= q.max_compute_idx_allowed:
            msgs += [Msg(
                'Computer', 'compute_this_array', qi, i,
            )]
            i += 1
        q.allowed_count = i

        return msgs

    # ******************************************************************************************* **

class _Query(object):

    def __init__(self):
        self.allowed_count = 0
        self.max_compute_idx_allowed = 0
