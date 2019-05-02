import numpy as np

from buzzard._actors.message import Msg

class ActorComputationGate2(object):
    """Actor that takes care of delaying the computation of a cache file until inputs are ready."""

    def __init__(self, raster):
        self._raster = raster
        self._queries = {}
        self._alive = True
        self.address = '/Raster{}/ComputationGate2'.format(self._raster.uid)

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
            assert q.allowed_up_count == compute_idx, ''
        else:
            assert compute_idx == 0, 'This is the first call for qi, compute_idx it should be 0'
            q = _Query()
            self._queries[qi] = q
        q.allowed_up_count = compute_idx + 1

        msgs += self._allow(qi, q)
        return msgs

    def receive_input_queue_update(self, queue_key):
        """Receive message: One of the input queues of a query changed in size.

        Parameters
        ----------
        queue_key: (_actors.cached.query_infos.QueryInfos, str)
        """
        msgs = []

        qi, _ = queue_key
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
        self._raster = None
        return []

    # ******************************************************************************************* **
    @staticmethod
    def _allow(qi, q):
        msgs = []
        qicc = qi.cache_computation

        if len(qicc.primitive_queue_per_primitive) > 0:
            qsizes = [v.qsize() for v in qicc.primitive_queue_per_primitive.values()]
            min_qsize = min(qsizes)
            allowed_gate2_count = qicc.collected_count + min_qsize
            assert q.allowed_down_count <= allowed_gate2_count, (
                'allowed more than ready count'
            )
        else:
            allowed_gate2_count = np.inf

        i = q.allowed_down_count
        while i < allowed_gate2_count and i < q.allowed_up_count:
            msgs += [Msg(
                'Computer', 'compute_this_array', qi, i,
            )]
            i += 1
        q.allowed_down_count = i

        return msgs

    # ******************************************************************************************* **

class _Query(object):
    def __init__(self):
        self.allowed_up_count = 0 # How many compute allowed by `ComputationGate1`
        self.allowed_down_count = 0 # How many allowed to `Computer`
