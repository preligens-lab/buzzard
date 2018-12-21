from buzzard._actors.message import Msg

class ActorComputationGate1(object):
    """Actor that takes care of delaying the computation of a cache file until needed soon by
    the query. It receives resquests to compute `cache footprints`, it outputs requests to compute
    `compute footprints`.
    """

    def __init__(self, raster):
        self._raster = raster
        self._queries = {}
        self._alive = True
        self.address = '/Raster{}/ComputationGate1'.format(self._raster.uid)

    @property
    def alive(self):
        return self._alive

    # ******************************************************************************************* **
    def receive_compute_those_cache_files(self, qi):
        """Receive message: The collection started for the computation of the missing cache files,
        allow computation as soon as the cache file is needed soon by query

        Parameters
        ----------
        qi: _actors.cached.query_infos.QueryInfos
        """
        msgs = []

        qicc = qi.cache_computation
        assert qicc is not None

        if qi in self._queries:
            # `receive_output_queue_update` happened before this call
            q = self._queries[qi]
        else:
            q = _Query()
            self._queries[qi] = q
        msgs += self._allow(qi, q)
        return msgs

    def receive_output_queue_update(self, qi, produced_count, queue_size):
        """Receive message: The output queue of a query changed in size.

        Parameters
        ----------
        qi: _actors.cached.query_infos.QueryInfos
        produced_count: int
            How many arrays were pushed in the output queue
        queue_size: int
            How many arrays are currently available in the output queue
        """
        msgs = []

        pulled_count = produced_count - queue_size
        qicc = qi.cache_computation

        if qi in self._queries:
            assert pulled_count >= self._queries[qi].pulled_count, (
                'new pulled_count:{} '.format(pulled_count) +
                'previous pulled_count:{}'.format(self._queries[qi].pulled_count)
            )
            assert produced_count >= self._queries[qi].produced_count, (
                'new produced_count:{} '.format(produced_count) +
                'previous produced_count:{}'.format(self._queries[qi].produced_count)
            )

        if produced_count == qi.produce_count:
            # Query finished
            if qi in self._queries:
                # Either qicc is None (no computation needed)
                #  or allowed count == produced_count (all computations allowed here)
                #  or allowed count <  produced_count (some other query computed the arrays)
                del self._queries[qi]
        else:
            if qi in self._queries:
                q = self._queries[qi]
            else:
                q = _Query()
                self._queries[qi] = q
            q.pulled_count = pulled_count
            if qicc is not None:
                msgs += self._allow(qi, q)

            q.produced_count = produced_count

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

        max_prod_idx_allowed = q.pulled_count + qi.max_queue_size - 1
        i = q.allowed_count
        while True:
            # list_of_compute_fp being sorted by priority, `min_prod_idx` is increasing between loops

            if i == len(qicc.list_of_compute_fp):
                break
            compute_fp = qicc.list_of_compute_fp[i]
            min_prod_idx = qicc.dict_of_min_prod_idx_per_compute_fp[compute_fp]
            if min_prod_idx > max_prod_idx_allowed:
                break
            msgs += [Msg(
                'ComputationGate2', 'compute_this_array', qi, i,
            )]
            i += 1
        q.allowed_count = i

        return msgs

    # ******************************************************************************************* **

class _Query(object):
    def __init__(self):
        self.pulled_count = 0
        self.allowed_count = 0
        self.produced_count = 0
