
class ActorBuilderBedroom(object):
    """Actor that takes care of delaying messages from Producer to Builder"""

    def __init__(self, raster):
        self._raster = raster
        self._queries = {}

    @property
    def address(self):
        return '/Raster{}/BuilderBedroom'.format(self._raster.uid)

    # ******************************************************************************************* **
    def receive_build_those_arrays_when_needed_soon(self, query_key, query_infos):
        """Receive message: Take care of that new query

        Wake up the ones that fit in the output queue
        """
        msgs = []

        q = _Query(
            query_infos
        )
        self._queries[query_key] = q

        msgs += self._wake(q)
        if q.woke_count == q.infos.produce_count:
            del self._queries[query_key]
        return msgs

    def receive_output_queue_update(self, query_key, produced_count, _):
        """Receive message: An update occured on the output queue

        Wake up production if `produced_count` changed
        """
        msgs = []

        if query_key in self._queries:
            q = self._queries[query_key]
            if produced_count != q.produced_count:
                assert produced_count > q.produced_count
                q.produced_count = produced_count

                msgs += self._wake(q)
                if q.woke_count == q.infos.produce_count:
                    del self._queries[query_key]
        return msgs

    def receive_kill_this_query(self, query_key):
        del self._queries[query_key]

    def receive_die(self):
        self._queries.clear()

    # ******************************************************************************************* **
    def _wake(self, q):
        msgs = []
        max_woke_count = q.produced_count + q.infos.max_queue_size
        while q.woke_count < q.infos.produce_count and q.woke_count < max_woke_count:
            msgs += [Msg(
                'Builder', 'build_this_array', query_key, q.infos, q.woke_count,
            )]
            q.woke_count += 1
        return msgs

    # ******************************************************************************************* **

class _Query(object):

    def __init__(self, infos):
        self.infos = infos
        self.woke_count = 0
        self.produced_count = 0
