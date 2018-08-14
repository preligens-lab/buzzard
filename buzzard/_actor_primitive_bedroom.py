
class ActorPrimitiveBedroom(object):

    def __init__(self, raster):
        self._raster = raster
        self._queries = {}

    # ******************************************************************************************* **
    def receive_primitives_ready(self, query_key, computation_index):
        """Receive message: All the primitive for a computation are ready"""
        msgs = []
        return msgs

    def receive_production_queue_update(self, query_key, produced_count, max_queue_size):
        """Receive message: An array was produced upstream, wake up some computations"""
        msgs = []

        if query_key in self._queries:
            self._queries[query_key] = _Query(
                produced_count, max_queue_size
            )
        query = self._queries[query_key]

        return msgs

class _Query(object):

    def __init__(self, produced_count, max_queue_size):
        self.produced_count = produced_count
        self.max_queue_size = max_queue_size
        self.woke_count = 0
        self.sleeping = {}
