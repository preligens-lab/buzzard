

class ActorCommunicator(object):

    def __init__(self, raster):
        self._raster = raster
        self._query_counter = 0
        self._queries = {}

    # ******************************************************************************************* **
    def receive_new_query(self, queue_wref, max_queue_size, produce_fps, band_ids, dst_nodata, interpolation):
        msgs = []

        query_key = self._query_counter
        self._query_counter += 1

        msgs += [
            Msg('Raster::Producer', '', query_key, produce_fps, band_ids, dst_nodata, interpolation)
        ]

        return msgs

    def receive_nothing(self):
        msgs = []
        killed_queries = [
            query_key
            for query_key, query in self._queries.items()
            if query.queue_wref() is None
        ]
        for query_key in killed_queries:
            msgs += [
                Msg('Raster::Sample', 'query_dropped', query_key),
                Msg('Raster::Resample', 'query_dropped', query_key),
                Msg('Raster::Producer', 'query_dropped', query_key),
                Msg('Raster::', 'query_dropped', query_key),
                Msg('Raster::', 'query_dropped', query_key),
                Msg('Raster::', 'query_dropped', query_key),
                Msg('Raster::', 'query_dropped', query_key),
                Msg('Raster::', 'query_dropped', query_key),
                Msg('Raster::', 'query_dropped', query_key),
                Msg('Raster::', 'query_dropped', query_key),
                Msg('Raster::', 'query_dropped', query_key),
                Msg('Raster::', 'query_dropped', query_key),
            ]

            del self._queries[query_key]
            pass

        return msgs

    def receive_produce_array(self, query_key, produce_id, array):
        """Receive message: This array is ready to be sent to the output queue. Just do it in the
        righ order.
        """
        query = self._queries[query_key]
        assert produce_id not in query.produce_arrays_dict
        assert produce_id <= query.produced_count
        query.produce_arrays_dict[produce_id] = array

        produce_id = query.produced_count
        queue = query.queue_wref()
        if queue is None:
            # Queue is None (Queue was collected upstream by gc) -> Ignore the problem,
            # `receive_kill_query` will be called soon on all actors
            pass
        else:
            # Put arrays in queue in the right order
            while True:
                if produce_id not in query.produce_arrays_dict:
                    # Next array is not ready yet
                    break
                array = query.produce_arrays_dict.pop(produce_id)

                # The way this is all designed, the system does not start to work on a `produce` if
                # it cannot be inserted in the output queue. It means that the `queue.Full`
                # exception cannot be raised by the following `put_nowait`.
                queue.put_nowait(array)

                query.produced_count += 1
                produce_id  = query.produced_count
            if query.produced_count == query.to_produce_count :
                del self._queries[query_key]

        return []

    # ******************************************************************************************* **

class _Query(object):

    def __init__(self, queue_wref, max_queue_size, to_produce_count):
        self.max_queue_size = max_queue_size
        self.queue_wref = queue_wref
        self.produced_count = 0
        self.to_produce_count = to_produce_count
        self.produce_arrays_dict = {}
