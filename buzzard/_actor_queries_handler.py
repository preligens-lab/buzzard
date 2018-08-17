
class ActorQueriesHandler(object):

    def __init__(self, raster):
        self._raster = raster
        self._query_counter = 0
        self._queries = {}

    @property
    def address(self):
        return '/Raster{}/QueriesHandler'.format(self._raster.uid)

    # ******************************************************************************************* **
    def receive_external_new_query(self, queue_wref, max_queue_size, produce_fps,
                                   band_ids, dst_nodata, interpolation):
        """Receive message sent by other thread: There is a new query"""
        msgs = []

        query_key = self._query_counter
        self._query_counter += 1
        query = _Query(queue_wref, max_queue_size, len(produce_fps))
        self._queries[query_key] = query
        msgs += [
            Msg('Producer', 'make_those_arrays', query_key, produce_fps, band_ids, dst_nodata, interpolation)
        ]

        return msgs

    def receive_nothing(self):
        """Receive message: What's up?
        Was output queue sinked?
        Was output queue collected by gc?
        """
        msgs = []

        killed_queries = []
        for query_key, query in self._queries.items():
            q = query.queue_wref()
            if q is None:
                killed_queries.append(query_key)
            else:
                new_queue_size = q.qsize()
                assert new_queue_size <= query.queue_size
                if new_queue_size != query.queue_size:
                    query.queue_size = new_queue_size
                    args = query_key, query.produced_count, query.queue_size
                    msgs += [
                        Msg('PriorityWatcher', 'output_queue_update', *args),
                        Msg('BuilderBedroom', 'output_queue_update', *args),
                        Msg('ComputationBedroom', 'output_queue_update', *args),
                    ]
            del q

        for query_key in killed_queries:
            msgs += [
                # Msg('RastersHandler', 'kill_this_query', query_key),
                # Msg('QueriesHandler', 'kill_this_query', query_key),
                Msg('Producer', 'kill_this_query', query_key),
                Msg('CacheHandler', 'kill_this_query', query_key),
                # Msg('FileHasher', 'kill_this_query', query_key),
                Msg('QueryWatcher', 'kill_this_query', query_key),
                Msg('PriorityWatcher', 'kill_this_query', query_key),
                Msg('Computer', 'kill_this_query', query_key),
                Msg('ComputationBedroom', 'kill_this_query', query_key),
                # Msg('ComputeAccumulator', 'kill_this_query', query_key),
                # Msg('Merger', 'kill_this_query', query_key),
                # Msg('Writer', 'kill_this_query', query_key),
                Msg('BuilderBedroom', 'kill_this_query', query_key),
                Msg('Builder', 'kill_this_query', query_key),
                Msg('Sampler', 'kill_this_query', query_key),
                Msg('Resampler', 'kill_this_query', query_key),
            ]
            del self._queries[query_key]

        return msgs

    def receive_made_this_array(self, query_key, produce_id, array):
        """Receive message: This array is ready to be sent to the output queue. Just do it in the
        righ order.
        """
        query = self._queries[query_key]
        assert produce_id not in query.produce_arrays_dict, 'This array was already computed'
        assert produce_id <= query.produced_count, 'This array was already sent'
        query.produce_arrays_dict[produce_id] = array

        produce_id = query.produced_count
        queue = query.queue_wref()
        if queue is None:
            # Queue is None (Queue was collected upstream by gc) -> Ignore the problem,
            # `receive_nothing` will be called soon
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

                query.queue_size += 1
                query.produced_count += 1
                produce_id  = query.produced_count
                args = query_key, query.produced_count, query.queue_size
                msgs += [
                    Msg('PriorityWatcher', 'output_queue_update', *args),
                    Msg('BuilderBedroom', 'output_queue_update', *args),
                    Msg('ComputationBedroom', 'output_queue_update', *args),
                ]
            if query.produced_count == query.to_produce_count:
                del self._queries[query_key]
        del queue

        return []

    def receive_die(self):
        """Receive message: The raster was killed"""
        self._queries.clear()

    # ******************************************************************************************* **

class _Query(object):

    def __init__(self, queue_wref, max_queue_size, to_produce_count):
        self.max_queue_size = max_queue_size
        self.queue_wref = queue_wref
        self.to_produce_count = to_produce_count

        self.produce_arrays_dict = {}
        self.produced_count = 0
        self.queue_size = 0
