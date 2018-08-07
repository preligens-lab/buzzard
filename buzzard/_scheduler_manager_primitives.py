
class ManagerPrimitives(object):

    def __init__(self, truc, raster):
        self._truc, self._raster = truc, raster

        # Mutable variables
        self._waiting_queries = []
        self._working_queries = []

    # ******************************************************************************************* **
    def put_in_waiting_room(self, query):
        self._waiting_queries.append(query)

    # ******************************************************************************************* **
    def list_events(self):
        events = {
            'collection-ready': []
        }
        for i, (queues, query, prev_ready_count) in enumerate(self._working_queries):
            ready_count = min(
                queue.qsize()
                for queue in queues
            )
            assert ready_count >= prev_ready_count
            if ready_count != prev_ready_count:
                for _ in range(ready_count - prev_ready_count):
                    events['collection-ready'].append(
                        (i, queues, query)
                    )
        return events

    def update_states(self, events):
        for i, queues, query in events['collection-ready']:
            query.spawn_compute_action(
                # closure with get to queues
            )
        for i in sorted(i for i, _, _ in events['collection-ready'])[::-1]:
            del self._working_queries[i]

    def take_actions(self):
        for query in self._waiting_queries:
            collect_fps_of_primitive_key = query.collect_fps_of_primitive_key
            queues = {
                key: fn(collect_fps_of_primitive_key[key])
                for key, fn in self._request_primitive_arrays.items()
            }
            self._working_queries.append((queues, query, 0))
        self._waiting_queries = []

    # ******************************************************************************************* **
