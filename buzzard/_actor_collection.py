import collections

class ActorCollection(object):
    """Takes care of the primitive collection phase before computation"""
    def __init__(self, raster):
        self._raster = raster
        self._queries = []

    def receive_nothing(self):
        msgs = []
        for query in self._queries:
            ready_count = min(
                q.qsize()
                for q in query.primitive_queues.values()
            )
            assert ready_count >= query.ready_count
            for _ in range(ready_count - query.ready_count):
                compute_index = query.sent_count + query.ready_count
                compute_fp = query.compute_fps[compute_index]
                msgs += [
                    Msg('Compute', 'schedule_one_compute',
                        self._raster,
                        compute_fp,
                        {
                            k: fps[compute_index]
                            for k, fps in query.primitives_fps
                        },
                        functools.partial(
                            get_primitive_arrays,
                            query,
                            compute_index,
                        ),
                    )
                ]
                query.ready_count += 1

        return msgs

    def receive_schedule_collection(self, compute_fps):
        msgs = []
        primitives_fps = collection.default_dict(list)
        primitive_queues = {}

        primitives = self._raster.request_queue_of_primitive_arrays
        if len(primitives) == 0:
            msgs += [
                Msg('Compute', 'schedule_one_compute',
                    self._raster, compute_fp, lambda: {}
                )
                for compute_fp in compute_fps
            ]
        else:
            for compute_fp in compute_fps:
                collect_fps = raster.to_collect_of_to_compute(compute_fp)
                for prim_key, collect_fp in collect_fps.items():
                    primitives_fps[prim_key].append(collect_fp)
            for k, collect_fps in primitives_fps.items():
                primitive_queues[k] = primitives[k](collect_fps)

            self._queries += [
                _Query(compute_fps, primitive_fps, primitive_queues)
            ]
        return msgs

class _Query(object):
    def __init__(self, compute_fps, primitive_fps, primitive_queues):
        self.compute_fps = compute_fps
        self.primitive_fps = primitive_fps
        self.primitive_queues = primitive_queues
        self.ready_count = 0
        self.sent_count = 0

def get_primitive_arrays(query, compute_index):
    assert compute_index == query.sent_count
    assert query.ready_count > 0
    query.sent_count += 1
    query.ready_count -= 1
    return {
        k: q.get(block=False)
        for k, q in query.primitive_queues
    }
