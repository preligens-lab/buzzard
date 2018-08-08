import collections

class ActorCollection(object):
    def __init__(self):
        self._collection_queries = []

    def receive_nothing(self):
        msgs = []
        for collection_query in self._collection_queries:
            ready_count = min(
                q.qsize()
                for q in collection_query.primitive_queues.values()
            )
            assert ready_count >= collection_query.ready_count
            for _ in range(ready_count - collection_query.ready_count):
                compute_index = collection_query.sent_count + collection_query.ready_count
                compute_fp = collection_query.compute_fps[compute_index]
                msgs += [
                    Msg('Compute', 'schedule_one_compute',
                        collection_query.raster,
                        collection_query.compute_fps[compute_index],
                        functools.partial(
                            get_primitive_arrays,
                            collection_query,
                            compute_index,
                        ),
                    )
                ]
                collection_query.ready_count += 1

        return msgs

    def receive_schedule_collection(self, raster, compute_fps):
        primitives_fps = collection.default_dict(list)
        primitive_queues = {}

        for compute_fp in compute_fps:
            collect_fps = raster.to_collect_of_to_compute(compute_fp)
            for k, collect_fp in collect_fps.items():
                primitives_fps[k].append(collect_fp)
        for k, collect_fps in primitives_fps.items():
            primitive_queues[k] = raster.primitives[k](collect_fps)

        self._collection_queries += [
            _CollectionQuery(
                raster, compute_fps, primitive_fps, primitive_queues
            )
        ]
        return []

class _CollectionQuery(object):
    def __init__(self, raster, compute_fps, primitive_fps, primitive_queues):
        self.raster = raster
        self.compute_fps = compute_fps
        self.primitive_fps = primitive_fps
        self.primitive_queues = primitive_queues
        self.ready_count = 0
        self.sent_count = 0

def get_primitive_arrays(collection_query, compute_index):
    assert compute_index == collection_query.sent_count
    assert collection_query.ready_count > 0
    collection_query.sent_count += 1
    collection_query.ready_count -= 1
