import collections

class ActorCollection(object):
    """Actor that takes care of the primitive collection phase before computation.

    The actual primitive collection via queue.Queue.get method call should be delayed as much as
    possible to avoid creating inter-raster backpressure in the computation pool waiting room, this
    means that the Queue.get call has to be performed just before launching the work in the
    computation pool, and not when the job joins the computation waiting room. To achieve this
    behevior the ActorCollection sends a closure to the ActorComputation to perform late
    collection. It clearly violates the classic rules of actor model.

    """
    def __init__(self, raster):
        self._raster = raster
        self._queries = {}

    def receive_schedule_collection(self, query_key, compute_fps):
        """Receive message: Schedule a primitive collection for those computation tiles"""
        msgs = []
        primitives = self._raster.request_queue_of_primitive_arrays

        if len(primitives) == 0:
            # There are not primitived, skip straight to computation phase
            msgs += [
                Msg('Raster::Compute', 'schedule_one_compute',
                    query_key,
                    compute_fp,
                    primitive_fps={},
                    primitive_arrays_thunk=lambda: {}
                )
                for compute_fp in compute_fps
            ]

        else:
            # Create the connection with each primitive
            primitives_fps = collection.default_dict(list)
            for compute_fp in compute_fps:
                collect_fps = raster.to_collect_of_to_compute(compute_fp)
                for prim_key, collect_fp in collect_fps.items():
                    primitives_fps[prim_key].append(collect_fp)

            primitive_queues = {}
            for k, collect_fps in primitives_fps.items():
                primitive_queues[k] = primitives[k](collect_fps)

            self._queries[query_key] =_Query(
                query_key, compute_fps, primitive_fps, primitive_queues
            )
        return msgs

    def receive_nothing(self):
        """Receive message: did something happened on your side?"""
        msgs = []
        for query_key, query in self._queries.items():
            ready_count = min(
                q.qsize()
                for q in query.primitive_queues.values()
            )
            assert ready_count >= query.ready_count
            for _ in range(ready_count - query.ready_count):
                compute_index = query.sent_count + query.ready_count
                compute_fp = query.compute_fps[compute_index]
                # TODO: Check that it was not launched
                msgs += [
                    Msg('Raster::Compute', 'schedule_one_compute',
                        query_key,
                        compute_fp,
                        primitive_fps={
                            k: fps[compute_index]
                            for k, fps in query.primitives_fps
                        },
                        primitive_arrays_thunk=functools.partial(
                            query.get_primitive_arrays,
                            compute_index,
                        ),
                    )
                ]
                query.ready_count += 1

        return msgs

    def receive_query_dropped(self, query_key):
        """Receive message: One query was dropped"""
        if query_key in self._queries:
            del self._queries[query_key]

class _Query(object):
    def __init__(self, query_key, compute_fps, primitive_fps, primitive_queues):
        self.query_key = query_key
        self.compute_fps = compute_fps
        self.primitive_fps = primitive_fps
        self.primitive_queues = primitive_queues
        self.ready_count = 0
        self.sent_count = 0

    def get_primitive_arrays(self, compute_index):
        assert compute_index == self.sent_count
        assert self.ready_count > 0

        self.sent_count += 1
        self.ready_count -= 1
        return {
            k: q.get(block=False)
            for k, q in self.primitive_queues
        }
