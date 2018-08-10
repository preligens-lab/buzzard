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
    def __init__(self, raster, pool_actor):
        self._raster = raster
        self._queries = {}
        self._pool_actor = pool_actor

    def _schedule_one_compute(self, query, compute_fp, primitive_fps,
                              computation_index):
        """This closure takes care of the lifetime of a computation and it's primitives collection.
        """
        def _join_waiting_room():
            status = self._compute_fps_status[compute_fp]

            if status == _ComputeTileStatus.unseen:
                self._compute_fps_status[compute_fp] = _ComputeTileStatus.waiting
            elif status == _ComputeTileStatus.waiting:
                # Some other query already scheduled this computation
                # The first compute to `leave waiting room` will pull the primitives and be
                # launched in pool.
                # The next ones to `leave waiting room` will pull and discard the primitives
                pass
            elif status == _ComputeTileStatus.working:
                # Some other query already scheduled this computation
                # When `leave waiting room` is called we will pull and discard the primitives
                pass
            elif status == _ComputeTileStatus.computed:
                # Some other query already scheduled this computation
                # When `leave waiting room` is called we will pull and discard the primitives
                pass
            else:
                assert False

            self._pool_actor.waiting += [
                (de_quoi_id_la_prio, _leave_waiting_room),
            ]
            return []

        def _leave_waiting_room():
            if len(primitive_fps) > 0:
                primitive_arrays = query.get_primitive_arrays(computation_index)
            else:
                primitive_arrays = {}

            status = self._compute_fps_status[compute_fp]
            assert status != _ComputeTileStatus.unseen

            if status != _ComputeTileStatus.waiting:
                # The computation was already launched, do nothing else
                return
            self._compute_fps_status[compute_fp] = _ComputeTileStatus.working

            bands = tuple(range(1, self._raster.band_count + 1))
            if self.pool_actor.same_address_space:
                params = (
                    compute_fp, bands, primitive_fps, primitive_arrays,
                    self._raster.facade_proxy,
                )
            else:
                params = (
                    compute_fp, bands, primitive_fps, primitive_arrays,
                    None,
                )

            future = self._pool_actor.pool.apply_async(
                raster.compute_array,
                params
            )
            self._pool_actor.working += [
                (future, _computation_done),
            ]
            return []

        def _computation_done(array):
            status = self._compute_fps_status[compute_fp]
            assert status == _ComputeTileStatus.working
            self._compute_fps_status[compute_fp] = _ComputeTileStatus.computed

            return [
                Msg('Raster::ComputeAccumulator', 'done_one_compute', compute_fp, array),
            ]

        return _join_waiting_room()

    def receive_schedule_collection(self, query_key, compute_fps):
        """Receive message: Schedule a primitive collection for those computation tiles"""
        assert len(compute_fps) == len(set(compute_fps))

        msgs = []
        primitives = self._raster.request_queue_of_primitive_arrays

        if len(primitives) == 0:
            # There are no primitives, skip straight to computation phase
            query = _Query(query_key, compute_fps)
            for i, compute_fp in enumerate(compute_fps):
                msgs += self._schedule_one_compute(query, compute_fp, {}, i)

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

            query =_ParameterizedQuery(
                query_key, compute_fps, primitive_fps, primitive_queues
            )

        self._queries[query_key] = query
        return msgs

    def receive_nothing(self):
        """Receive message: What's up?

        Check if new primitive arrays arrived in the primitive queues
        """
        msgs = []
        for query_key, query in self._queries.items():
            queues_min_qsize = min(
                q.qsize()
                for q in query.primitive_queues.values()
            )
            assert queues_min_qsize >= query.queues_min_qsize
            for _ in range(queues_min_qsize - query.queues_min_qsize):
                compute_index = query.pulled_count + query.queues_min_qsize
                compute_fp = query.compute_fps[compute_index]

                primitive_fps = {
                    k: fps[compute_index]
                    for k, fps in query.primitives_fps
                }
                msgs += self._schedule_one_compute(query, compute_fp, primitive_fps, compute_index)
                query.queues_min_qsize += 1

        return msgs

    def receive_query_dropped(self, query_key):
        """Receive message: One query was dropped"""
        if query_key in self._queries:
            del self._queries[query_key]

class _Query(object):
    def __init__(self, query_key, compute_fps):
        self.query_key = query_key
        self.compute_fps = compute_fps

class _ParameterizedQuery(_Query):
    """Class that stores info about a query that has primitives"""

    def __init__(self, query_key, compute_fps, primitive_fps, primitive_queues):
        super().__init__(query_key, compute_fps)

        self.primitive_fps = primitive_fps
        self.primitive_queues = primitive_queues
        self.queues_min_qsize = 0
        self.pulled_count = 0

    def get_primitive_arrays(self, compute_index):
        assert compute_index == self.pulled_count
        assert self.queues_min_qsize > 0

        self.pulled_count += 1
        self.queues_min_qsize -= 1
        return {
            k: q.get(block=False)
            for k, q in self.primitive_queues
        }

class _ComputationTileStatus(enum.Enum):
    unseen = 0
    waiting = 1
    working = 2
    computed = 3
