"""
Multi-threaded, back pressure management, caching
"""
import multiprocessing as mp
import multiprocessing.pool
import time
import datetime
import collections
from collections import defaultdict
import uuid
import typing

import numpy as np
import networkx as nx

from buzzard._footprint import Footprint
from buzzard._tools import DebugCtxMngmnt
# get_uname, qeinfo, qrinfo, self._ds.thread_pool_task_counter


class BackendRaster(object):
    """
    class defining the raster behaviour
    """
    def __init__(self,
                 ds,
                 footprint: Footprint,
                 dtype,
                 nbands: int,
                 nodata: typing.Union[None, float],
                 sr,
                 computation_function,
                 io_pool: mp.pool.Pool,
                 computation_pool: mp.pool.Pool,
                 primitives: dict,
                 to_collect_of_to_compute,
                 max_computation_size,
                 merge_pool: mp.pool.Pool,
                 merge_function,
                 debug_callbacks):

        self._ds = ds

        self._debug_watcher = DebugCtxMngmnt(debug_callbacks, self)

        self._full_fp = footprint
        if computation_function is None:
            raise ValueError("computation function must be provided")
        self._compute_data = computation_function
        self._dtype = dtype
        self._num_bands = nbands
        self._nodata = nodata
        self._wkt_origin = sr

        self._primitive_functions = primitives
        self._primitive_rasters = {
            key: (primitives[key].primitive if hasattr(primitives[key], "primitive") else None)
            for key in primitives
        }

        self._io_pool = io_pool
        self._computation_pool = computation_pool
        self._merge_pool = merge_pool

        self._to_collect_of_to_compute = to_collect_of_to_compute

        self._max_computation_size = max_computation_size

        self._queries = []
        self._new_queries = []

        self._graph = nx.DiGraph()

        self._merge_data = merge_function

        # Used to track the number of pending tasks
        self._num_pending = defaultdict(int)

        self._stop_scheduler = False


    def _pressure_ratio(self, query):
        """
        defines a pressure ration of a query: lesser values -> emptier query
        """
        if query.produced() is None:
            return -1
        num = query.produced().qsize() + self._num_pending[id(query)]
        den = query.produced().maxsize
        return num/den
        # return np.random.rand()

    @property
    def fp(self):
        """
        returns the raster footprint
        """
        return self._full_fp

    @property
    def nbands(self):
        """
        returns the raster's number of bands'
        """
        return self._num_bands

    @property
    def nodata(self):
        """
        returns the raster nodata
        """
        return self._nodata

    @property
    def wkt_origin(self):
        """
        returns the raster wkt origin
        """
        return self._wkt_origin

    @property
    def dtype(self):
        """
        returns the raster dtype
        """
        return self._dtype

    @property
    def pxsizex(self):
        """
        returns the raster 1D pixel size
        """
        return self._full_fp.pxsizex

    @property
    def primitives(self):
        """
        Returns dictionnary of raster primitives
        """
        return self._primitive_rasters.copy()


    def __len__(self):
        return int(self._num_bands)


    def _scheduler(self):
        from buzzard._backend_cached_raster import BackendCachedRaster
        header = '{!s:25}'.format('<prim[{}] {} {}>'.format(
            ','.join(self._primitive_functions.keys()),
            self._num_bands,
            self.dtype,
        ))
        # header = f"{list(self._primitive_functions.keys())!s:20} {self._num_bands!s:3} {self.dtype!s:10}"
        self.h = header
        print(header, "scheduler in")
        # list of available and produced to_collect footprints
        available_to_produce = set()
        put_counter = collections.Counter()
        get_counter = collections.Counter()

        while True:
            # time.sleep(0.05)
            skip = False

            if self._stop_scheduler:
                print("going to sleep")
                return

            assert len(set(map(id, self._queries))) == len(self._queries)
            assert len(set(map(id, self._new_queries))) == len(self._new_queries)

            # Consuming the new queries
            while self._new_queries:
                with self._debug_watcher("scheduler::new_query"):
                    query = self._new_queries.pop(0)

                    # if cached raster, checking the cache
                    if isinstance(self, BackendCachedRaster):
                        # adding the queries to check
                        unique_to_read_fps = set()
                        for to_produce in query.to_produce:
                            unique_to_read_fps |= set(self._to_read_of_to_produce(to_produce[0]))
                        query.to_check = list(unique_to_read_fps)

                    self._queries.append(query)

                    # print(self.h, qrinfo(query), f'new query with {len(query.to_produce)} to_produce arrived')
                    skip = True
                    break

            # ordering queries accroding to their pressure
            assert len(set(map(id, self._queries))) == len(self._queries)
            assert len(set(map(id, self._new_queries))) == len(self._new_queries)

            ordered_queries = sorted(self._queries, key=self._pressure_ratio)

            # getting the emptiest query
            for query in ordered_queries:
                if skip:
                    break

                # If the query has been dropped
                if query.produced() is None:
                    with self._debug_watcher("scheduler::cleaning_dropped_query"):
                        # print(self.h, qrinfo(query), f'cleaning: dropped by main program')
                        if self._num_pending[id(query)]: # could be false because dropped too early
                            del self._num_pending[id(query)]
                        if query.was_included_in_graph:
                            to_delete_nodes = list(nx.dfs_postorder_nodes(self._graph, source=id(query)))
                            for node_id in to_delete_nodes:
                                node = self._graph.nodes[node_id]
                                node["linked_queries"].remove(query)
                                if not node["linked_queries"]:
                                    self._graph.remove_node(node_id)
                        self._queries.remove(query)

                        skip = True
                        break

                # If there are still fps to check
                if query.to_check and self._ds.thread_pool_task_counter[id(self._io_pool)] < self._io_pool._processes:
                    assert isinstance(self, BackendCachedRaster)
                    with self._debug_watcher("scheduler::starting_check_fp"):
                        to_check_fp = query.to_check.pop(0)
                        index = self._indices_of_cache_tiles[to_check_fp]

                        if self._cache_checksum_array[index] is None:
                            # print(self.h, qrinfo(query), f'checking a cache footprint')
                            query.checking.append((index, self._io_pool.apply_async(self._check_cache_file, (to_check_fp, ))))
                            self._ds.thread_pool_task_counter[id(self._io_pool)] += 1

                            skip = True
                            break

                # If there are still fps currently being checked
                if query.checking:
                    assert isinstance(self, BackendCachedRaster)
                    for still_checking in query.checking:
                        if still_checking[1].ready():
                            with self._debug_watcher("scheduler::ending_check_fp"):
                                # print(self.h, qrinfo(query), f'checked a cache footprint')
                                self._cache_checksum_array[still_checking[0]] = still_checking[1].get()
                                self._ds.thread_pool_task_counter[id(self._io_pool)] -= 1
                                query.checking.remove(still_checking)

                                skip = True
                                break

                # If there are still fps to check or currently being checked, skipping query
                if query.to_check or query.checking:
                    assert isinstance(self, BackendCachedRaster)
                    continue

                if not query.was_included_in_graph:
                    with self._debug_watcher("scheduler::updating_graph"):
                        self._update_graph_from_query(query)
                        query.was_included_in_graph = True
                        # print(self.h, qrinfo(query), f'new query with {list(len(p) for p in query.to_collect.values())} to_collect was added to graph')
                        skip = True
                        break

                # If all to_produced was consumed: query ended
                if not query.to_produce:
                    with self._debug_watcher("scheduler::cleaning_ended_query"):
                        # print(self.h, qrinfo(query), f'cleaning: treated all produce')
                        del self._num_pending[id(query)]
                        self._graph.remove_node(id(query))
                        self._queries.remove(query)

                        skip = True
                        break

                isolates_not_query = [
                    node_id for node_id in list(nx.isolates(self._graph))
                    if node_id not in list(map(id, self._queries))
                ]
                # checking if the graph was correctly cleaned
                assert len(isolates_not_query) == 0, isolates_not_query

                # if the emptiest query is full, waiting
                if query.produced().full():
                    continue

                # detecting which produce footprints are available
                # while there is space
                while query.produced().qsize() + self._num_pending[id(query)] < query.produced().maxsize and query.to_produce[-1][1] == "sleeping":
                    with self._debug_watcher("scheduler::produce_sleeping_to_pending"):
                        # getting the first sleeping to_produce
                        first_sleeping_i = [to_produce[1] for to_produce in query.to_produce].index('sleeping')
                        to_produce_available = query.to_produce[first_sleeping_i][0]

                        # getting its id in the graph
                        to_produce_available_id = query.to_produce[first_sleeping_i][2]

                        available_to_produce.add(to_produce_available_id)

                        to_produce_index = query.to_produce.index((to_produce_available, "sleeping", to_produce_available_id))
                        query.to_produce[to_produce_index] = (to_produce_available, "pending", to_produce_available_id)

                        self._num_pending[id(query)] += 1

                        assert query.produced().qsize() + self._num_pending[id(query)] <= query.produced().maxsize

                        skip = True
                        break

                # getting the in_queue of data to discard
                for primitive in query.collected:
                    if not query.collected[primitive].empty() and query.to_collect[primitive][0] in query.to_discard[primitive]:
                        with self._debug_watcher("scheduler::discard"):
                            query.collected[primitive].get(block=False)
                            skip = True
                            break

                # iterating through the graph
                for index, to_produce in enumerate(query.to_produce):
                    if skip:
                        break

                    if to_produce[1] == "sleeping":
                        continue

                    # beginning at to_produce
                    first_node_id = to_produce[2]

                    # going as deep as possible
                    depth_node_ids = iter(nx.dfs_postorder_nodes(self._graph, source=first_node_id))

                    while True:
                        try:
                            node_id = next(depth_node_ids)
                        except StopIteration:
                            break

                        node = self._graph.nodes[node_id]

                        # If there are out edges, not stopping (unless it is a compute node)
                        if len(self._graph.out_edges(node_id)) > 0:
                            continue

                        # Skipping the nodes not linked to available (pending) to_produce
                        if available_to_produce.isdisjoint(node["linked_to_produce"]):
                            continue

                        # if deepest is to_compute, collecting (if possible) and computing
                        if node["type"] == "to_compute" and node["future"] is None:
                            # testing if at least 1 of the collected queues is empty (1 queue per primitive)
                            if any([query.collected[primitive].empty() for primitive in query.collected]):
                                continue

                            # asserting it's the 1st to_compute
                            if query.to_compute.index(node['footprint']) != 0:
                                continue

                            if self._ds.thread_pool_task_counter[id(node["pool"])] < node["pool"]._processes:
                                with self._debug_watcher("scheduler::compute"):
                                    collected_data = []
                                    primitive_footprints = []

                                    get_counter[query] += 1
                                    # print(self.h, qrinfo(query), f'compute data for the {get_counter[query]:02d}th time node_id:({node_id})')

                                    for collected_primitive in query.collected.keys():
                                        collected_data.append(query.collected[collected_primitive].get(block=False))
                                        primitive_footprints.append(query.to_collect[collected_primitive].pop(0))

                                    assert len(collected_data) == len(self._primitive_functions.keys())

                                    node["future"] = self._computation_pool.apply_async(
                                        self._compute_data,
                                        (
                                            node["footprint"],
                                            collected_data,
                                            primitive_footprints,
                                            self
                                        )
                                    )

                                    self._ds.thread_pool_task_counter[id(self._computation_pool)] += 1
                                    query.to_compute.pop(0)
                                    node["linked_queries"].remove(query)

                                    for linked_query in node["linked_queries"]:
                                        for collected_primitive, primitive_footprint in zip(query.collected.keys(), primitive_footprints):
                                            linked_query.to_discard[collected_primitive].append(primitive_footprint)

                                    skip = True
                                    break

                        # if the deepest is to_produce, updating produced
                        if index == 0 and node["type"] == "to_produce":
                            with self._debug_watcher("scheduler::put_data"):
                                # If the query has not been dropped
                                if query.produced() is not None:
                                    assert not query.produced().full()
                                    if node["is_flat"]:
                                        node["in_data"] = node["in_data"].squeeze(axis=-1)
                                    query.produced().put(node["in_data"].astype(self._dtype), timeout=1e-2)

                                query.to_produce.pop(0)

                                put_counter[query] += 1
                                # print(self.h, qrinfo(query), f'    put data for the {put_counter[query]:02d}th time, {len(query.to_produce):02d} left')

                                self._graph.remove_node(node_id)
                                self._num_pending[id(query)] -= 1

                                skip = True
                                break

                        # skipping the ready to_produce that are not at index 0
                        if node["type"] == "to_produce":
                            continue

                        if node["type"] == "to_merge" and node["future"] is None:
                            if self._ds.thread_pool_task_counter[id(self._merge_pool)] < self._merge_pool._processes:
                                with self._debug_watcher("scheduler::starting_merge"):
                                    node["future"] = self._merge_pool.apply_async(
                                        self._merge_data,
                                        (
                                            node["footprint"],
                                            node["in_fp"],
                                            node["in_data"]
                                        )
                                    )
                                    self._ds.thread_pool_task_counter[id(self._merge_pool)] += 1
                                    assert self._ds.thread_pool_task_counter[id(self._merge_pool)] <= self._merge_pool._processes

                                    skip = True
                                    break

                        in_edges = list(self._graph.in_edges(node_id))

                        if node["future"] is None:
                            if self._ds.thread_pool_task_counter[id(node["pool"])] < node["pool"]._processes:
                                if node["type"] == "to_read":
                                    with self._debug_watcher("scheduler::starting_read"):
                                        assert len(in_edges) == 1
                                        in_edge = in_edges[0]
                                        produce_node = self._graph.nodes[in_edge[0]]
                                        if produce_node["in_data"] is None:
                                            produce_node["in_data"] = np.full(
                                                tuple(produce_node["footprint"].shape) + (len(query.bands),),
                                                self.nodata or 0,
                                                dtype=self.dtype
                                            )
                                        node["future"] = self._io_pool.apply_async(
                                            self._read_cache_data,
                                            (
                                                node["footprint"],
                                                produce_node["footprint"],
                                                produce_node["in_data"],
                                                node["bands"]
                                            )
                                        )
                                else:
                                    assert node["type"] == "to_write"
                                    with self._debug_watcher("scheduler::starting_write"):
                                        node["future"] = node["pool"].apply_async(
                                            self._write_cache_data,
                                            (
                                                node["footprint"],
                                                node["in_data"]
                                            )
                                        )
                                    self._ds.thread_pool_task_counter[id(node["pool"])] += 1

                                skip = True
                                break

                        elif node["future"].ready():
                            with self._debug_watcher("scheduler::ending_" + node["type"] + "_operation"):
                                in_data = node["future"].get()
                                if node["type"] == "to_write":
                                    self._cache_checksum_array[self._indices_of_cache_tiles[node["footprint"]]] = True
                                if in_data is not None:
                                    in_data = in_data.astype(self._dtype).reshape(tuple(node["footprint"].shape) + (len(node["bands"]),))
                                self._ds.thread_pool_task_counter[id(node["pool"])] -= 1

                                for in_edge in in_edges:
                                    in_node = self._graph.nodes[in_edge[0]]
                                    if in_node["type"] == "to_merge":
                                        in_node["in_data"].append(in_data)
                                        in_node["in_fp"].append(node["footprint"])
                                    elif in_node["type"] == "to_produce" and node["type"] == "to_read":
                                        pass
                                    else:
                                        in_node["in_data"] = in_data
                                    self._graph.remove_edge(*in_edge)

                                self._graph.remove_node(node_id)

                                skip = True
                                break

            if not skip:
                if not self._queries:
                    time.sleep(0.2)
                else:
                    time.sleep(0.1)



    def _collect_data(self, to_collect):
        """
        collects data from primitives
        in: {
           "prim_1": [to_collect_p1_1, ..., to_collect_p1_n],
           ...,
           'prim_p": [to_collect_pp_1, ..., to_collect_pp_n]
        }
        out: {"prim_1": queue_1, "prim_2": queue_2, ..., "prim_p": queue_p}
        """
        # print(self.h, "collecting")
        results = {}
        for primitive in self._primitive_functions.keys():
            results[primitive] = self._primitive_functions[primitive](to_collect[primitive])
        return results

    def _to_compute_of_to_produce(self, fp):
        count = np.ceil(fp.rsize / self._max_computation_size)
        tiles = fp.tile_count(*count)
        return list(tiles.flat)


    def _update_graph_from_query(self, new_query):
        """
        Updates the dependency graph from the new queries (NO CACHE!)
        """

        # {
        #    "p1": [to_collect_p1_1, ..., to_collect_p1_n],
        #    ...,
        #    "pp": [to_collect_pp_1, ..., to_collect_pp_n]
        # }
        # with p # of primitives and n # of to_compute fps

        # initializing to_collect dictionnary
        new_query.to_collect = {key: [] for key in self._primitive_functions.keys()}
        new_query.to_discard = {key: [] for key in self._primitive_functions.keys()}

        self._graph.add_node(
            id(new_query),
            linked_queries=set([new_query]),
        )
        # time_dict = defaultdict(float)
        # counter = defaultdict(int)
        for to_produce, _, to_produce_uid in new_query.to_produce:
            # start = datetime.datetime.now()
            # print(self.h, qrinfo(new_query), f'{"to_produce":>15}', to_produce_uid)
            self._graph.add_node(
                to_produce_uid,
                futures=[],
                footprint=to_produce,
                in_data=None,
                type="to_produce",
                linked_to_produce=set([to_produce_uid]),
                linked_queries=set([new_query]),
                bands=new_query.bands,
                is_flat=new_query.is_flat
            )

            self._graph.add_edge(id(new_query), to_produce_uid)
            to_merge = to_produce

            to_merge_uid = str(uuid.uuid4())
            # to_merge_uid = get_uname()

            # print(self.h, qrinfo(new_query), f'    {"to_merge":>15}', to_merge_uid)

            self._graph.add_node(
                to_merge_uid,
                footprint=to_merge,
                future=None,
                futures=[],
                type="to_merge",
                pool=self._merge_pool,
                in_data=[],
                in_fp=[],
                linked_to_produce=set([to_produce_uid]),
                linked_queries=set([new_query]),
                bands=new_query.bands
            )
            self._graph.add_edge(to_produce_uid, to_merge_uid)

            if self._max_computation_size is not None:
                multi_to_compute = self._to_compute_of_to_produce(to_merge)
            else:
                multi_to_compute = [to_produce]
            # time_dict["produce"] += (datetime.datetime.now() - start).total_seconds()

            for to_compute in multi_to_compute:
                # start = datetime.datetime.now()
                to_compute_uid = str(uuid.uuid4())
                # to_compute_uid = get_uname()

                # print(self.h, qrinfo(new_query), f'        {"to_compute":>15}', to_compute_uid)

                self._graph.add_node(
                    to_compute_uid,
                    footprint=to_compute,
                    future=None,
                    type="to_compute",
                    pool=self._computation_pool,
                    in_data=None,
                    linked_to_produce=set([to_produce_uid]),
                    linked_queries=set([new_query]),
                    bands=new_query.bands
                )
                new_query.to_compute.append(to_compute)

                self._graph.add_edge(to_merge_uid, to_compute_uid)

                if self._to_collect_of_to_compute is None:
                    continue
                # time_dict["compute1"] += (datetime.datetime.now() - start).total_seconds()
                # start = datetime.datetime.now()
                multi_to_collect = self._to_collect_of_to_compute(to_compute)
                # counter["multi_to_c"] += 1
                # time_dict["compute2"] += (datetime.datetime.now() - start).total_seconds()
                # start = datetime.datetime.now()
                # np.arange(10000)**2**0.5
                # time_dict["compute3"] += (datetime.datetime.now() - start).total_seconds()
                # start = datetime.datetime.now()

                if multi_to_collect.keys() != self._primitive_functions.keys():
                    raise ValueError("to_collect keys do not match primitives")

                for key in multi_to_collect:
                    new_query.to_collect[key].append(multi_to_collect[key])

            # print(time_dict)
            # print(counter)
        new_query.collected = self._collect_data(new_query.to_collect)
