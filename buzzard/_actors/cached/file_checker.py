import functools

from buzzard._actors.message import Msg
from buzzard._actors.pool_job import MaxPrioJobWaiting, PoolJobWorking

class ActorFileChecker(object):
    """Actor that takes care of performing various checks on a cache file from a pool"""

    def __init__(self, raster):
        self._raster = raster
        self._alive = True
        self._waiting_room_address = '/Pool{}/WaitingRoom'.format(id(raster.file_checker_pool))
        self._working_room_address = '/Pool{}/WorkingRoom'.format(id(raster.file_checker_pool))
        self._waiting_jobs = set()
        self._working_jobs = set()

    @property
    def address(self):
        return '/Raster{}/FileChecker'.format(self._raster.uid)

    @property
    def alive(self):
        return self._alive

    # ******************************************************************************************* **
    def receive_infer_cache_file_status(self, cache_fp, path):
        wait = Wait(self, cache_fp, path)
        self._waiting_jobs.add(wait)

    def receive_token_to_working_room(self, job, token):
        self._waiting_jobs.remove(job)
        work = Work(self, job.cache_fp, job.path)
        self._working_jobs.add(work)
        return [
            Msg(self._working_room_address, 'launch_job_with_token', work, token)
        ]

    def receive_job_done(self, job, status):
        self._working_jobs.remove(job)
        return [
            Msg('CacheSupervisor', 'inferred_cache_file_status', job.cache_fp, job.path, status)
        ]

    def receive_die(self):
        msgs = []
        for job in self._waiting_jobs:
            msgs += [Msg(self._waiting_room_address, 'unschedule_job', job)]
        for job in self._working_jobs:
            msgs += [Msg(self._working_room_address, 'cancel_job', job)]
        self._waiting_jobs.clear()
        self._working_jobs.clear()
        return msgs

    # ******************************************************************************************* **

class Wait(MaxPrioJobWaiting):
    def __init__(self, actor, cache_fp, path):
        self.cache_fp = cache_fp
        self.path = path
        super().__init__(actor.address)

class Work(PoolJobWorking):
    def __init__(self, actor, cache_fp, path):
        self.cache_fp = cache_fp
        self.path = path
        func = functools.partial(
            _cache_file_check,
            cache_fp, path, len(actor._raster), actor._raster.dtype,
        )
        super().__init__(actor.address, func)

# class Wait: # FileChecker

#     prio = (0,)

#     def __init__(self, actor, cache_fp, path):
#         super().__init__(
#             actor.address,
#             is_file_check=True,
#             cache_fp=cache_fp,
#             qi=None,
#             prod_idx=None,
#             action_priority=None,
#         )

#         self.cache_fp = cache_fp
#         self.path = path

#     def get_prio(self):
#         return () # Maximal priority

# class Wait(): # Computation
#     def get_prio(self):
#         prod_array_pulled_count = ...
#         prod_fp = ...
#         return (
#             1,
#             self.prod_idx - prod_array_pulled_count,
#             round(prod_fp.rarea / 200000) * 200000,
#             4,
#         )

# class Wait(): # Merge
#     def get_prio(self):
#         most_urgent_qi, most_urgent_prod_idx = ...
#         prod_array_pulled_count = ...
#         prod_fp = ...
#         return (
#             1,
#             most_urgent_prod_idx - prod_array_pulled_count,
#             round(prod_fp.rarea / 200000) * 200000,
#             3,
#         )



# class GPW():

#     # d = {} # type: Mapping[CachedQueryInfos, Mapping[]]



#     cache_fp = ...
#     d = {} # type: Mapping[CacheFootprint, Set[CachedQueryInfos]]
#     qis = d[cache_fp]


#     e = {} # type: Mapping[Tuple(CachedQueryInfos, int) ]




# class PrioNode(object):
#     def __init__(self):
#         self.downstrem_keys = []
#         self.downstrem_count = collections.Counter()

# class Prio(object):

#     def __init__(self):
#         self._nodes = {}
#         self._job_count = 0
#         self._node_id_of_job = {}

#     @property
#     def job_count(self):
#         return self._job_count

#     def insert_job(self, job, prio):
#         prio_full = prio
#         del prio

#         # Retrieve node ids
#         *job_prio_keys, job_set_key = [
#             prio_full[:i]
#             for i in range(len(prio_full) + 1)
#         ]

#         # Allocate Prio nodes if missing
#         for job_prio_key, next_prio_value in zip(job_prio_keys, prio_full):
#             # Retrieve/Allocate prio node
#             if job_prio_key not in self._nodes:
#                 self._nodes[job_prio_key] = PrioNode()
#             prio_node = self._nodes[job_prio_key]

#             # Update prio node
#             assert prio_node.downstrem_count[next_prio_value] >= 0
#             prio_node.downstrem_count[next_prio_value] += 1
#             if prio_node.downstrem_count[next_prio_value] == 1:
#                 bisect.insort_left(prio_node.downstrem_keys, next_prio_value)

#         # Allocate JobSet node if missing
#         if job_set_key not in self._nodes:
#             self._nodes[job_set_key] = set()
#         job_set_node = self._nodes[job_set_key]

#         # Insert Job
#         job_set_node.add(job)
#         self._job_count += 1
#         self._node_id_of_job[job] = job_set_node

#     def remove_job(self, job):
#         # Retrieve node ids
#         prio_full = self._node_id_of_job[job]
#         *job_prio_keys, job_set_key = [
#             prio_full[:i]
#             for i in range(len(prio_full) + 1)
#         ]

#         # Clean JobSet
#         job_set = self._nodes[job_set_key]
#         job_set.remove(job)
#         if len(job_set) == 0:
#             del self._nodes[job_set_key]

#         # Clean PrioNodes
#         for job_prio_key, next_prio_value in zip(job_prio_keys, prio_full):
#             prio_node = self._nodes[job_prio_key]
#             assert prio_node.downstrem_count[next_prio_value] > 0
#             prio_node.downstrem_count[next_prio_value] -= 1
#             if prio_node.downstrem_count[next_prio_value] == 0:
#                 prio_node.downstrem_keys.remove(next_prio_value)
#             if len(prio_node.downstrem_keys) == 0:
#                 del self._nodes[job_prio_key]

#     def pop_first(self):
#         if self._job_count == 0:
#             raise ValueError('Empty PrioGraph')

#         node_key = ()
#         while True:
#             node = self._nodes[node_key]
#             if isinstance(prio_node, set):
#                 job = next(iter(node))
#                 self.remove_job(job)
#                 return job
#             node_key = node_key + (node.downstrem_keys[0],)



# class WaitingRoom(): # Read/Resample/Compute

#     def __init__(self):
#         self._jobs_of_prio = {} # type: Dict[int, Set[Job]]
#         self._prio_of_job = {} # type: Dict[Job, int]
#         self._jobs_of_qi = {} # type: Dict[CachedQueryInfos, Set[Job]]

#     def query_pulled(self, qi):
#         for job in self._jobs_of_qi[qi]:
#             old_prio = self._prio_of_job[job]
#             new_prio = job.prod_idx - qi.prod_array_pulled_count
#             # if new_prio != old_prio:
#             self._jobs_of_prio[old_prio].remove(job)
#             self._jobs_of_prio[new_prio].add(job)




#     def get_prio(self):

#         is_needed_by_a_query = ... # TODO
#         if is_needed_by_a_query:
#             most_urgent_qi, most_urgent_prod_idx = ... # TODO
#             prod_array_pulled_count = ... # TODO
#             prod_fp = self._qi[self.prod_idx]
#             cx, cy = prod_fp.c
#             return (
#                 1,
#                 most_urgent_prod_idx - prod_array_pulled_count,
#                 -cy,
#                 cx,
#                 2,
#             )
#         else:
#             return (
#                 np.inf,
#             )

# class Wait(): # Read
#     def get_prio(self):
#         prod_array_pulled_count = ... # TODO
#         prod_fp = self._qi[self.prod_idx]
#         cx, cy = prod_fp.c
#         return (
#             1,
#             self.prod_idx - prod_array_pulled_count,
#             -cy,
#             cx,
#             1,
#         )

# class Wait(): # Resample
#     def get_prio(self):
#         prod_array_pulled_count = ... # TODO
#         prod_fp = self._qi[self.prod_idx]
#         cx, cy = prod_fp.c
#         return (
#             1,
#             self.prod_idx - prod_array_pulled_count,
#             -cy,
#             cx,
#             0,
#         )


# class Work(PoolJobWorking):
#     def __init__(self, actor, cache_fp, path):
#         func = functools.partial(
#             _cache_file_check,
#             cache_fp, path, len(actor._raster), actor._raster.dtype,
#         )
#         super().__init__(actor.address, func)

#         self.cache_fp = cache_fp
#         self.path = path

def _cache_file_check(cache_fp, path, band_count, dtype):
    # TODO: Check file opening/footprint/band_count/dtype/md5
    return (True or False) == 'That is the TODO question'
