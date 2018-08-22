import collections

from buzzard._actors._pool_waiting_room import import ActorPoolWaitingRoom
from buzzard._actors._pool_working_room import import ActorPoolWorkingRoom

class ActorRastersHandler(object):
    """Actor that takes care of the lifetime of rasters' and pools' actors"""
    def __init__(self):
        self._rasters = set()
        self._actor_addresses_of_raster = {}
        self._rasters_of_pool = collections.defaultdict(list)
        self._actor_addresses_of_pool = {}

    address = '/RastersHandler'

    # ******************************************************************************************* **
    def ext_receive_new_raster(self, raster):
        """Receive message sent by something else than an actor, still treated synchronously: There
        is a new raster
        """
        msgs = []
        self._rasters.add(raster)

        # Instanciate raster's actors ******************************************
        actors = raster.create_actors()
        msgs += actors
        self._actor_addresses_of_raster[raster] = [
            actor.address
            for actor in actors
        ]
        del actors

        # Instanciate pools' actors ********************************************
        pools = {
            id(pool): pool
            for attr in [
                'computation_pool', 'merge_pool', 'write_pool',
                'file_checker_pool', 'read_pool', 'resample_pool',
            ]
            for pool in [getattr(raster, attr)]
        }
        for pool_id, pool in pools.items():
            if pool_id not in self._rasters_of_pool:
                actors = self._create_pool_actors(pool)
                msgs += actors

                self._actor_addresses_of_pool[pool_id] = [
                    actor.address
                    for actor in actors
                ]

            self._rasters_of_pool.append(raster)

        return msgs

    def ext_receive_kill_raster(self, raster):
        """Receive message sent by something else than an actor, still treated synchronously: An
        actor is closing
        """
        msgs = []
        self._rasters.remove(raster)

        # Deleting raster's actors *********************************************
        msgs += [
            Msg(address, 'die')
            for address in self._actor_addresses_of_raster[actor]
        ]
        del self._actor_addresses_of_raster[actor]

        # Deleting pools' actors ***********************************************
        pools = {
            id(pool): pool
            for attr in [
                'computation_pool', 'merge_pool', 'write_pool',
                'file_checker_pool', 'read_pool', 'resample_pool',
            ]
            for pool in [getattr(raster, attr)]
        }
        for pool_id, pool in pools.items():
            self._rasters_of_pool[pool_id].remove(raster)
            if len(self._rasters_of_pool) == 0:
                del self._rasters_of_pool[pool_id]
                msgs += [
                    actor.address
                    for actor in self._actor_addresses_of_pool[pool_id]
                ]
                del self._actor_addresses_of_pool[pool_id]

        return msgs

    # ******************************************************************************************* **
    def _create_pool_actors(self, pool):
        return [
            ActorPoolWaitingRoom(pool),
            ActorPoolWorkingRoom(pool),
        ]

    # ******************************************************************************************* **
