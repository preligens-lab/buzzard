import collections

from buzzard._actors.message import Msg
from buzzard._actors.pool_waiting_room import ActorPoolWaitingRoom
from buzzard._actors.pool_working_room import ActorPoolWorkingRoom
from buzzard._actors.global_priorities_watcher import ActorGlobalPrioritiesWatcher

class ActorTopLevel(object):
    """Actor that takes care of the lifetime of all other actors.

    This is the only actor that is instantiated by the scheduler. All other actors are
    instantiated from here.

    This class does not implement a `die` method, since destroying this actor is the same event
    as stopping the scheduler's loop. If a destruction is ever needed, call a die method from
    the scheduler using the `top_level_actor` variable.
    """
    def __init__(self):
        self._rasters = set()
        self._rasters_per_pool = collections.defaultdict(list)

        self._actor_addresses_of_raster = {}
        self._actor_addresses_of_pool = {}

        self._primed = False
        self._alive = True

    address = '/Global/TopLevel'

    @property
    def alive(self):
        return self._alive

    # ******************************************************************************************* **
    def ext_receive_prime(self):
        """Receive message sent by something else than an actor, still treated synchronously: Prime
        yourself, we are about to start the show!
        """
        assert not self._primed
        self._primed = True
        return [
            ActorGlobalPrioritiesWatcher()
        ]

    def ext_receive_new_raster(self, raster):
        """Receive message sent by something else than an actor, still treated synchronously: There
        is a new raster

        Parameter
        ---------
        raster: _a_recipe_raster.ABackRecipeRaster
        """
        msgs = []
        self._rasters.add(raster)
        raster.debug_mngr.event('raster_started', raster.facade_proxy)

        # Instantiate raster's actors ******************************************
        actors = raster.create_actors()
        msgs += actors
        self._actor_addresses_of_raster[raster] = [
            actor.address
            for actor in actors
        ]
        del actors

        # Instantiate pools' actors ********************************************
        pools = {
            id(pool): pool
            for attr in [
                'computation_pool', 'merge_pool', 'io_pool', 'resample_pool',
            ]
            if hasattr(raster, attr)
            for pool in [getattr(raster, attr)]
            if pool is not None
        }
        for pool_id, pool in pools.items():
            if pool_id not in self._rasters_per_pool:
                actors = [
                    ActorPoolWaitingRoom(pool),
                    ActorPoolWorkingRoom(pool),
                ]
                msgs += actors

                self._actor_addresses_of_pool[pool_id] = [
                    actor.address
                    for actor in actors
                ]

            self._rasters_per_pool[pool_id].append(raster)

        return msgs

    def ext_receive_kill_raster(self, raster):
        """Receive message sent by something else than an actor, still treated synchronously: An
        actor is closing

        Parameter
        ---------
        raster: _a_recipe_raster.ABackRecipeRaster
        """
        msgs = []
        self._rasters.remove(raster)
        raster.debug_mngr.event('raster_stopped', raster.facade_proxy)

        # Deleting raster's actors *********************************************
        # Deal with QueriesHandler first.
        # TODO: Should the order of 'die' messages be enforced somewhere else?
        msgs += [
            Msg(address, 'die')
            for address in sorted(
                    self._actor_addresses_of_raster[raster],
                    key=lambda address: 'QueriesHandler' not in address,
            )
        ]
        del self._actor_addresses_of_raster[raster]

        # Deleting pools' actors ***********************************************
        pools = {
            id(pool): pool
            for attr in [
                'computation_pool', 'merge_pool', 'io_pool', 'resample_pool',
            ]
            if hasattr(raster, attr)
            for pool in [getattr(raster, attr)]
            if pool is not None
        }
        for pool_id in pools.keys():
            self._rasters_per_pool[pool_id].remove(raster)
            if len(self._rasters_per_pool[pool_id]) == 0:
                del self._rasters_per_pool[pool_id]
                msgs += [
                    Msg(actor_adress, 'die')
                    for actor_adress in self._actor_addresses_of_pool[pool_id]
                ]
                del self._actor_addresses_of_pool[pool_id]

        return msgs

    # ******************************************************************************************* **
