
class ActorRastersHandler(object):

    def __init__(self):
        self.rasters = set()
        self.actor_addresses_of_raster = {}

    address = '/RastersHandler'

    # ******************************************************************************************* **
    def receive_external_new_raster(self, raster):
        """Receive message sent by other thread: There is a new raster"""
        msgs = []
        self.rasters.add(raster)
        actors = raster.create_actors()
        self.actor_addresses_of_raster[raster] = [
            actor.address
            for actor in actors
        ]
        msgs += actors
        return msgs

    def receive_external_kill_raster(self, raster):
        """Receive message sent by other thread: A raster was closed"""
        msgs = [
            Msg(address, 'die')
            for address in self.actor_addresses_of_raster[actor]
        ]
        del self.actor_addresses_of_raster[actor]
        self.rasters.remove(raster)

        return msgs

    # ******************************************************************************************* **
