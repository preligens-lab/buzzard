# Waiting *************************************************************************************** **
class PoolJobWaiting:
    def __init__(self, sender_address):
        self.sender_address = sender_address

class MaxPrioJobWaiting(PoolJobWaiting):
    pass

class ProductionJobWaiting(PoolJobWaiting):
    def __init__(self, sender_address, qi, prod_idx, action_priority):
        super().__init__(sender_address)
        self.qi = qi
        self.prod_idx = prod_idx
        self.action_priority = action_priority

class CacheJobWaiting(PoolJobWaiting):
    def __init__(self, sender_address, raster_uid, cache_fp, action_priority):
        super().__init__(sender_address)
        self.raster_uid = raster_uid
        self.cache_fp = cache_fp
        self.action_priority = action_priority

# Working *************************************************************************************** **
class PoolJobWorking(object):
    def __init__(self, sender_address, func):
        self.sender_address = sender_address
        self.func = func
