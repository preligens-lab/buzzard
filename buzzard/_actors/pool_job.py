# Waiting *************************************************************************************** **
class PoolJobWaiting:
    def __init__(self, sender_address):
        self.sender_address = sender_address

class MaxPrioJobWaiting(PoolJobWaiting):
    pass

class ProductionJobWaiting(PoolJobWaiting):
    def __init__(self, sender_address, qi, prod_idx, action_priority, fp):
        super().__init__(sender_address)
        self.fp = fp
        self.qi = qi
        self.prod_idx = prod_idx
        self.action_priority = action_priority

class CacheJobWaiting(PoolJobWaiting):
    def __init__(self, sender_address, raster_uid, cache_fp, action_priority, fp):
        super().__init__(sender_address)
        self.fp = fp
        self.raster_uid = raster_uid
        self.cache_fp = cache_fp
        self.action_priority = action_priority

# Working *************************************************************************************** **
class PoolJobWorking(object):
    __slots__ = ['sender_address', 'func']

    def __init__(self, sender_address, func):
        self.sender_address = sender_address
        self.func = func
