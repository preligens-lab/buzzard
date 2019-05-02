# Waiting *************************************************************************************** **
class PoolJobWaiting:
    """Base class of all waiting jobs.

    A waiting job has a priority, it waits for a token in a PoolWaitingRoom with other waiting jobs.
    This token allows a waiting job to become a working job, and go to the PoolWorkingRoom to
    get some computations done.
    """
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
    """Base class of all working jobs.

    A working job paired with a token from a PoolWaitingRoom can be fed to a PoolWorkingRoom to
    compute things on the wrapped thread/process pool.
    """
    __slots__ = ['sender_address', 'func']

    def __init__(self, sender_address, func):
        self.sender_address = sender_address
        self.func = func
