"""
Cooperative single-threaded actor model
"""
from _actor_computation import ActorMixinComputation
from _actor_merge import ActorMixinMerge
from _actor_write import ActorMixinWrite

class ActorPool(
        ActorMixinComputation,
        ActorMixinMerge,
        ActorMixinWrite,
):
    """This actor may be used multiple times as virtually different actors. This way the waiting
    room is shared amongst different actors.

    Works are not scheduled right away in the pool to avoid massive trafic jams. They instead
    wait in a waiting room and are scheduled one by one.
    """

    def __init__(self, pool):
        self._pool = pool
        self._working = []
        self._working_count = 0
        self._waiting = []

    def receive_nothing(self):
        msgs = []
        for future, then in self._working:
            if future.ready():
                msgs += then(future.result())
        return msgs
