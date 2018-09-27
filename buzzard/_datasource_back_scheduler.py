import collections
import time

from buzzard._actors.top_level import ActorTopLevel
from buzzard._actors.message import Msg, DroppableMsg

class BackDataSourceSchedulerMixin(object):

    def __init__(self, ):
        pass

    def _scheduler_loop_until_datasource_close(self):

        def _register_actor(a):
            if hasattr(a, 'ext_receive_nothing'):
                keep_alive_actors.append(a)

            address = a.address
            actors[address] = a

            grp_name, name = address.split('/')
            assert name not in actors[grp_name]
            actors[grp_name][name] = a

        def _find_actor(self, address, relative_actor):
            names = address.split('/')
            if len(names) == 2:
                return actors[names[0]].get(names[1])
            elif len(names) == 1:
                grp_name = relative_actor.address.split('/')[0]
                return actors[grp_name].get(names[0])
            else:
                assert False

        def _unregister_actor(a):


        actors = collections.defaultdict(dict)
        keep_alive_actors = [] # Should never be empty

        top_level_actor = ActorTopLevel()
        piles_of_msgs = [
            (top_level_actor, top_level_actor.ext_receive_prime())
        ] # type: List[Tuple[Actor, List[Union[Msg, Actor]]]]
        keep_alive_iterator = _cycle_list(keep_alive_actors)

        while True:
            # Step 1: Process all messages on flight
            while piles_of_msgs:
                msgs = piles_of_msgs[-1]
                if not msgs:
                    del piles_of_msgs[-1]
                    continue
                src_actor, msg = msgs.pop(-1)
                if isinstance(msg, Msg):
                    dst_actor = _find_actor(msg.address, src_actor)
                    if dst_actor is None:
                        assert isinstance(msg, DroppableMsg)
                    else:
                        new_msgs = getattr(dst_actor, 'receive_' + msg.title)(*msg.args)
                        if new_msgs:
                            piles_of_msgs.append((
                                dst_actor, new_msgs
                            ))
                else:
                    _register_actor(msg)

            # Step 2: Find a "keep alive" actor that has messages to send
            assert len(keep_alive_actors) != 0
            for actor, _ in zip(keep_alive_iterator, range(len(keep_alive_actors))):
                # Iter at most once on each "keep alive" actor
                new_msgs = actor.ext_receive_nothing()
                if not actor.alive:

                if new_msgs:
                    piles_of_msgs.append((
                        actor, new_msgs
                    ))
                    break

            # Step 3: Sleep if no messages from step 2
            if not piles_of_msgs:
                time.sleep(1 / 20)

def _cycle_list(l):
    """Loop in a list forever, even if its size changes. Error if empty."""
    i = -1
    while True:
        i = (i + 1) % len(l)
        yield l[i]
