import collections
import time
import threading

from buzzard._actors.top_level import ActorTopLevel
from buzzard._actors.message import Msg, DroppableMsg

class BackDataSourceSchedulerMixin(object):

    def __init__(self, ds_id, **kwargs):
        self._ext_message_to_scheduler_queue = []
        self._thread = None
        self._thread_exn = None
        self._ds_id = ds_id
        self._stop = False
        self._started = False
        super().__init__(**kwargs)

    # Public methods **************************************************************************** **
    def ensure_scheduler_living(self):
        if self._thread is None:
            self._thread = threading.Thread(
                target=self._exception_catcher,
                name='DataSource{:#x}Scheduler'.format(self._ds_id),
                daemon=True,
            )
            self._thread.start()
        else:
            self.ensure_scheduler_still_alive()

    def ensure_scheduler_still_alive(self):
        if not self._thread.isAlive():
            raise self._thread_exn

    def put_message(self, msg):
        self.ensure_scheduler_living()
        # a list is thread-safe: https://stackoverflow.com/a/6319267/4952173
        self._ext_message_to_scheduler_queue.append(msg)

    def stop_scheduler(self):
        assert not self._stop
        self._stop = True

    # Private methods *************************************************************************** **
    def _exception_catcher(self):
        try:
            self._scheduler_loop_until_datasource_close()
        except Exception as e:
            self._thread_exn = e
            raise

    def _scheduler_loop_until_datasource_close(self):
        """This is the entry point of a DataSource's scheduler.
        The design of this method would be much better with recursive calls, but much slower too. (maybe)
        """

        def _register_actor(a):
            if hasattr(a, 'ext_receive_nothing'):
                keep_alive_actors.append(a)

            address = a.address
            actors[address] = a

            grp_name, name = address.split('/')
            assert name not in actors[grp_name]
            actors[grp_name][name] = a

        def _find_actor(address, relative_actor):
            names = address.split('/')
            if len(names) == 2:
                return actors[names[0]].get(names[1])
            elif len(names) == 1:
                grp_name = relative_actor.address.split('/')[0]
                return actors[grp_name].get(names[0])
            else:
                assert False

        def _unregister_actor(a):
            address = a.address
            grp_name, name = address.split('/')
            del actors[grp_name][name]
            if not actors[grp_name]:
                del actors[grp_name]
            if hasattr(a, 'ext_receive_nothing'):
                keep_alive_actors.remove(a)

        # Dicts of actors
        actors = collections.defaultdict(dict) # type: Mapping[str, Mapping[str, Actor]]

        # List of actors that need to be kept alive with calls to `ext_receive_nothing`
        # `keep_alive_iterator` should never be iterated if `keep_alive_actors` is empty
        keep_alive_actors = []
        keep_alive_iterator = _cycle_list(keep_alive_actors)

        # Stack of pending messages
        piles_of_msgs = [] # type: List[Tuple[Actor, List[Union[Msg, Actor]]]]

        # Instanciate and register the top level actor
        top_level_actor = ActorTopLevel()
        _register_actor(top_level_actor)
        piles_of_msgs.append(
            (top_level_actor, top_level_actor.ext_receive_prime()),
        )

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
                        # This message may be discarted
                        assert isinstance(msg, DroppableMsg)
                    else:
                        new_msgs = getattr(dst_actor, 'receive_' + msg.title)(*msg.args)
                        if self._stop:
                            # DataSource is closing. This is the same as `step 5`. (optimisation purposes)
                            return
                        if not dst_actor.alive:
                            # Actor is closing
                            _unregister_actor(dst_actor)
                        if new_msgs:
                            # Message need to be sent
                            piles_of_msgs.append((
                                dst_actor, new_msgs
                            ))
                else:
                    _register_actor(msg)

            # Step 2: Receive external messages
            # a list is thread-safe: https://stackoverflow.com/a/6319267/4952173
            if self._ext_message_to_scheduler_queue:
                msg = self._ext_message_to_scheduler_queue.pop(0)
                piles_of_msgs.append((
                    _find_actor(msg, None), [msg]
                ))

            # Step 3: If no messages from phase 2 and some `keep_alive_actors`
            #   Find "keep alive" actors that need to be closed
            #   Find a "keep alive" actor that has messages to send
            if keep_alive_actors and not piles_of_msgs:
                actors_to_remove = []
                for actor, _ in zip(keep_alive_iterator, range(len(keep_alive_actors))):
                    # Iter at most once on each "keep alive" actor
                    new_msgs = actor.ext_receive_nothing()
                    if self._stop:
                        # DataSource is closing. This is the same as `step 5`. (optimisation purposes)
                        return
                    if not actor.alive:
                        # Actor is closing
                        actors_to_remove.append(actor)
                    if new_msgs:
                        # Messages need to be sent
                        piles_of_msgs.append((
                            actor, new_msgs
                        ))
                        break
                for actor in actors_to_remove:
                    _unregister_actor(actor)

            # Step 4: If no messages from phase 2 nor from phase 3
            #   Sleep
            if not piles_of_msgs:
                time.sleep(1 / 20)

            # Step 5: Check if DataSource was collected
            if self._stop:
                return


def _cycle_list(l):
    """Loop in a list forever, even if its size changes. Error if empty."""
    i = -1
    while True:
        i = (i + 1) % len(l)
        yield l[i]
