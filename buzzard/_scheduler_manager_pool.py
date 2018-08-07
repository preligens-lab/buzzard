
class ManagerPool(object):

    def __init__(self, truc, pool):
        # Constant variables
        self._pool = pool
        self._max_working = pool._processes

        # Mutable variables
        self._working_actions = []
        self._waiting_actions = []

    # ******************************************************************************************* **
    def put_in_waiting_room(self, action):
        self._waiting_actions.append(action)

    # ******************************************************************************************* **
    def list_events(self):
        return {'work-done': [
            future, action
            for future, action in self._working_actions
            if future.ready()
        ]}

    def update_states(self, events):
        for future, action in events['work-done']:
            res = future.get()
            self._working_actions.remove(action)
            action.done(res) # Put in next waiting room?

    def take_actions(self):
        waiting_count = len(self._waiting_actions)
        working_count = len(self._working_actions)
        first = True
0
        while waiting_count > 0 and working_count < self._max_working:
            if first:
                self._waiting_actions = sorted(self._waiting_actions)
                first = False

            action = self._waiting_actions.pop(0)
            future = self._pool.apply_async(action.get_worker_fn())
            self._working_actions.append((future, action))

            working_count += 1
            waiting_count -= 1

    # ******************************************************************************************* **
