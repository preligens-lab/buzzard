
class Msg(object):
    """Message exchanged:
    - between two actors (receiver method prefixed by `receive_`)
    - from scheduler to actor (receiver method prefixed by `ext_receive_`)
    - from user's thread to scheduler to actor (receiver method prefixed by `ext_receive_`)
    """
    __slots__ = ['address', 'title', 'args']

    def __init__(self, address, title, *args):
        """
        Parameters
        ----------
        address: str
            Destination of the message.
            Relative or Absolute address
        title: str
            Method to call
        *args: sequence of object
            Arguments for the method call
        """
        self.address = address
        self.title = title
        self.args = args

    def __str__(self):
        return '{u}{b}{letter}{}{z}.{}({}){u}{b}{letter}{z}'.format(
            self.address,
            self.title,
            ', '.join(type(a).__name__ for a in self.args),
            letter=u"\u2709",
            # a='\033[36m',
            u='\033[4m',

            b=_COLOR_PER_CLASSNAME.get(self.address.split('/')[2], '\033[37m'),
            z='\033[0m'
        )

class DroppableMsg(Msg):
    pass

_COLOR_PER_CLASSNAME = {
    'TopLevel': '\033[37m',
    'GlobalPrioritiesWatcher': '\033[37m',

    'PoolWaitingRoom': '\033[31m',
    'PoolWorkingRoom': '\033[31m',

    'ProductionGate': '\033[31m',
    'Producer': '\033[31m',
    'Resampler': '\033[31m',
    'CacheExtractor': '\033[31m',
    'Reader': '\033[31m',

    'QueriesHandler': '\033[33m',
    'CacheSupervisor': '\033[33m',
    'FileChecker': '\033[33m',

    'ComputationAccumulator': '\033[35m',
    'Computer': '\033[35m',
    'Merger': '\033[35m',
    'Writer': '\033[35m',
    'ComputationGate1': '\033[35m',
    'ComputationGate2': '\033[35m',
}
