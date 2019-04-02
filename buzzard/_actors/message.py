import sys
import uuid

import numpy as np

class Msg(object):
    """Message exchanged:
    - between two actors (receiver method prefixed by `receive_`)
    - from scheduler to actor (receiver method prefixed by `ext_receive_`)
    - from user's thread to scheduler to actor (receiver method also prefixed by `ext_receive_`)
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

    def __str__(self): # pragma: no cover
        if self.address[0] == '/':
            b = _COLOR_PER_CLASSNAME.get(self.address.split('/')[2], '\033[37m')
        else:
            b = _COLOR_PER_CLASSNAME.get(self.address, '\033[37m')

        def _dump_param(a):

            # if isinstance(a, str):
                # return '"{}"'.format(a)
            if isinstance(a, (int, uuid.UUID)):
                return str(a)
            elif isinstance(a, (np.ndarray)):
                return '{}{}'.format(a.dtype, a.shape)
            elif isinstance(a, (tuple, list)):
                types = list(set(type(b).__name__ for b in a))
                types = '|'.join(types)
                s = '[{}]*{}'.format(types, len(a))
                t = '[{}]'.format(', '.join(map(_dump_param, a)))
                return min([t, s], key=len)
            elif type(a).__name__ == 'CachedQueryInfos':
                return 'qi:{:#x}'.format(id(a))
            elif type(a).__name__ == 'Footprint':
                return 'Footprint{:#18x}'.format(hash(a) % ((sys.maxsize + 1) * 2))
            else:
                s = type(a).__name__
                t = str(a)
                return min([t, s], key=len)

        return '{u}{b}{letter}{}{z}.{}({}){u}{b}{letter}{z}'.format(
            self.address,
            self.title,
            ', '.join(_dump_param(a) for a in self.args),
            letter=u"\u2709",
            u='\033[4m',
            b=b,
            z='\033[0m'
        )

class DroppableMsg(Msg):
    pass

class AgingMsg(Msg):
    def __init__(self, address, title, id_args, other_args):
        self.id_args = id_args
        super().__init__(address, title, *(list(id_args) + list(other_args)))

_COLOR_PER_CLASSNAME = {
    'TopLevel': '\033[37m',
    'GlobalPrioritiesWatcher': '\033[37m',

    'WaitingRoom': '\033[36m',
    'WorkingRoom': '\033[36m',

    'ProductionGate': '\033[32m',
    'Producer': '\033[32m',
    'Resampler': '\033[32m',
    'CacheExtractor': '\033[32m',
    'Reader': '\033[32m',

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
