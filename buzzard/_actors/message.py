
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

class DroppableMsg(Msg):
    pass
