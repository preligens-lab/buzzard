
class Msg(object):

    def __init__(self, address, title, *args):
        self.address = address
        self.title = title
        self.args = args
