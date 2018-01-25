""">>> help(CallOrContext)"""

class CallOrContext(object):
    """Private helper class to provide a common behaviour both on call and on exit"""
    def __init__(self, obj, routine):
        self._obj = obj
        self._routine = routine

    def __call__(self):
        self._routine()

    def __enter__(self):
        return self._obj

    def __exit__(self, *args, **kwargs):
        self._routine()
