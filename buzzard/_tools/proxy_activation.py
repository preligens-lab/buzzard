import functools

def ensure_activated(f):
    """Activate a proxy before method call"""
    @functools.wraps(f)
    def g(that, *args, **kwargs):
        that.activate()
        return f(that, *args, **kwargs)
    return g

def ensure_activated_iteration(f):
    """Activate a proxy before method call and force it to stay opened until
    - iteration is over
    - iterator is collected by gc
    """
    @functools.wraps(f)
    def g(that, *args, **kwargs):
        that._lock_activate()
        try:
            it = f(that, *args, **kwargs)
            for v in it:
                yield v
        finally:
            that._unlock_activate()
    return g
