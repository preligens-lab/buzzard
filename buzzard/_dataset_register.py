import threading

class DatasetRegisterMixin(object):
    """Private mixin for the Dataset class containing subroutines for proxies registration"""

    def __init__(self, **kwargs):
        self._keys_of_source = {}
        self._source_of_key = {}
        self._register_lock = threading.Lock()
        super(DatasetRegisterMixin, self).__init__(**kwargs)

    def _register(self, keys, prox):
        with self._register_lock:
            for key in keys:
                if key in self.__dict__: # pragma: no cover
                    raise ValueError('key `{}` is already bound'.format(key))
            self._keys_of_source[prox] = keys
            for key in keys:
                self._source_of_key[key] = prox
                self.__dict__[key] = prox

    def _unregister(self, source):
        for key in self._keys_of_source[source]:
            del self.__dict__[key]
            del self._source_of_key[key]
        del self._keys_of_source[source]
