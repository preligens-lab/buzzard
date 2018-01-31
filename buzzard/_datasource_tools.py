""">>> help(DataSourceToolsMixin)"""

class DataSourceToolsMixin(object):
    """Private mixin for the DataSource class containing subroutines for proxies registration"""

    def __init__(self):
        self._keys_of_proxy = {}
        self._proxy_of_key = {}

    def _validate_key(self, key):
        hash(key)
        if key in self.__dict__:
            raise ValueError('key `{}` is already bound'.format(key)) # pragma: no cover

    def _register(self, keys, prox):
        self._keys_of_proxy[prox] = keys
        for key in keys:
            self._proxy_of_key[key] = prox
            self.__dict__[key] = prox

    def _unregister(self, proxy):
        for key in self._keys_of_proxy[proxy]:
            del self.__dict__[key]
            del self._proxy_of_key[key]
        del self._keys_of_proxy[proxy]
