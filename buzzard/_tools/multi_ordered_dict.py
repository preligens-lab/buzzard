import collections
import itertools

class MultiOrderedDict(object):
    """Data structure derived from collections.OrderedDict that accept several keys"""

    def __init__(self):
        self._od = collections.OrderedDict()
        self._key_of_ukey = {}
        self._ukeys_of_key = collections.defaultdict(list)
        self._i = 0

    def __str__(self): # pragma: no cover
        l = []
        for key, g in itertools.groupby(map(self._key_of_ukey.get, self._od.keys())):
            l += ['{} x{}'.format(
                key, len(list(g))
            )]
        return '<' + ', '.join(l) + '>'

    def __contains__(self, key):
        return key in self._ukeys_of_key

    def __len__(self):
        return len(self._od)

    def count(self, key):
        m = len(self._ukeys_of_key[key])
        if m == 0:
            del self._ukeys_of_key[key]
        return m

    def pop_back(self):
        ukey, value = self._od.popitem(last=False)

        key = self._key_of_ukey[ukey]

        ukey_list = self._ukeys_of_key[key]
        assert len(ukey_list) > 0
        assert ukey_list[-1] == ukey
        ukey_list.pop(-1)

        del self._key_of_ukey[ukey]
        if len(ukey_list) == 0:
            del self._ukeys_of_key[key]

        return key, value

    def push_front(self, key, value):
        ukey = self._i
        self._i += 1
        self._ukeys_of_key[key].insert(0, ukey)
        self._key_of_ukey[ukey] = key
        self._od[ukey] = value

    def pop_first_occurrence(self, key):
        if key not in self: # pragma: no cover
            raise KeyError('{} not in MultiOrderedDict'.format(key))

        ukey_list = self._ukeys_of_key[key]
        assert len(ukey_list) > 0
        ukey = ukey_list.pop(0)

        value = self._od[ukey]

        del self._od[ukey]
        del self._key_of_ukey[ukey]
        if len(ukey_list) == 0:
            del self._ukeys_of_key[key]

        return value

    def pop_last_occurrence(self, key):
        if key not in self: # pragma: no cover
            raise KeyError('{} not in MultiOrderedDict'.format(key))

        ukey_list = self._ukeys_of_key[key]
        assert len(ukey_list) > 0
        ukey = ukey_list.pop(-1)

        value = self._od[ukey]

        del self._od[ukey]
        del self._key_of_ukey[ukey]
        if len(ukey_list) == 0:
            del self._ukeys_of_key[key]

        return value

    def pop_all_occurrences(self, key):
        if key not in self:
            return []

        ukey_list = self._ukeys_of_key[key]

        res = [
            self._od[ukey]
            for ukey in reversed(ukey_list)
        ]
        for ukey in ukey_list:
            del self._od[ukey]
            del self._key_of_ukey[ukey]
        del self._ukeys_of_key[key]

        return res
