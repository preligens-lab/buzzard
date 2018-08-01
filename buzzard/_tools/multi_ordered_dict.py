import collections
import uuid
import itertools

class MultiOrderedDict(object):
    """Data structure derived from collections.OrderedDict that accept several keys"""

    def __init__(self):
        self._od = collections.OrderedDict()
        self._key_of_ukey = {}
        self._ukeys_of_key = collections.defaultdict(list)

    def __str__(self):
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
        ukey = uuid.uuid4()
        self._ukeys_of_key[key].insert(0, ukey)
        self._key_of_ukey[ukey] = key
        self._od[ukey] = value

    def pop_first_occurrence(self, key):
        if key not in self:
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
        if key not in self:
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

class _MultiOrderedDict_NSquared(object):
    """Class with the same specifications as MultiOrderedDict but with a simpler and less effective
    implementation. It exsits for unit testing purposes"""

    def __init__(self):
        self._l = list()

    def __len__(self):
        return len(self._l)

    def __contains__(self, key):
        for k, v in self._l:
            if k == key:
                return True
        else:
            return False

    def count(self, key):
        return sum(
            key == k
            for k, _ in self._l
        )

    def pop_back(self):
        if len(self._l) == 0:
            collections.OrderedDict().popitem(last=True)
        return self._l.pop(-1)

    def push_front(self, key, value):
        self._l.insert(0, (key, value))

    def pop_first_occurrence(self, key):
        for i, (k, v) in enumerate(self._l):
            if k == key:
                break
        else:
            raise KeyError('{} not in MultiOrderedDict'.format(key))
        self._l.pop(i)
        return v

    def pop_last_occurrence(self, key):
        for i, (k, v) in list(enumerate(self._l))[::-1]:
            if k == key:
                break
        else:
            raise KeyError('{} not in MultiOrderedDict'.format(key))
        self._l.pop(i)
        return v

    def pop_all_occurrences(self, key):
        indices = []
        res = []
        for i, (k, v) in enumerate(self._l):
            if k == key:
                indices.append(i)
        for i in sorted(indices, reverse=True):
            res.append(self._l.pop(i)[1])
        return res

if __name__ == '__main__':
    # Stochastic test that proves that MultiOrderedDict behaves like _MultiOrderedDict_NSquared
    # Since _MultiOrderedDict_NSquared implementation is straightforward we can trust it, and
    # transfer this confidence to MultiOrderedDict using this test.
    import numpy as np
    from tqdm import tqdm
    import weakref
    import gc

    class Value():
        def __init__(self, i, h):
            self.i = i
            self.h = h

        def __eq__(self, other):
            return self.i == other.i

        def __hash__(self):
            return self.h.int

        def __str__(self):
            return '<{}, {}>'.format(
                self.i, self.h
            )

        def __repr__(self):
            return str(self)

    def _assert(ref_res, test_res):
        if isinstance(ref_res, Exception):
            assert isinstance(test_res, Exception), (ref_res, test_res, ref._l)
            assert type(ref_res) is type(test_res) and ref_res.args == test_res.args, (ref_res, test_res, ref._l)
        else:
            assert ref_res == test_res, (ref_res, test_res, ref._l)

    def _assert_collection():
        if len(ref_wset) != len(test_wset):
            gc.collect()
        assert ref_wset == test_wset, (set(ref_wset), set(test_wset))

    def _testa():
        k = rng.randint(0, 5)
        v = rng.randint(9999)
        h = uuid.uuid4()

        vref = Value(v, h)
        vtest = Value(v, h)
        assert vref == vtest

        if verbose:
            print(f'{"push_front":>20}: k:{k:5} v:{v:5}')

        ref.push_front(k, vref)
        test.push_front(k, vtest)

        ref_wset.add(vref)
        test_wset.add(vtest)
        _assert_collection()

    def _testb():
        try:
            ref_res = ref.pop_back()
        except Exception as e:
            ref_res = e
        try:
            test_res = test.pop_back()
        except Exception as e:
            test_res = e

        if verbose:
            if isinstance(test_res, Exception):
                print(f'{"pop_back":>20}:                     {test_res!r}')
            else:
                k, v = test_res
                print('{:>20}: k:{:5} v:{!s:5}'.format("pop_back", k, v))
        _assert(ref_res, test_res)

    def _testc():
        k = rng.randint(0, 5)
        try:
            ref_res = ref.pop_first_occurrence(k)
        except Exception as e:
            ref_res = e
        try:
            test_res = test.pop_first_occurrence(k)
        except Exception as e:
            test_res = e

        if verbose:
            if isinstance(test_res, Exception):
                print('{:>20}:                     {!r}'.format("pop_first_occurrence", test_res))
            else:
                print('{:>20}: k:{:5} v:{!s:5}'.format("pop_first_occurrence", k, test_res))
        _assert(ref_res, test_res)
        _assert_collection()

    def _testd():
        k = rng.randint(0, 5)
        try:
            ref_res = ref.pop_last_occurrence(k)
        except Exception as e:
            ref_res = e
        try:
            test_res = test.pop_last_occurrence(k)
        except Exception as e:
            test_res = e

        if verbose:
            if isinstance(test_res, Exception):
                print('{:>20}:                     {!r}'.format("pop_last_occurrence", test_res))
            else:
                print('{:>20}: k:{:5} v:{!s:5}'.format("pop_last_occurrence", k, test_res))
        _assert(ref_res, test_res)
        _assert_collection()

    def _teste():
        k = rng.randint(0, 5)
        assert (k in ref) == (k in test)

    def _testf():
        assert len(ref) == len(test)

    def _testg():
        k = rng.randint(0, 5)
        try:
            ref_res = ref.pop_all_occurrences(k)
        except Exception as e:
            ref_res = e
        try:
            test_res = test.pop_all_occurrences(k)
        except Exception as e:
            test_res = e

        if verbose:
            if isinstance(test_res, Exception):
                print('{:>20}:                     {!r}'.format("pop_all_occurrences", test_res))
            else:
                print('{:>20}: k:{:5} v:{!s:5}'.format("pop_all_occurrences", k, test_res))
        _assert(ref_res, test_res)
        _assert_collection()

    def _testh():
        k = rng.randint(0, 5)
        assert ref.count(k) == test.count(k)

    tests = [
        _testa,
        _testb,
        _testc,

        _testd,
        _teste,
        _testf,

        _testg,
        _testh,
    ]


    ref = _MultiOrderedDict_NSquared()
    test = MultiOrderedDict()

    ref_wset = weakref.WeakSet()
    test_wset = weakref.WeakSet()

    # verbose = True
    verbose = False
    seed = np.random.randint(9999)
    seed = 3877
    print('seed is', seed)
    rng = np.random.RandomState(seed)

    for _ in tqdm(range(100000)):
        i = rng.randint(0, len(tests))
        tests[i]()
