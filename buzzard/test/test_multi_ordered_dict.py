""" Stochastic test that proves that MultiOrderedDict behaves like _MultiOrderedDict_NSquared
Since _MultiOrderedDict_NSquared implementation is straightforward we can trust it, and
transfer this confidence to MultiOrderedDict using this test.
"""

import weakref
import gc
import collections
import uuid
import itertools

import pytest
import numpy as np

from buzzard._tools import MultiOrderedDict

class _MultiOrderedDict_NSquared(object):
    """Class with the same specifications as MultiOrderedDict but with a simpler and less effective
    implementation. It exsits for unit testing purposes"""

    def __init__(self):
        self._l = list()

    def __len__(self):
        return len(self._l)

    def __contains__(self, key):
        for k, _ in self._l:
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


def _assert(ref, ref_res, test_res):
    if isinstance(ref_res, Exception):
        assert isinstance(test_res, Exception), (ref_res, test_res, ref._l)
        assert type(ref_res) is type(test_res) and ref_res.args == test_res.args, (ref_res, test_res, ref._l)
    else:
        assert ref_res == test_res, (ref_res, test_res, ref._l)

def _assert_collection(ref_wset, test_wset):
    if len(ref_wset) != len(test_wset):
        gc.collect()
    assert ref_wset == test_wset, (set(ref_wset), set(test_wset))

def _test_a(ref, test, ref_wset, test_wset, rng, verbose=False):
    k = rng.randint(0, 5)
    v = rng.randint(9999)
    h = uuid.uuid4()

    vref = Value(v, h)
    vtest = Value(v, h)
    assert vref == vtest

    if verbose:
        print('{:>20}: k:{:5} v:{:5}'.format("push_front", k, v))

    ref.push_front(k, vref)
    test.push_front(k, vtest)

    ref_wset.add(vref)
    test_wset.add(vtest)
    _assert_collection(ref_wset, test_wset)

def _test_b(ref, test, _ref_wset, _test_wset, _rng, verbose=False):
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
            print('{:>20}:                     {!r}'.format("pop_back", test_res))
        else:
            k, v = test_res
            print('{:>20}: k:{:5} v:{!s:5}'.format("pop_back", k, v))
    _assert(ref, ref_res, test_res)

def _test_c(ref, test, ref_wset, test_wset, rng, verbose=False):
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
    _assert(ref, ref_res, test_res)
    _assert_collection(ref_wset, test_wset)

def _test_d(ref, test, ref_wset, test_wset, rng, verbose=False):
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
    _assert(ref, ref_res, test_res)
    _assert_collection(ref_wset, test_wset)

def _test_e(ref, test, _ref_wset, _test_wset, rng, _verbose=False):
    k = rng.randint(0, 5)
    assert (k in ref) == (k in test)

def _test_f(ref, test, _ref_wset, _test_wset, _rng, _verbose=False):
    assert len(ref) == len(test)

def _test_g(ref, test, ref_wset, test_wset, rng, verbose=False):
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
    _assert(ref, ref_res, test_res)
    _assert_collection(ref_wset, test_wset)

def _test_h(ref, test, _ref_wset, _test_wset, rng, _verbose=False):
    k = rng.randint(0, 5)
    assert ref.count(k) == test.count(k)


def test_multi_ordered_dict():
    ref = _MultiOrderedDict_NSquared()
    test = MultiOrderedDict()
    ref_wset = weakref.WeakSet()
    test_wset = weakref.WeakSet()
    rng = np.random.RandomState()

    tests = [
        _test_a,
        _test_b,
        _test_c,
        _test_d,
        _test_e,
        _test_f,
        _test_g,
        _test_h,
    ]

    for _ in range(5000):
        i = rng.randint(0, len(tests))
        tests[i](ref, test, ref_wset, test_wset, rng, False)
