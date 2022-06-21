"""
Run buzzard's tests in parallel. Pass the same arguments as you would pass to `pytest`.
Gets the tests down to 2min from 7.5min. One test takes 2min, it would be a good idea to split it.

```sh
$ python buzzard/test/pytest_parallel.py -x .
```

"""

import sys
import subprocess
from concurrent.futures import ThreadPoolExecutor
import multiprocessing as mp
import uuid
import os
import tempfile
import collections
import datetime

TOCHUNK = [
    'test_cached_raster_recipe',
    'test_vectorsource_getsetdata_general',
]

def group(n, iterable):
    stop = False
    l = []
    for obj in iterable:
        l.append(obj)
        if len(l) == n:
            yield l
            l = []
    if l:
        yield l

def _print_cmd(s):
    print(f'\033[33m$ {s}\033[0m')

def _gen_tests():
    path = os.path.join(tempfile.gettempdir(), 'pytest-collection-tmp')
    args_phase0 = [
        s
        for s in sys.argv[1:]
        if 'junitxml' not in s and
        'cov' not in s
    ]
    cmd = ['pytest', '--collect-only'] + args_phase0 + [f'&>{path}']
    cmd = ' '.join(cmd)
    cmd = f'bash -c "{cmd}"'

    try:
        _print_cmd(cmd)
        a = datetime.datetime.now()
        code = os.system(cmd)
        b = datetime.datetime.now()
    finally:
        res = ''
        if os.path.isfile(path):
            with open(path) as stream:
                res = stream.read()
            os.remove(path)
    dt = (b - a).total_seconds()
    print(' ', cmd, f'(took {dt:.1f}sec)')
    if code != 0:
        raise Exception(
            '{} failed with code {}\n============= output:\n{}\n=============\n'.format(
                cmd, code, res
            )
        )
    d = collections.defaultdict(list)

    m = None
    for l in res.split('\n'):
        l = l.strip()
        if l.startswith("<Module "):
            m = l.replace("<Module ", '')[:-1]
        elif l.startswith("<Function "):
            assert m is not None, l
            f = l.replace("<Function ", '')[:-1]
            d[os.path.join("buzzard", "test", m)].append(f)
        else:
            pass

    print('Found {} tests scattered on {} files'.format(
        sum(map(len, d.values())),
        len(d),
    ))
    for m, fs in d.items():
        if any(
                n in m
                for n in TOCHUNK
        ):
            print(f'  {m} -> ({len(fs)} calls of 1 test)')
            for f in fs:
                yield f'{m}::{f}'
        else:
            print(f'  {m} -> (1 call of {len(fs)} tests)')
            yield m

def _run_test(batch):
    uid = str(uuid.uuid4())
    path = os.path.join(tempfile.gettempdir(), uid)
    args_phase1 = [
        s.replace(
            'pytest-report.xml', f'pytest-report-{uid}.xml'
        ).replace(
            'coverage.xml', f'coverage-{uid}.xml'
        )
        for s in sys.argv[1:]
        if s[0] == '-'
    ]

    cmd = ' '.join(
        ['pytest'] +
        args_phase1 +
        [f"'{s}'"
          for s in batch
        ] +
        [f'&>{path}']
    )
    cmd = f'COVERAGE_FILE=.coverage.{uid} bash -c "{cmd}"'
    try:
        _print_cmd(cmd)
        a = datetime.datetime.now()
        code = os.system(cmd)
        b = datetime.datetime.now()
    finally:
        res = ''
        if os.path.isfile(path):
            with open(path) as stream:
                res = stream.read()
            os.remove(path)
    dt = (b - a).total_seconds()
    print(' ', cmd, f'(took {dt:.1f}sec)')
    if code != 0:
        raise Exception(
            '{} failed with code {}\n============= output:\n{}\n=============\n'.format(
                cmd, code, res
            )
        )

if __name__ == '__main__':
    print('-- Discovering tests --')
    tests = list(_gen_tests())
    tests = sorted(tests)[::-1]

    tests = list(group(1, tests))
    print('-- Running tests, {} calls to pytest, {} simulateneous --'.format(
        len(tests), mp.cpu_count()
    ))
    with ThreadPoolExecutor(mp.cpu_count()) as ex:
        list(ex.map(_run_test, tests))
