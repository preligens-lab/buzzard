import sys
import subprocess
from concurrent.futures import ThreadPoolExecutor
import multiprocessing as mp
import uuid
import os
import tempfile

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

def gen_tests():
    res = subprocess.check_output(['pytest', '--collect-only'] + args_phase0)
    res = res.decode('utf8')

    m = None
    for l in res.split('\n'):
        if l.startswith("<Module '"):
            m = l.replace("<Module '", '')[:-2]
            yield m
        elif l.startswith("  <Function '"):
            pass
        else:
            pass

def test(batch):
    path = os.path.join(tempfile.gettempdir(), str(uuid.uuid4()))
    cmd = ' '.join([
        'pytest',
        *args_phase1,
        *[f"'{s}'"
          for s in batch
        ],
        f'&>{path}'
    ])
    cmd = f'bash -c "{cmd}"'
    try:
        print(cmd)
        code = os.system(cmd)
    finally:
        res = ''
        if os.path.isfile(path):
            with open(path) as stream:
                res = stream.read()
            os.remove(path)
    if code != 0:
        raise Exception(
            f'{cmd} failed with code {code}\n============= output:\n{res}\n=============\n'
        )

args_phase0 = list(sys.argv[1:])
args_phase1 = [
    s
    for s in sys.argv[1:]
    if s[0] == '-'
]

tests = list(gen_tests())
tests = sorted(tests)[::-1]

tests = group(1, tests)
with ThreadPoolExecutor(mp.cpu_count()) as ex:
    list(ex.map(test, tests))
