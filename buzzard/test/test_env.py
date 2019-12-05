import time
from concurrent.futures import ThreadPoolExecutor

import buzzard as buzz
import numpy as np

def work(i):
    j = i % 9 + 1
    assert buzz.env.significant == 42
    with buzz.Env(significant=j):
        assert buzz.env.significant == j
        time.sleep(np.random.rand() / 100)
        assert buzz.env.significant == j
    assert buzz.env.significant == 42

def test_thread_pool():
    with buzz.Env(significant=42):
        with ThreadPoolExecutor(10) as ex:
            it = ex.map(
                work,
                range(100),
            )
            list(it)
