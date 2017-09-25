"""
Run all unit tests
"""

import os

import pytest

def _m():
    directory = os.path.join(os.path.dirname(os.path.abspath(__file__)))
    pytest.main([directory])

if __name__ == '__main__':
    _m()
