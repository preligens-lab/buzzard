"""Tests with pytest"""

import os

import numpy as np

if os.environ.get('LOG_DEBUG'):
    np.set_printoptions(linewidth=300, threshold=np.nan, suppress=True, precision=17)
