""">>> help(create_slopes)"""

from __future__ import division, print_function
import numpy as np

def _calcdzdx(dsm, reso, dtype):
    dx = np.zeros(dsm.shape, dtype=dtype)
    dx[1:-1, 1:-1] += (
        ((dsm[:-2, 2:] + dsm[1:-1, 2:] +
          dsm[1:-1, 2:] + dsm[2:, 2:]) -
         (dsm[:-2, :-2] + dsm[2:, 1:-1] +
          dsm[2:, 1:-1] + dsm[2:, :-2])) / (8 * reso)
    )
    return dx.astype(dtype)

def _calcdzdy(dsm, reso, dtype):
    dy = np.zeros(dsm.shape, dtype=dtype)
    dy[1:-1, 1:-1] += (
        ((dsm[2:, :-2] + dsm[2:, 1:-1] +
          dsm[2:, 1:-1] + dsm[2:, 2:]) -
         (dsm[:-2, :-2] + dsm[:-2, 1:-1] +
          dsm[:-2, 1:-1] + dsm[:-2, 2:])) / (8 * reso)
    )
    return dy.astype(dtype)

def create_slopes(dsm, reso, dtype=np.float32, crop=False):
    '''
    Compute the slopes from dsm
    - dsm: (M, N) numpy array
    - reso: float or (float, float)
      - (resox, resoy) = reso: Spanning of a pixel in space
      - Should be in the same unit as dsm's values
      - If a single float is given, it is interpreted as (reso, -reso)
    - dtype: numpy dtype
      - Output dtype
    - crop: bool
      - Cropping of output array
    - return value: numpy array
      - If crop: (M - 1, N - 1)
      - Else: (M, N)
    '''
    try:
        reso = tuple(reso)
    except TypeError:
        reso = (reso, -reso)
    dx = _calcdzdx(dsm, reso[0], dtype)
    dy = _calcdzdy(dsm, reso[1], dtype)
    rise_run = np.sqrt(dx ** 2 + dy ** 2)
    slopes = np.arctan(rise_run) * (180.0 / np.pi)
    slopes = slopes.astype(dtype)
    if crop:
        return slopes[1:-1, 1:-1]
    return slopes
