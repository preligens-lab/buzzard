#inspired by github.com/buckinha/DiamondSquare

import sys
import uuid
import random
import itertools

import numpy as np
import numba as nb
from numba import jit

import buzzard as buzz


def generate_dsm(rsize=(16000, 16000),
                 resolution=0.03,
                 delta_z=(10, 110),
                 roughness=0.45,
                 nb_houses=5,
                 verbose=False):
    """generates a `.tif` file containing an artificial dsm.
    the generated dsm does not have a projection, nor no_data values.

    Parameters
    ----------
    - rsize: (int, int)
        desired raster size
    - resolution: (float)
        desired resolution in meters
    - delta_z: (float, float)
        span of elevations on generated dsm in meters
    - roughness: float
        value ranging from 0 to 1. 0 will generate a very smooth dsm (a plane). 0.5 generates
        midly rough landscapes (imagine a valley in the Swiss Alps). 1 generates very sharp results.
    - nb_houses: int
        number of houses to be added on the dsm
    - verbose: boolean
        if True, some information about the generated dsm will be printed.
    """

    w, l = rsize
    min_z, max_z = delta_z

    if verbose:
        print("==== Metrics on generated dsm ====")
        print(f"  w, l = {w}, {l}")
        print(f"  resolution = {resolution}")
        print(f"  roughness = {roughness}")
        print(f"  min_z, max_z = {min_z}, {max_z}")
        print(f"  nb_houses = {nb_houses}")

    dsm = diamond_square((l, w), min_z, max_z, roughness)
    for _ in range(nb_houses):
        _put_house_on_dsm(dsm, resolution, verbose)

    tlx = np.random.uniform(42, 1337)
    tly = np.random.uniform(32, 111)
    fp = buzz.Footprint(tl=(tlx, tly), size=(w * resolution, l * resolution), rsize=(w, l))

    ds = buzz.Dataset(allow_interpolation=False)
    filename = f'{uuid.uuid4()}.tif'
    if verbose:
        print(f'  {fp}')
        print('  filename = ' + filename)

    with ds.acreate_raster(filename, fp, dtype='float32', channel_count=1, sr=None).close as out:
        out.set_data(dsm)
    return filename


def _put_house_on_dsm(dsm, resolution, verbose):
    """put a cosy little house somewhere on the dsm, but not on the edge so it doesn't fall over"""
    try:
        h_sx, h_sy = np.random.uniform(2, 20, 2) // resolution
        h_sx, h_sy = int(h_sx), int(h_sy)
        if verbose:
            print(f'  putting house at {(h_sx, h_sy)}')
        h_tly = int(np.random.uniform(2 * h_sx, dsm.shape[1] - 2 * h_sx))
        h_tlx = int(np.random.uniform(2 * h_sy, dsm.shape[0] - 2 * h_sy))

        house = dsm[h_tlx:h_tlx + h_sx, h_tly:h_tly + h_sy]
        house[:, :] = house.max() + 5
    except:
        pass


def diamond_square(desired_shape, min_z, max_z, roughness, random_seed=None):
    """Runs a diamond square algorithm and returns a ndarray of desired size.
    /!\\ Warning: this algorithm is computed on a square array which is then cropped.
    The size of this squared array is the power of 2 immediately following the biggest
    of the 2 given dimensions.
    This means that the computation effort will be the same
    for a (10, 1000), a (600, 600) or a (1000, 1000) desired shape, ie a (ndarray of 1024, 1024).

    Parameters
    ----------
    - size (iterable of 2 int): shape of the array to be returned

    - min_z (float): minimum height allowed on the landscape

    - max_z (float): maximum height allowed on the landscape

    - roughness (float in [0, 1]):
        how bumpy the dsm should be. values near 1 will result in landscapes that are
        extremly rough, and have almost no cell-to-cell smoothness.
        Values near 0 will result in landscapes that are almost perfectly smooth.

    random_seed (int): if a not None value is given, it is used as seed for random number generator

    """
    if not hasattr(desired_shape, '__iter__'):
        raise TypeError(f'desired_shape is not an iterable but a {type(desired_shape)}')
    else:
        size = desired_shape[:2]

    DS_size, iterations = _get_DS_size_and_iters(size)
    DS_array = np.zeros((DS_size,DS_size), dtype='float32')
    DS_array -= 1.0

    # seed the corners
    random.seed(random_seed)
    DS_array[0, 0] = random.uniform(0, 1)
    DS_array[DS_size - 1, 0] = random.uniform(0, 1)
    DS_array[0, DS_size - 1] = random.uniform(0, 1)
    DS_array[DS_size - 1, DS_size - 1] = random.uniform(0, 1)

    roughness = min(max(roughness, 0), 1)
    # main algo
    for i in range(iterations):
        r = roughness ** i
        step_size = (DS_size - 1) // 2 ** i
        _diamond_step(DS_array, step_size, r)
        _square_step(DS_array, step_size, r)

    DS_array = min_z + (DS_array * (max_z - min_z))
    final_array = DS_array[:size[0], :size[1]]
    return final_array


def _get_DS_size_and_iters(requested_size):
    largest_edge = max(requested_size)
    for power in itertools.count():
        d = 2 ** power + 1
        if largest_edge <= d:
            return d, power


@jit(nb.void(nb.float32[:,:], nb.int32, nb.float32), nopython=True, parallel=True, target='cpu', nogil=True)
def _diamond_step(DS_array, step_size, roughness):
    half_step = step_size // 2

    for i in nb.prange(0, DS_array.shape[0] // step_size):
        i = i * step_size + half_step
        for j in nb.prange(0, DS_array.shape[0] // step_size):
            j = j * step_size + half_step

            if DS_array[i,j] == -1.0:
                ul = DS_array[i - half_step, j - half_step]
                ur = DS_array[i - half_step, j + half_step]
                ll = DS_array[i + half_step, j - half_step]
                lr = DS_array[i + half_step, j + half_step]

                ave = (ul + ur + ll + lr) / 4.0
                rand_val = random.uniform(0, 1)
                DS_array[i, j] = roughness * rand_val + (1.0 - roughness) * ave


@jit(nb.float32(nb.int32, nb.int32, nb.float32, nb.int32, nb.float32[:, :]), nopython=True, target='cpu', nogil=True)
def _compute_mid_value(i, j, roughness, half_step, DS_array):
        _sum = 0.0
        divide_by = 4

        #check cell "above"
        if i - half_step >= 0:
            _sum += DS_array[i-half_step, j]
        else:
            divide_by -= 1

        #check cell "below"
        if i + half_step < DS_array.shape[0]:
            _sum += DS_array[i+half_step, j]
        else:
            divide_by -= 1

        #check cell "left"
        if j - half_step >= 0:
            _sum += DS_array[i, j-half_step]
        else:
            divide_by -= 1

        #check cell "right"
        if j + half_step < DS_array.shape[0]:
            _sum += DS_array[i, j+half_step]
        else:
            divide_by -= 1

        ave = _sum / divide_by
        rand_val = random.uniform(0, 1)
        return roughness * rand_val + (1.0 - roughness) * ave


@jit(nb.void(nb.float32[:, :], nb.int32, nb.float32), nopython=True, parallel=True, target='cpu', nogil=True)
def _square_step(DS_array, step_size, roughness):

    half_step = step_size // 2
    for i in nb.prange(0, DS_array.shape[0] // step_size):
        i = i * step_size + half_step
        for j in nb.prange(0, DS_array.shape[1] // step_size):
            j *= step_size
            DS_array[i,j] = _compute_mid_value(i, j, roughness, half_step, DS_array)

    for i in nb.prange(0, DS_array.shape[0] // step_size):
        i *= step_size
        for j in nb.prange(0, DS_array.shape[1] // step_size):
            j = j * step_size + half_step
            DS_array[i,j] = _compute_mid_value(i, j, roughness, half_step, DS_array)



if __name__ == '__main__':
    generate_dsm()
