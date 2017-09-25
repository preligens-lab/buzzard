"""Tests for _tools"""

# pylint: disable=redefined-outer-name

from __future__ import division, print_function

import numpy as np
import pytest

import buzzard as buzz

def test_band_parameters():
    """Tests for _tools.parameters.normalize_band_parameter"""

    # Flat - integers/floats ******************************************************************** **
    def _test_integers(f):
        for i in [-2, 0, 4]:
            with pytest.raises(ValueError):
                f(i)
        assert f(-1) == ([1, 2, 3], False)
        assert f(1) == ([1], True)
        assert f(2) == ([2], True)
        assert f(3) == ([3], True)

    _test_integers(lambda v: buzz._tools.normalize_band_parameter(v, 3, 42j))
    _test_integers(lambda v: buzz._tools.normalize_band_parameter(np.int8(v), 3, 42j))
    _test_integers(lambda v: buzz._tools.normalize_band_parameter(np.int64(v), 3, 42j))
    _test_integers(lambda v: buzz._tools.normalize_band_parameter(float(v), 3, 42j))
    _test_integers(lambda v: buzz._tools.normalize_band_parameter(np.float32(v), 3, 42j))

    def _test_failing_floats(f):
        for i in np.arange(-1.5, 4, 1.):
            with pytest.raises(ValueError):
                f(i)

    _test_failing_floats(lambda v: buzz._tools.normalize_band_parameter(float(v), 3, 42j))
    _test_failing_floats(lambda v: buzz._tools.normalize_band_parameter(np.float32(v), 3, 42j))

    # Flat - complex **************************************************************************** **
    def _test_complex(f):
        for i in [-2, -1, 1, 2, 3, 4]:
            for j in range(-2, 4 + 1):
                with pytest.raises(ValueError):
                    f(i + j * 1j)
        for i in [-2, 4] + list(np.arange(-1.5, 4, 1.)):
            with pytest.raises(ValueError):
                f(i * 1j)
        assert f(0j) == ([42j], True)
        assert f(-1j) == ([1j, 2j, 3j], False)
        assert f(1j) == ([1j], True)
        assert f(2j) == ([2j], True)
        assert f(3j) == ([3j], True)

    _test_complex(lambda v: buzz._tools.normalize_band_parameter(v, 3, 42j))
    _test_complex(lambda v: buzz._tools.normalize_band_parameter(np.complex64(v), 3, 42j))

    # ******************************************************************************************* **
    f = lambda v: buzz._tools.normalize_band_parameter(v, 3, 42j)

    # Flat - extra ****************************************************************************** **
    with pytest.raises(TypeError):
        f('salut')
    with pytest.raises(TypeError):
        f('')
    with pytest.raises(TypeError):
        f(int)

    # Iterable ********************************************************************************** **
    assert f([-1]) == ([1, 2, 3], False)
    assert f([-1, -1]) == ([1, 2, 3, 1, 2, 3], False)
    assert f([1]) == ([1], False)
    assert f([2]) == ([2], False)
    assert f([3]) == ([3], False)
    assert f([1, 2]) == ([1, 2], False)
    assert f([1, 2, 3]) == ([1, 2, 3], False)
    assert f([0j]) == ([42j], False)
    assert f([-1j]) == ([1j, 2j, 3j], False)
    assert f([-1j, -1j]) == ([1j, 2j, 3j, 1j, 2j, 3j], False)
    assert f([-1j, -1, 0j, 3, 3, 2, 1]) == ([1j, 2j, 3j, 1, 2, 3, 42j, 3, 3, 2, 1], False)
    assert f(range(1, 4)) == ([1, 2, 3], False)
    assert f((i for i in range(1, 4))) == ([1, 2, 3], False)

    assert f([-1.]) == ([1, 2, 3], False)
    assert f([-1., np.int8(-1)]) == ([1, 2, 3, 1, 2, 3], False)
    assert f([1.]) == ([1], False)
    assert f([np.float16(2)]) == ([2], False)
    assert f([np.int64(3)]) == ([3], False)
    assert f([1., 2]) == ([1, 2], False)
    assert f([1., np.float64(2), 3]) == ([1, 2, 3], False)
    assert f([np.complex64(0j)]) == ([42j], False)
    assert f([np.complex128(-1j)]) == ([1j, 2j, 3j], False)
    assert f([-1j, np.complex_(-1j)]) == ([1j, 2j, 3j, 1j, 2j, 3j], False)
    assert (
        f([-1j, -1, np.complex_(0j), 3, 3., np.uint8(2), 1]) ==
        ([1j, 2j, 3j, 1, 2, 3, 42j, 3, 3, 2, 1], False)
    )

    ll = [
        [0],
        [-2],
        [0.5],
        [1, 2, 3, 4],
        [0, 1],
        [1, 2, 3., .4],
        [1, 1., np.float16(1), np.int8(1), 0],
        [1, 1., np.float16(1), np.int8(1), -2],
        [1, 1., np.float16(1), np.int8(1), -2j],
        [1, 1., np.float16(1), np.int8(1), 4],
        [1, 1., np.float16(1), np.int8(1), 1 + 0j],
    ]
    for l in ll:
        with pytest.raises(ValueError):
            f(l)

    # Iterable - failing ************************************************************************ **
    ll = [
        ['hello'],
        [''],
        [-1, ''],
        [-1j, ''],
        [-1j, []],
        [-1j, [1]],
    ]
    for l in ll:
        with pytest.raises(TypeError):
            f(l)
    with pytest.raises(ValueError):
        f([])
