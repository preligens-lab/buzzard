"""Tests for _tools"""

# pylint: disable=redefined-outer-name

from __future__ import division, print_function

import numpy as np
import pytest

import buzzard as buzz

def test_band_parameters():
    """Tests for _tools.parameters.normalize_channels_parameter"""

    # Flat - integers/floats ******************************************************************** **
    def _test_integers(f):
        for i in [-2, 4]:
            with pytest.raises(ValueError):
                f(i)
        assert f(-1) == ([0, 1, 2], False)
        assert f(0) == ([0], True)
        assert f(1) == ([1], True)
        assert f(2) == ([2], True)

    _test_integers(lambda v: buzz._tools.normalize_channels_parameter(int(v), 3))
    _test_integers(lambda v: buzz._tools.normalize_channels_parameter(np.int8(v), 3))
    _test_integers(lambda v: buzz._tools.normalize_channels_parameter(np.int64(v), 3))
    _test_integers(lambda v: buzz._tools.normalize_channels_parameter(float(v), 3))
    _test_integers(lambda v: buzz._tools.normalize_channels_parameter(np.float32(v), 3))

    def _test_failing_floats(f):
        for i in np.arange(-1.5, 4.5,):
            with pytest.raises(TypeError):
                f(i)

    _test_failing_floats(lambda v: buzz._tools.normalize_channels_parameter(float(v), 3))
    _test_failing_floats(lambda v: buzz._tools.normalize_channels_parameter(np.float32(v), 3))

    # Flat - extra ****************************************************************************** **
    f = lambda v: buzz._tools.normalize_channels_parameter(v, 3)
    with pytest.raises(TypeError):
        f('salut')
    with pytest.raises(TypeError):
        f('')
    with pytest.raises(TypeError):
        f(int)

    # Iterable for 3 **************************************************************************** **
    f = lambda v: buzz._tools.normalize_channels_parameter(v, 3)
    assert f(-1) == ([0, 1, 2], False)
    assert f([-1]) == ([0, 1, 2], False)
    assert f([-1, -1]) == ([0, 1, 2, 0, 1, 2], False)
    assert f([0]) == ([0], False)
    assert f([1]) == ([1], False)
    assert f([2]) == ([2], False)
    assert f([1, 2]) == ([1, 2], False)
    assert f([0, 1, 2]) == ([0, 1, 2], False)
    assert f(range(0, 3)) == ([0, 1, 2], False)

    # Iterable for 1 **************************************************************************** **
    f = lambda v: buzz._tools.normalize_channels_parameter(v, 1)
    assert f(-1) == ([0], True)
    assert f([-1]) == ([0], False)
    assert f([-1, -1]) == ([0, 0], False)
    assert f([0]) == ([0], False)
    assert f(range(0, 1)) == ([0], False)

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
