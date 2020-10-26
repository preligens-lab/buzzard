# pylint: disable=redefined-outer-name, unused-argument

import itertools

import pytest
import buzzard as buzz
import numpy as np

try:
    import torch
except ImportError:
    pytest.skip('No pytorch', allow_module_level=True)

@pytest.fixture(scope="module", autouse=True)
def env():
    np.set_printoptions(linewidth=400, threshold=np.iinfo('int64').max, suppress=True)
    with buzz.Env(allow_complex_footprint=1):
        yield

def pytest_generate_tests(metafunc):
    return _pytest_generate_tests(metafunc)

@buzz.Env(allow_complex_footprint=1)
def _pytest_generate_tests(metafunc):

    # *********************************************************************** **
    scales = [
        (1, 1),
        (1, -1),
        (2, 3),
    ]
    rots = [
        0,
        90, 180, -135,
    ]
    if 'output_padding' in metafunc.fixturenames:
        rsizes = [
            (4, 5),
            (6, 7),
        ]
    else:
        rsizes = [
            (15, 16),
            (17, 18),
        ]
    src_fp = buzz.Footprint(rsize=(1, 1), gt=(100, 1, 0, 100, 0, 1))
    confs = list(itertools.product(scales, rots, rsizes))
    fp0s = []

    for (scalex, scaley), rot, rsize in confs:
        fp = src_fp.dilate(rsize[0] * 3).intersection(
            src_fp.dilate(rsize[0] * 3),
            rotation=rot,
            scale=src_fp.scale * [scalex, scaley]
        ).clip(0, 0, *rsize)
        assert np.allclose(fp.scale, (scalex, scaley))
        assert np.allclose((fp.angle + 360) % 360, (rot + 360) % 360)
        assert np.all(fp.rsize == rsize)
        fp0s.append(fp)

    # *********************************************************************** **
    kernel_sizes = np.asarray([
        (1, 2),
        (3, 4),
    ])

    # *********************************************************************** **
    strides = np.asarray([
        (1, 2),
        (3, 4),
    ])

    # *********************************************************************** **
    if 'output_padding' in metafunc.fixturenames:
        paddings = np.asarray([
            (0, 1),
            (2, 3),
        ])
    else:
        paddings = np.asarray([
            (0, 1),
            (2, 3),
        ])

    # *********************************************************************** **
    dilations = np.asarray([
        (1, 1),
        (2, 3),
    ])

    # *********************************************************************** **
    output_paddings = np.asarray([
        (0, 1),
        (2, 3),
    ])

    # *********************************************************************** **
    if 'output_padding' in metafunc.fixturenames:
        tests = list(itertools.product(
            fp0s, kernel_sizes, strides, paddings, dilations, output_paddings,
        ))
        tests = [
            (fp0, kernel_size, stride, padding, dilation, output_padding)
            for fp0, kernel_size, stride, padding, dilation, output_padding in tests
            if np.all(output_padding < kernel_size) and np.all(output_padding < dilation)
        ]
        metafunc.parametrize(
            argnames='fp0,kernel_size,stride,padding,dilation,output_padding',
            argvalues=tests,
        )
    else:
        metafunc.parametrize(
            argnames='fp0,kernel_size,stride,padding,dilation',
            argvalues=list(itertools.product(
                fp0s, kernel_sizes, strides, paddings, dilations,
            )),
        )

def test_conv2d(fp0, kernel_size, stride, padding, dilation):
    # Test forward against meshgrid computed using pytorch ****************** **
    fp1 = fp0.forward_conv2d(kernel_size, stride, padding, dilation)
    truth = meshgrid_of_forward_conv2d(fp0, kernel_size, stride, padding, dilation)
    assert np.allclose(truth, fp1.meshgrid_spatial)

    # Test backward against forward ***************************************** **
    # (modulo small bottom-right overflows caused by stride and fp1.rsize)
    fp0_bis = fp1.backward_conv2d(kernel_size, stride, padding, dilation)

    angle = (fp0.angle + 360) % 90
    if np.allclose(np.cos(angle * 4), 1):
        # is orthogonal
        assert np.all(fp0.gt == fp0_bis.gt)
    else:
        assert np.allclose(fp0.gt, fp0_bis.gt)

    shape_overflow = fp0.shape - fp0_bis.shape
    assert np.all(shape_overflow >= 0)
    assert np.all(shape_overflow < stride)

def test_convtranspose2d(fp0, kernel_size, stride, padding, dilation, output_padding):
    # Test forward against meshgrid computed with pytorch ******************* **
    # (modulo the meshgrid values at output_padding)
    fp1 = fp0.forward_convtranspose2d(kernel_size, stride, padding, dilation, output_padding)
    truth = meshgrid_of_forward_convtranspose2d(
        fp0, kernel_size, stride, padding, dilation, output_padding,
    )
    fromfp = np.asarray(fp1.meshgrid_spatial)
    fromfp = fromfp[:, :fp1.shape[0] - output_padding[0], :fp1.shape[1] - output_padding[1]]
    assert np.allclose(truth, fromfp)

    # Test backward against forward ***************************************** **
    fp0_bis = fp1.backward_convtranspose2d(kernel_size, stride, padding, dilation, output_padding)

    angle = (fp0.angle + 360) % 90
    if np.allclose(np.cos(angle * 4), 1):
        # is orthogonal
        assert fp0 == fp0_bis
    else:
        assert fp0.almost_equals(fp0_bis)

# *********************************************************************************************** **
# Utils ***************************************************************************************** **
def meshgrid_of_forward_conv2d(fp, kernel_size, stride, padding, dilation):
    fp_padded = fp.dilate(*np.flipud(padding))

    c = torch.nn.Conv2d(
        in_channels=2, out_channels=2, groups=2, bias=False, padding=0,
        kernel_size=kernel_size, stride=stride.tolist(), dilation=dilation.tolist(),
    )
    c.weight = torch.nn.Parameter(
        torch.ones_like(c.weight) / np.prod(kernel_size),
        requires_grad=False,
    )

    x = np.asarray(fp_padded.meshgrid_spatial).reshape(1, 2, *fp_padded.shape).astype('float32')
    with torch.no_grad():
        y = c(torch.from_numpy(x)).numpy()
    return y.reshape(2, *y.shape[2:])

def create_bilinear_kernel(stride, dtype=None):
    # Parameters ************************************************************ **
    v = stride
    v = np.asarray(v).reshape(-1)
    if v.size == 1:
        v = np.asarray((v, v))
    if v.size != 2:
        raise ValueError('{} should have size 1 or 2'.format(k))
    w = v.astype(int, copy=False)
    if np.any(v != w):
        raise ValueError('{} should be of type int'.format(k))
    if np.any(v < 1):
        raise ValueError('{} should be greater or equal to 1'.format(k))
    stride = v

    # Build kernel ********************************************************** **
    shape = (stride - 1) * 2 + 1
    center = stride - (shape % 2 + 1) / 2
    kernel = np.zeros(shape, dtype='float32')
    for yx in np.ndindex(*shape):
        val = 1 - np.abs((yx - center) / stride)
        kernel[yx[0], yx[1]] = np.prod(val)

    # Assert correctness **************************************************** **
    indices = np.asarray(list(np.ndindex(*kernel.shape)))
    for i in range(stride[0]):
        for j in range(stride[1]):
            mask = (indices % stride == (i, j)).all(axis=1)
            facts = kernel.reshape(-1)[mask]
            assert np.allclose(facts.sum(), 1)

    return kernel

def inner_meshgrid_of_forward_convtranspose2d(fp, stride):
    kernel = create_bilinear_kernel(stride)
    kernel_shape = np.asarray(kernel.shape)
    assert np.all(kernel_shape % 2 == 1)
    padding_input = np.asarray((kernel_shape - 1) // 2)
    padding_parameter = (kernel_shape - 1) - padding_input

    # Assert that the `padding_parameter` is good *************************** **
    c = torch.nn.ConvTranspose2d(
        in_channels=1, out_channels=1, groups=1, bias=False, padding=padding_parameter.tolist(),
        kernel_size=kernel.shape, stride=stride.tolist(),
    )
    c.weight = torch.nn.Parameter(
        torch.ones_like(c.weight) * torch.from_numpy(kernel), requires_grad=False
    )
    x = np.ones(shape=(1, 1, *fp.shape), dtype='float32')
    with torch.no_grad():
        y = c(torch.from_numpy(x)).numpy()
    assert np.allclose(y, 1)
    del x, y

    # Compute the meshgrid ************************************************** **
    c = torch.nn.ConvTranspose2d(
        in_channels=2, out_channels=2, groups=2, bias=False, padding=padding_parameter.tolist(),
        kernel_size=kernel.shape, stride=stride.tolist(),
    )
    c.weight = torch.nn.Parameter(
        torch.from_numpy(np.stack([kernel, kernel])[:, None, :, :]), requires_grad=False
    )

    x = np.asarray(fp.meshgrid_spatial).reshape(1, 2, *fp.shape).astype('float32')
    with torch.no_grad():
        y = c(torch.from_numpy(x)).numpy()
    return y[0]

def meshgrid_of_forward_convtranspose2d(fp, kernel_size, stride, padding, dilation, output_padding):
    dilation_parameter = dilation
    kernel_size_parameter = kernel_size
    padding_parameter = padding
    del padding, dilation

    kernel_size = 1 + (kernel_size_parameter - 1) * dilation_parameter
    padding_input = dilation_parameter * (kernel_size_parameter - 1) - padding_parameter
    todilate_input = np.ceil(padding_input / stride).astype(int)
    tocrop_inner_meshgrid = todilate_input * stride - padding_input
    fp_padded = fp.dilate(*np.flipud(todilate_input))

    # Create inner meshgrid ************************************************* **
    # First create a larger one and then crop what's needed
    mg = inner_meshgrid_of_forward_convtranspose2d(fp_padded, stride)
    mg = mg[
        :,
        tocrop_inner_meshgrid[0]:mg.shape[1] - tocrop_inner_meshgrid[0],
        tocrop_inner_meshgrid[1]:mg.shape[2] - tocrop_inner_meshgrid[1],
    ]

    # Apply conv2d on inner meshgrid **************************************** **
    c = torch.nn.Conv2d(
        in_channels=2, out_channels=2, groups=2, bias=False, padding=0, stride=1, dilation=1,
        kernel_size=kernel_size,
    )
    c.weight = torch.nn.Parameter(
        torch.ones_like(c.weight) / np.prod(kernel_size),
        requires_grad=False,
    )

    x = mg[None, :, :, :]
    with torch.no_grad():
        y = c(torch.from_numpy(x)).numpy()
    mg = y[0]

    # Check meshgrid shape ************************************************** **
    truth_shape = output_shape_of_conv2dtranspose(
        fp,
        kernel_size=kernel_size_parameter, stride=stride,
        padding=padding_parameter, dilation=dilation_parameter,
        output_padding=output_padding,
    )
    found_shape = mg.shape[1:]
    found_shape = tuple(found_shape + output_padding)
    assert found_shape == truth_shape

    return mg

def output_shape_of_conv2dtranspose(fp, **kwargs):
    c = torch.nn.ConvTranspose2d(
        in_channels=1, out_channels=1, groups=1, bias=False,
        **{
            k: v.tolist()
            for k, v in kwargs.items()
        },
    )
    x = np.asarray(fp.meshgrid_spatial[0]).reshape(1, 1, *fp.shape).astype('float32')
    with torch.no_grad():
        y = c(torch.from_numpy(x)).numpy()
    return y.shape[2:]
