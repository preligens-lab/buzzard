"""
# Part 3: Mandelbrot set computed on the fly

When creating a recipe you define a _Footprint_ through the `fp` parameter. When calling your `compute_array` function the scheduler will only ask for slices of `fp`. This means that the scheduler takes care of those boilerplate steps:
- If you request a Footprint on a different grid in a `get_data` call, the scheduler __takes care of resampling__ the outputs of your `compute_array` function.
- If you request a Footprint partially or fully outside of the raster's extent, the scheduler will call your `compute_array` function to get the interior pixels and then __pad the output with nodata__.

This system can be deactivated by passing `automatic_remapping=False` to the constructor of a _NocacheRasterRecipe_, in this case the scheduler will call your `compute_array` function for any kind of _Footprint_, your function must be able to comply with any request. In `Part 2` the slopes could have been opened that way without changing the rest of the code, the resampling operations would have been deferred to the `elevation` raster.

In the following example `mand_100px`, `mand_10kpx`, `mand_1mpx`, `mand_100mpx`, `mand_10gpx`, `mand_1tpx` are instanciated with automatic remapping, and `ds.mand` is instanciated without.

"""

import buzzard as buzz
import numpy as np
from numba import jit
import shapely.geometry

import example_tools
from part1 import test_raster

def main():
    ds = buzz.DataSource(allow_interpolation=True)
    rwidths = {
        'mand_100px': 10,
        'mand_10kpx': 100,
        'mand_1mpx': 1_000,
        'mand_100mpx': 10_000,
        'mand_10gpx': 100_000,
        'mand_1tpx': 1_000_000,
    }

    # Instanciate 6 fixed scale mandelbrot rasters
    for key, rwidth in rwidths.items():
        # Create a Footprint that ranges from -2 to 2 on both x and y axes
        fp = buzz.Footprint(
            gt=(-2, 4 / rwidth, 0, -2, 0, 4 / rwidth), # TODO: Shrink to -1.5/1.5 or less?
            rsize=(rwidth, rwidth),
        )
        ds.create_raster_recipe(
            key,
            fp=fp,
            dtype='float32',
            band_count=1,
            compute_array=mandelbrot_of_footprint,
            automatic_remapping=True, # default value
            max_computation_size=128,
        )

    # Instanciate 1 flexible scale mandelbrot raster
    ds.create_raster_recipe(
        'mand',
        fp=ds.mand_10kpx.fp, # Scale of Footprint does not mean much here, extent is still important
        dtype='float32',
        band_count=1,
        compute_array=mandelbrot_of_footprint,
        automatic_remapping=False,
        max_computation_size=128,
    )

    # Test 1 - Perform basic tests ****************************************** **
    test_raster(ds.mand_100px)
    test_raster(ds.mand_10kpx)
    test_raster(ds.mand_1mpx)

    # Test 2 - Play with resampling with the non-flexible scale rasters ***** **
    fp100k = buzz.Footprint(
        gt=(-2, 4 / 316, 0, -2, 0, 4 / 316),
        rsize=(316, 316),
    )
    example_tools.show_several_images(
        ('10kpx', ds.mand_10kpx.fp, ds.mand_10kpx.get_data()),
        ('1mpx', ds.mand_1mpx.fp, ds.mand_1mpx.get_data()),
        ('10kpx to 100kpx', fp100k, ds.mand_10kpx.get_data(fp=fp100k)), # upsample * 10
        ('1mpx to 100kpx', fp100k, ds.mand_1mpx.get_data(fp=fp100k)), # downsample * 10
    )

    # Test 3 - Play with with the flexible scale raster ********************* **
    example_tools.show_several_images(
        ('10kpx', ds.mand_10kpx.fp, ds.mand.get_data(fp=ds.mand_10kpx.fp)),
        ('1mpx', ds.mand_1mpx.fp, ds.mand.get_data(fp=ds.mand_1mpx.fp)),
    )

    # Test 4 - Zoom to a point ********************************************** **
    focus = shapely.geometry.Point(-1.1172, -0.221103)
    for key in rwidths.items():
        fp = ds[key].fp
        fp = fp.dilate(250) & focus.buffer(fp.pxsizex * 250)
        arr = ds[key].get_data(fp=fp)
        title = f'{fp.rw}x{fp.rh} rect of the {key} image'
        example_tools.show_several_images((title, fp, arr))

    ds.close()

def mandelbrot_of_footprint(fp, *_):
    """A function to be fed to `compute_array` when constructing a recipe"""
    maxit = int(np.ceil((4 / fp.pxsizex)))
    array = np.empty(fp.shape, 'uint32')
    mandelbrot_jit(array, fp.tl, fp.scale, maxit)
    array = array.astype('float32') / maxit
    return array

@jit(nopython=True, nogil=True, cache=True)
def mandelbrot_jit(array, tl, scale, maxit):
    """Compute a https://en.wikipedia.org/wiki/Mandelbrot_set"""
    for j in range(array.shape[0]):
        y0 = tl[1] + j * scale[1]
        for i in range(array.shape[1]):
            x0 = tl[0] + i * scale[0]
            x, y, x2, y2 = 0., 0., 0., 0.
            iteration = 0
            while x2 + y2 < 4 and iteration < maxit:
                y = 2 * x * y + y0
                x = x2 - y2 + x0
                x2 = x * x
                y2 = y * y
                iteration += 1
            array[j][i] = iteration

if __name__ == '__main__':
    with buzz.Env(allow_complex_footprint=True, warnings=False):
        main()
