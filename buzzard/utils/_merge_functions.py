import numpy as np

def concat_arrays(fp, array_per_fp, _):
    """TODO: move to buzz.algo?.concat_arrays
    buzz.algo.concat_arrays
    buzz.algo.slopes_recipe
    buzz.algo.cascaded_resampled_recipes

    """
    # Allocate
    for a in array_per_fp.values():
        band_count = a.shape[-1]
        dtype = a.dtype
    arr = np.empty(np.r_[fp.shape, band_count], dtype)

    # Burn
    for tile, tile_arr in array_per_fp.items():
        assert tuple(tile.shape) == tile_arr.shape[:2]
        slices = tile.slice_in(fp)
        arr[slices] = tile_arr

    # Return
    return arr
