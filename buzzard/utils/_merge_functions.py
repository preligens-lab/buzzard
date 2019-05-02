import numpy as np

def concat_arrays(fp, array_per_fp, _):
    """Concatenate arrays from `array_per_fp` to form `fp`.

    This function is meant to be fed to the `merge_arrays` parameter when constructing a recipe.
    """
    # Allocate
    for a in array_per_fp.values():
        channel_count = a.shape[-1]
        dtype = a.dtype
        break
    arr = np.empty(np.r_[fp.shape, channel_count], dtype)

    # Burn
    for tile, tile_arr in array_per_fp.items():
        assert tuple(tile.shape) == tile_arr.shape[:2]
        slices = tile.slice_in(fp)
        arr[slices] = tile_arr

    # Return
    return arr
