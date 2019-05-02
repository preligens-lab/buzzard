import numpy as np

def _slices_of_vector(vec):
    """Generates slices of oneline mask parts"""
    assert vec.ndim == 1
    diff = np.diff(np.r_[[False], vec, [False]].astype('int'))
    starts = np.where(diff == 1)[0]
    ends = np.where(diff == -1)[0]
    for s, e in zip(starts, ends):
        yield slice(s, e)

def slices_of_matrix(mask):
    """Generates slices of mask parts"""
    if mask is None:
        yield slice(0, None), slice(0, None)
        return

    ystart = None
    y = 0
    while True:
        # Iteration analysis
        if y == 0:
            begin_group = True
            send_group = False
            stop = False
        elif y == mask.shape[0]:
            begin_group = False
            send_group = True
            stop = True
        elif (mask[y - 1] != mask[y]).any():
            begin_group = True
            send_group = True
            stop = False
        else:
            begin_group = False
            send_group = False
            stop = False

        # Actions
        if send_group:
            yslice = slice(ystart, y)
            for xslice in _slices_of_vector(mask[ystart]):
                yield yslice, xslice
        if begin_group:
            ystart = y

        # Loop control
        if stop:
            break
        y += 1
