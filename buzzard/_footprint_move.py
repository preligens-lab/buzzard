""">>> help(TileMixin)"""

import numpy as np
from buzzard._env import env

class MoveMixin(object):
    """Private mixin for the Footprint class containing move subroutines"""

    def _snap_target_coordinates_before_move(self, tl1, tr1, br1):
        rw, rh = self.rsize

        # Extract `source` values *********************************************** **
        v0 = self.pxlrvec
        w0 = self.pxtbvec
        norm_v0, norm_w0 = self.pxsize
        i0 = v0 / norm_v0
        j0 = w0 / norm_w0

        # Compute `transformed values` ****************************************** **
        v1 = (tr1 - tl1) / rw
        w1 = (br1 - tr1) / rh
        norm_v1 = np.linalg.norm(v1)
        norm_w1 = np.linalg.norm(w1)
        i1 = v1 / norm_v1
        j1 = w1 / norm_w1

        # Prepare comparison function ******************************************* **
        largest_coord = np.abs([tl1, tr1, br1]).max()
        spatial_precision = largest_coord * 10 ** -env.significant
        def _are_close_enough(p, q):
            return (np.abs(p - q) < spatial_precision).all()

        # Compute `transformed and snapped values` ****************************** **
        tl2 = tl1

        # Attempt rounding to preserve `angle` and `pxsize`
        # e.g. The transformation is just a shift
        # e.g. The transformation is a flip or a mirror
        i2 = np.copysign(i0, i1)
        j2 = np.copysign(j0, j1)
        norm_v2 = norm_v0
        norm_w2 = norm_w0

        v2 = i2 * norm_v2
        w2 = j2 * norm_w2
        tr2 = tl2 + v2 * rw
        br2 = tr2 + w2 * rh
        if _are_close_enough(tr1, tr2) and _are_close_enough(br1, br2):
            return tl2, tr2, br2

        # Attempt rounding to preserve `angle` and `pxsizex / pxsizey`
        # e.g. The transformation is a change of unit
        i2 = np.copysign(i0, i1)
        j2 = np.copysign(j0, j1)
        norm_v2 = norm_v1
        norm_w2 = norm_v1 / norm_v0 * norm_w0

        v2 = i2 * norm_v2
        w2 = j2 * norm_w2
        tr2 = tl2 + v2 * rw
        br2 = tr2 + w2 * rh
        if _are_close_enough(tr1, tr2) and _are_close_enough(br1, br2):
            return tl2, tr2, br2

        # Attempt rounding to preserve `angle`
        # e.g. The transformation is a change of unit different on each axis
        i2 = np.copysign(i0, i1)
        j2 = np.copysign(j0, j1)
        norm_v2 = norm_v1
        norm_w2 = norm_w1

        v2 = i2 * norm_v2
        w2 = j2 * norm_w2
        tr2 = tl2 + v2 * rw
        br2 = tr2 + w2 * rh
        if _are_close_enough(tr1, tr2) and _are_close_enough(br1, br2):
            return tl2, tr2, br2

        # Attempt rounding to preserve `pxsize`
        # e.g. The transformation is a rotation
        i2 = i1
        j2 = j1
        norm_v2 = norm_v0
        norm_w2 = norm_w0

        v2 = i2 * norm_v2
        w2 = j2 * norm_w2
        tr2 = tl2 + v2 * rw
        br2 = tr2 + w2 * rh
        if _are_close_enough(tr1, tr2) and _are_close_enough(br1, br2):
            return tl2, tr2, br2

        # Attempt rounding to preserve `pxsizex / pxsizey`
        # e.g. The transformation is a rotation and a change of unit
        i2 = i1
        j2 = j1
        norm_v2 = norm_v1
        norm_w2 = norm_v1 / norm_v0 * norm_w0

        v2 = i2 * norm_v2
        w2 = j2 * norm_w2
        tr2 = tl2 + v2 * rw
        br2 = tr2 + w2 * rh
        if _are_close_enough(tr1, tr2) and _are_close_enough(br1, br2):
            return tl2, tr2, br2

        # Don't perform snapping
        return tl1, tr1, br1
