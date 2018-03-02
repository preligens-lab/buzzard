""">>> help(Rect)"""

from buzzard._env import env
import numpy as np
import affine

class Rect(object):
    """Private tool class used to assess the attributes of an imperfect rectangle"""

    def __init__(self, tl, bl, br, tr):
        self.tl = np.float64(tl)
        self.bl = np.float64(bl)
        self.br = np.float64(br)
        self.tr = np.float64(tr)

    @property
    def empty(self):
        return np.all(self.tl == self.tr) or np.all(self.tl == self.bl)

    @property
    def lrvec(self):
        return self.tr - self.tl

    @property
    def horizontal_parallelism_error(self):
        return (self.tr - self.tl) - (self.br - self.bl)

    @property
    def tbvec(self):
        return self.bl - self.tl

    @property
    def vertical_parallelism_error(self):
        return (self.bl - self.tl) - (self.br - self.tr)

    @property
    def coords(self):
        return np.asarray(
            [self.tl, self.bl, self.br, self.tr]
        )

    @property
    def angle(self):
        lrvec = self.lrvec
        return float(np.arctan2(lrvec[1], lrvec[0]) * 180. / np.pi)

    @property
    def size(self):
        lrvec = self.tr - self.tl
        angle = float(np.arctan2(lrvec[1], lrvec[0]) * 180. / np.pi)
        diagvec = self.br - self.tl
        return np.abs(~affine.Affine.rotation(angle) * diagvec, dtype=np.float64)

    @property
    def spatial_precision(self):
        largest_coord = np.abs(self.coords).max()
        return largest_coord * 10 ** -env.significant

    @property
    def slack_right(self):
        return self.lrvec / self.size[0] * self.spatial_precision

    @property
    def slack_bottom(self):
        return self.tbvec / self.size[1] * self.spatial_precision

    @property
    def tr_slack_angles(self):
        slackr = self.slack_right
        slackb = self.slack_bottom
        return np.asarray((
            _angle_between(self.tl + slackb, self.tr, self.br - slackr),
            _angle_between(self.tl - slackb, self.tr, self.br + slackr),
        ))

    @property
    def tl_slack_angles(self):
        slackr = self.slack_right
        slackb = self.slack_bottom
        return np.asarray((
            _angle_between(self.bl + slackr, self.tl, self.tr + slackb),
            _angle_between(self.bl - slackr, self.tl, self.tr - slackb),
        ))

    @property
    def bl_slack_angles(self):
        slackr = self.slack_right
        slackb = self.slack_bottom
        return np.asarray((
            _angle_between(self.br - slackb, self.bl, self.tl + slackr),
            _angle_between(self.br + slackb, self.bl, self.tl - slackr),
        ))

    @property
    def br_slack_angles(self):
        slackr = self.slack_right
        slackb = self.slack_bottom
        return np.asarray((
            _angle_between(self.tr - slackr, self.br, self.bl - slackb),
            _angle_between(self.tr + slackr, self.br, self.bl + slackb),
        ))

    @property
    def ratio(self):
        return np.linalg.norm(self.lrvec) / np.linalg.norm(self.tbvec)

    @property
    def ratio_slaking(self):
        tbvec0 = (
            self.tbvec / np.linalg.norm(self.tbvec) *
            (np.linalg.norm(self.tbvec) + self.slack_bottom)
        )
        tbvec1 = (
            self.tbvec / np.linalg.norm(self.tbvec) *
            (np.linalg.norm(self.tbvec) - self.slack_bottom)
        )
        lrvec0 = (
            self.lrvec / np.linalg.norm(self.lrvec) *
            (np.linalg.norm(self.lrvec) + self.slack_right)
        )
        lrvec1 = (
            self.lrvec / np.linalg.norm(self.lrvec) *
            (np.linalg.norm(self.lrvec) - self.slack_right)
        )
        ratios = [
            np.linalg.norm(lrvec0) / np.linalg.norm(tbvec0),
            np.linalg.norm(lrvec1) / np.linalg.norm(tbvec0),
            np.linalg.norm(lrvec0) / np.linalg.norm(tbvec1),
            np.linalg.norm(lrvec1) / np.linalg.norm(tbvec1),
        ]
        return np.min(ratios), np.max(ratios)

    def scale(self, rsize):
        aff = ~affine.Affine.rotation(self.angle)
        tl = np.asarray(aff * self.tl)
        br = np.asarray(aff * self.br)
        return np.asarray((br - tl) / rsize, dtype=np.float64)

    def significant_min(self, smallest_reso):
        largest_coord = np.abs(self.coords).max()
        ratio_pixel_increment = smallest_reso / largest_coord
        return -np.log10(ratio_pixel_increment)

    def abstract_grid_density(self, smallest_reso):
        pixel_precision = self.spatial_precision / smallest_reso
        return np.floor(1 / pixel_precision)

def _angle_between(a, b, c):
    return np.arccos(np.dot(
        (a - b) / np.linalg.norm(a - b),
        (c - b) / np.linalg.norm(c - b),
    )) / np.pi * 180.

def _round_sig(arr, sig_digit):
    assert 0 < sig_digit <= 16
    arr = np.asarray(arr)
    nonzero_mask = arr != 0
    out = np.zeros_like(arr)
    if nonzero_mask.any():
        nz_arr = arr[nonzero_mask]
        sign = np.sign(nz_arr)
        nz_arr *= sign
        round_place = 10 ** (np.ceil(np.log10(nz_arr)) - sig_digit)
        out[nonzero_mask] = round_place * np.around(nz_arr / round_place) * sign
    return out
