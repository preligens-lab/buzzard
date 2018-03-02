"""help(Analysis)"""

import numpy as np

from buzzard import Footprint
from buzzard import _tools

class Analysis(object):
    """Private class used to assess the quality of a coordinate transformation"""

    def __init__(self, transformation, inverse, rect=None):
        self.messages = []
        self.conversions_finite = None
        self.inverse_valid = None
        self.angles_valid = None
        self.ratio_valid = None

        if isinstance(rect, Footprint):
            self._run_rect_analysis(transformation, inverse, _tools.Rect(*rect.coords))
        elif np.asarray(rect).shape == (4,):
            minx, maxx, miny, maxy = rect
            rect = _tools.Rect(
                (minx, maxy),
                (minx, miny),
                (maxx, miny),
                (maxx, maxy),
            )
            self._run_rect_analysis(transformation, inverse, rect)

    def _run_rect_analysis(self, transformation, inverse, rect1):
        # *************************************************************************************** **
        self.conversions_finite = False
        rect2 = _tools.Rect(*np.asarray(transformation(rect1.coords))[:, :2])
        if not np.isfinite(rect2.coords).all():
            self.messages.append('forward transformation yielded an infite or nan value')
            return
        rect3 = _tools.Rect(*np.asarray(inverse(rect2.coords))[:, :2])
        if not np.isfinite(rect3.coords).all():
            self.messages.append('inverse transformation yielded an infite or nan value')
            return
        self.conversions_finite = True

        # *************************************************************************************** **
        self.inverse_valid = False
        if (np.abs(rect1.coords - rect3.coords) > rect1.spatial_precision).any():
            self.messages.append(
                'inverse transformation resulted in an error of {} (should be <{})'.format(
                    np.max(np.abs(rect3.coords - rect1.coords)), rect1.spatial_precision,
                )
            )
            return
        self.inverse_valid = True

        # *************************************************************************************** **
        self.angles_valid = False

        if rect1.empty:
            self.messages.append(
                'rectangle is empty (height:{}, width:{})'.format(
                    np.linalg.norm(rect1.tl - rect1.bl),
                    np.linalg.norm(rect1.tl - rect1.tr),
                )
            )
            return

        slack_angles = rect2.tr_slack_angles
        assert slack_angles[0] < slack_angles[1]
        if np.prod(np.sign(slack_angles - 90)) != -1:
            self.messages.append(
                'tl-tr-br angle is between {} and {} degree (should be <90 and >90)'.format(*slack_angles)
                )
            return
        slack_angles = rect2.tl_slack_angles
        assert slack_angles[0] < slack_angles[1]
        if np.prod(np.sign(slack_angles - 90)) != -1:
            self.messages.append(
                'bl-tl-tr angle is between {} and {} degree (should be <90 and >90)'.format(*slack_angles)
                )
            return
        slack_angles = rect2.bl_slack_angles
        assert slack_angles[0] < slack_angles[1]
        if np.prod(np.sign(slack_angles - 90)) != -1:
            self.messages.append(
                'br-bl-tl angle is between {} and {} degree (should be <90 and >90)'.format(*slack_angles)
                )
            return
        slack_angles = rect2.br_slack_angles
        assert slack_angles[0] < slack_angles[1]
        if np.prod(np.sign(slack_angles - 90)) != -1:
            self.messages.append(
                'tr-br-bl angle is between {} and {} degree (should be <90 and >90)'.format(*slack_angles)
                )
            return
        self.angles_valid = True

        # *************************************************************************************** **
        self.ratio_valid = False
        ratiomin, ratiomax = rect1.ratio_slaking
        if not ratiomin < rect2.ratio < ratiomax:
            self.messages.append(
                'rectangle proportion is {} and should be between {} and {}'.format(
                    rect2.ratio, ratiomin, ratiomax)
            )
            return
        self.ratio_valid = True
        # *************************************************************************************** **
