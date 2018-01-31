"""Tile set creation for Footprint tests"""

from __future__ import division, print_function
import itertools
import attrdict

import numpy as np

from buzzard import Footprint

ALL_LETTERS = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvw"


def make_tile_set(width, reso, tilevec=(1, -10)):
    """
    Tile set creation for Footprint tests
    In a grid of width by width, create a Footprint for all rectangles.

    # *********************************************************************** **
    Ex with=3, reso=(1, -1), tilevec=(1, -10):
    A B C
    D E F
    G H I

    Footprint list (36):
    size 1: A, B, C, D, E, F, G, H, I (9)
    size 2 horizontal: AB, BC, DE, EF, GH, HI (6)
    size 2 vertical: AD, DG, BE, EH, CF, FI (6)
    size 3: AC, DF, GI, AG, BH, CI (6)
    size 4: AE, BF, DH, EI (4)
    size 6: AH, BI, AF, DI (4)
    size 9: AI (1)

    topleft coords:
    A: 0, 30
    B: 1, 30
    C: 2, 30
    D: 0, 20
    E: 1, 20
    F: 2, 20
    G: 0, 10
    H: 1, 10
    I: 2, 10

    # *********************************************************************** **
    All sizes

    1 *********************************************************************** **
    len(fps) = 1
    A
    2 *********************************************************************** **
    len(fps) = 9
    A B
    C D
    3 *********************************************************************** **
    len(fps) = 36
    A B C
    D E F
    G H I
    4 *********************************************************************** **
    len(fps) = 100
    A B C D
    E F G H
    I J K L
    M N O P
    5 *********************************************************************** **
    len(fps) = 225
    A B C D E
    F G H I J
    K L M N O
    P Q R S T
    U V W X Y
    6 *********************************************************************** **
    len(fps) = 441
    A B C D E F
    G H I J K L
    M N O P Q R
    S T U V W X
    Y Z a b c d
    e f g h i j
    7 *********************************************************************** **
    len(fps) = 784
    A B C D E F G
    H I J K L M N
    O P Q R S T U
    V W X Y Z a b
    c d e f g h i
    j k l m n o p
    q r s t u v w
    """
    assert width <= 7
    count = int(width ** 2)
    letters = ALL_LETTERS[0:count]
    reso = np.asarray(reso)
    tilevec = np.asarray(tilevec)

    tlx = 0
    tly = -tilevec[1] * width
    tlxs, tlys = np.meshgrid(
        np.arange(tlx, tilevec[0] * width, tilevec[0]),
        np.arange(tly, tilevec[1] * width, tilevec[1]),
    )

    tl_of_letter = {
        letter: np.array((tlx, tly), dtype=int)
        for letter, tlx, tly in zip(letters, tlxs.flatten(), tlys.flatten())
    }
    br_of_letter = {
        letter: np.array((tlx, tly), dtype=int) + tilevec
        for letter, tlx, tly in zip(letters, tlxs.flatten(), tlys.flatten())
    }

    combos = [
        ''.join([a, b])
        for a, b in itertools.combinations(letters, 2)
        if tl_of_letter[a][0] <= tl_of_letter[b][0] and tl_of_letter[a][1] >= tl_of_letter[b][1]
    ] + list(letters)

    def _footprint_of_letters(letters):
        tl = tl_of_letter[letters[0]]
        br = br_of_letter[letters[-1]]
        diagvec = br - tl
        fp = Footprint(
            tl=tl, size=np.abs(diagvec), rsize=(diagvec / reso)
        )
        return fp
    fps = attrdict.AttrDict({combo: _footprint_of_letters(combo) for combo in combos})
    return fps
