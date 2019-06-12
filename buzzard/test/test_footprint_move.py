# pylint: disable=redefined-outer-name
import numpy as np
import pytest
import buzzard as buzz

S = 2 ** 14

def strigify(fp):
    return '{:<25} {!s:<45} {:<25} tl:{!s:<35} tr:{!s:<35} br:{!s:<35} bl:{!s:<35} {} {}'.format(
        fp.angle,
        fp.scale.tolist(),
        np.divide.reduce(fp.scale),
        fp.tl,
        fp.tr,
        fp.br,
        fp.bl,
        str(fp.aff6).replace('\n', ''),
        fp.rsize,
    )

def pytest_generate_tests(metafunc):
    with buzz.Env(allow_complex_footprint=1, significant=10):
        fp0 = buzz.Footprint(rsize=(S, S), size=(S, S), tl=(50000, 50000))
        transfos = [
            (fp0,
             fp0,
             'identity',
            ),
            (fp0,
             buzz.Footprint(rsize=(S, S), size=(S * 2, S * 2), tl=(50000, 50000)),
             'double unit',
            ),
            (fp0,
             buzz.Footprint(rsize=(S, S), gt=(50000, 1, 0, 50000, 0, -2)),
             'double y unit',
            ),
            (fp0,
             fp0.intersection(fp0, rotation=45).clip(0, 0, S, S),
             'rotation 45',
            ),
            (fp0,
             buzz.Footprint(rsize=(S, S), size=(S * 2, S * 2), tl=(50000, 50000)).intersection(fp0.dilate(S), rotation=45).clip(0, 0, S, S),
             'rotation 45 and double unit'
            ),
            (fp0,
             buzz.Footprint(rsize=(S, S), gt=(50000, 1, 0, 50000, 0, -2)).intersection(fp0.dilate(S), rotation=45).clip(0, 0, S, S),
             'rotation 45 and double y unit'
            ),
            (fp0,
             buzz.Footprint(rsize=(S, S), gt=(50000, 1, 0, 50000, 0, 1)),
             'mirror',
            ),
        ]

    tests = []
    for src_fp, trg_fp, transfo_name in transfos:
        assert np.all(src_fp.rsize == trg_fp.rsize), (src_fp.rsize, trg_fp.rsize)

        for scalex in [1, -1]:
            for scaley in [1, -1]:
                for rot in [0, 45, -45, 90, -90, -135, 135, 180, -180]:
                    for noise_factor in [0, 1e-6]:
                        tests.append((
                            transfo_name, src_fp, trg_fp, scalex, scaley, rot, noise_factor
                        ))
    metafunc.parametrize(
        argnames='transfo_name,src_fp,trg_fp,scalex,scaley,rot,noise_factor',
        argvalues=tests,
    )

def test_move(transfo_name, src_fp, trg_fp, scalex, scaley, rot, noise_factor):
    with buzz.Env(allow_complex_footprint=1, significant=10):

        src_fp = src_fp.dilate(S).intersection(
            src_fp.dilate(S), rotation=src_fp.angle + rot, scale=src_fp.scale * [scalex, scaley]
        ).clip(0, 0, S, S)
        trg_fp = trg_fp.dilate(S).intersection(
            trg_fp.dilate(S), rotation=trg_fp.angle + rot, scale=trg_fp.scale * [scalex, scaley]
        ).clip(0, 0, S, S)

        print('// src_properties:', strigify(src_fp))
        print('// trg_properties:', strigify(trg_fp))
        assert np.all(src_fp.rsize == trg_fp.rsize)

        print('->{} noise at {:.1e}'.format(transfo_name, noise_factor))
        tl, tr, br = trg_fp.tl, trg_fp.tr, trg_fp.br
        tl, tr, br = np.asarray([tl, tr, br]) + np.random.rand(3, 2) * noise_factor

        out_fp0 = src_fp.move(tl, tr, br)
        print('  without snap:   ', strigify(out_fp0))

        # Assert that `Footprint.move` does no give strange results
        assert trg_fp.almost_equals(out_fp0)

        out_fp1 = src_fp.move(tl, tr, br, True)
        print('  with snap:      ', strigify(out_fp1))

        # Assert that `new function` does no give strange results
        assert trg_fp.almost_equals(out_fp1)

        if src_fp.angle == trg_fp.angle and np.isclose(0, (src_fp.angle + 360) % 90):
            # Assert that angles are be fully preserved when rotation if a multiple of 90
            assert out_fp1.angle == src_fp.angle
