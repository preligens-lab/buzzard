""">>> help(TileMixin)"""

import numpy as np

class TileMixin(object):
    """Private mixin for the Footprint class containing tiling subroutines"""

    _TILE_BOUNDARY_EFFECTS = set(['extend', 'exclude', 'overlap', 'shrink', 'exception'])
    _TILE_OCCURRENCE_BOUNDARY_EFFECTS = set(['extend', 'exception'])
    _TILE_BOUNDARY_EFFECT_LOCI = set(['br', 'tr', 'tl', 'bl'])

    @staticmethod
    def _details_of_tiling_direction(tile_size, overlap_size, raster_size):
        """Compute tiling operation details"""
        increment = tile_size - overlap_size
        if tile_size > raster_size:
            count = 0
        else:
            count = 1 + (raster_size - tile_size) // increment
        gap = raster_size - tile_size - max(0, count - 1) * increment

        def _gen():
            val = 0
            for _ in range(count):
                yield val
                val += increment
        return gap, _gen()

    def _tile_extend_deltax_gen(self, sizex, overlapx):
        gap, gen = self._details_of_tiling_direction(sizex, overlapx, self.rsizex)
        if gap < 0:
            yield 0, sizex
        else:
            for val in gen:
                yield val, sizex
            if gap != 0:
                yield self.rsizex - gap - overlapx, sizex

    def _tile_extend_deltay_gen(self, sizey, overlapy):
        gap, gen = self._details_of_tiling_direction(sizey, overlapy, self.rsizey)
        if gap < 0:
            yield 0, sizey
        else:
            for val in gen:
                yield val, sizey
            if gap != 0:
                yield self.rsizey - gap - overlapy, sizey

    def _tile_exclude_deltax_gen(self, sizex, overlapx):
        _, gen = self._details_of_tiling_direction(sizex, overlapx, self.rsizex)
        for val in gen:
            yield val, sizex

    def _tile_exclude_deltay_gen(self, sizey, overlapy):
        _, gen = self._details_of_tiling_direction(sizey, overlapy, self.rsizey)
        for val in gen:
            yield val, sizey

    def _tile_overlap_deltax_gen(self, sizex, overlapx):
        gap, gen = self._details_of_tiling_direction(sizex, overlapx, self.rsizex)
        if gap < 0:
            raise ValueError(
                'Cannot apply boundary_effect=overlap with a tile(%s) bigger than source(%s)' % (
                    sizex, self.rsizex
                ))
        else:
            for val in gen:
                yield val, sizex
            if gap != 0:
                yield self.rsizex - sizex, sizex

    def _tile_overlap_deltay_gen(self, sizey, overlapy):
        gap, gen = self._details_of_tiling_direction(sizey, overlapy, self.rsizey)
        if gap < 0:
            raise ValueError(
                'Cannot apply boundary_effect=overlap with a tile(%s) bigger than source(%s)' % (
                    sizey, self.rsizey
                ))
        else:
            for val in gen:
                yield val, sizey
            if gap != 0:
                yield self.rsizey - sizey, sizey

    def _tile_raise_deltax_gen(self, sizex, overlapx):
        gap, gen = self._details_of_tiling_direction(sizex, overlapx, self.rsizex)
        if gap != 0:
            raise ValueError(
                ('There is a gap of %d pixel in the x direction, ' +
                 '`gap:%d %% (sizex:%d - overlapx:%d) == 0` was required') % (

                     (gap, gap, sizex, overlapx)))
        for val in gen:
            yield val, sizex

    def _tile_raise_deltay_gen(self, sizey, overlapy):
        gap, gen = self._details_of_tiling_direction(sizey, overlapy, self.rsizey)
        if gap != 0:
            raise ValueError(
                ('There is a gap of %d pixel in the y direction, ' +
                 '`gap:%d %% (sizey:%d - overlapy:%d) == 0` was required') % (
                     (gap, gap, sizey, overlapy)))
        for val in gen:
            yield val, sizey

    def _tile_shrink_deltax_gen(self, sizex, overlapx):
        gap, gen = self._details_of_tiling_direction(sizex, overlapx, self.rsizex)
        if gap < 0:
            yield 0, self.rsizex
        else:
            for val in gen:
                yield val, sizex
            if gap != 0:
                yield self.rsizex - gap - overlapx, gap + overlapx

    def _tile_shrink_deltay_gen(self, sizey, overlapy):
        gap, gen = self._details_of_tiling_direction(sizey, overlapy, self.rsizey)
        if gap < 0:
            yield 0, self.rsizey
        else:
            for val in gen:
                yield val, sizey
            if gap != 0:
                yield self.rsizey - gap - overlapy, gap + overlapy

    def _tile_unsafe(self, size, overlapx, overlapy, boundary_effect, boundary_effect_locus):
        if boundary_effect == 'extend':
            gen_xinfo = self._tile_extend_deltax_gen(size[0], overlapx)
            gen_yinfo = self._tile_extend_deltay_gen(size[1], overlapy)
        elif boundary_effect == 'exclude':
            gen_xinfo = self._tile_exclude_deltax_gen(size[0], overlapx)
            gen_yinfo = self._tile_exclude_deltay_gen(size[1], overlapy)
        elif boundary_effect == 'overlap':
            gen_xinfo = self._tile_overlap_deltax_gen(size[0], overlapx)
            gen_yinfo = self._tile_overlap_deltay_gen(size[1], overlapy)
        elif boundary_effect == 'shrink':
            gen_xinfo = self._tile_shrink_deltax_gen(size[0], overlapx)
            gen_yinfo = self._tile_shrink_deltay_gen(size[1], overlapy)
        elif boundary_effect == 'exception':
            gen_xinfo = self._tile_raise_deltax_gen(size[0], overlapx)
            gen_yinfo = self._tile_raise_deltay_gen(size[1], overlapy)
        else:
            assert False # pragma: no cover

        if boundary_effect_locus == 'br':
            origin = self.tl
            direction = np.array([+1, +1], dtype='int')
        elif boundary_effect_locus == 'tr':
            origin = self.bl
            direction = np.array([+1, -1], dtype='int')
        elif boundary_effect_locus == 'tl':
            origin = self.br
            direction = np.array([-1, -1], dtype='int')
        elif boundary_effect_locus == 'bl':
            origin = self.tr
            direction = np.array([-1, +1], dtype='int')
        else:
            assert False # pragma: no cover

        pxvec = self.pxvec * direction
        pxvec_abs = np.abs(pxvec)

        def _footprint_of_deltacoords(y, x, sizex, sizey):
            tl = pxvec * [x, y] + origin
            rsize = np.asarray([sizex, sizey])
            tl -= rsize * (direction == -1) * (1, -1)
            return self.__class__(
                tl=tl,
                size=[sizex, sizey] * pxvec_abs,
                rsize=rsize,
            )

        infoxs = list(gen_xinfo)
        infoys = list(gen_yinfo)
        deltaxs, deltays = np.meshgrid(
            [deltax for (deltax, _) in infoxs],
            [deltay for (deltay, _) in infoys],
        )
        sizexs, sizeys = np.meshgrid(
            [sizex for (_, sizex) in infoxs],
            [sizey for (_, sizey) in infoys],
        )
        tiles = np.asarray(list(map(
            _footprint_of_deltacoords,
            deltays.flatten(),
            deltaxs.flatten(),
            sizexs.flatten(),
            sizeys.flatten(),
        )), dtype=object)
        if tiles.size > 0:
            tiles = tiles.reshape(len(infoys), len(infoxs))
        if direction[0] == -1:
            tiles = np.fliplr(tiles)
        if direction[1] == -1:
            tiles = np.flipud(tiles)
        return tiles
