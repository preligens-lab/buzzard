""">>> help(RasterGetSetMixin)"""

import numpy as np
from osgeo import gdal


class RasterGetSetMixin(object):
    """Private mixin for the Raster class containing subroutines for read and writes"""

    def _gdalband_of_index(self, index):
        """Convert a band index to a gdal band"""
        if isinstance(index, int):
            return self._gdal_ds.GetRasterBand(index)
        else:
            return self._gdal_ds.GetRasterBand(int(index.imag)).GetMaskBand()

    def _sample_bands(self, fp, samplefp, bands, mask, interpolation, onodata):
        """Pull raster values from gdal

        Parameters:
        fp: Footprint
            Footprint of mask
        samplefp: Footprint
            Footprint to query
        """

        rtlx, rtly = self.fp.spatial_to_raster(samplefp.tl)

        assert rtlx >= 0 and rtlx < self.fp.rsizex
        assert rtly >= 0 and rtly < self.fp.rsizey
        if mask is None:
            samplebands = []
            for i in bands:
                a = self._gdal_ds.GetRasterBand(i).ReadAsArray(
                    int(rtlx), int(rtly), int(samplefp.rsizex), int(samplefp.rsizey)
                )
                if a is None:
                    raise ValueError('Could not read array (gdal error: `{}`)'.format(
                        gdal.GetLastErrorMsg()
                    ))
                samplebands.append(a)
            samplebands = np.stack(samplebands, -1)
        else:
            samplemask = self._remap(
                fp,
                samplefp,
                interpolation=interpolation,
                array=None,
                mask=mask,
                nodata=None,
                mask_mode='dilate',
            )
            samplebands = np.full(np.r_[samplefp.shape, len(bands)], onodata, self.dtype)
            assert samplemask.shape == samplebands.shape[:2]

            # Read each raster bands split in tiles and copy bands values outside of the mask
            for tile, band, dim in self._blocks_of_footprint(samplefp, bands):
                leftx, topy = self.fp.spatial_to_raster(tile.tl)
                tileslice = tile.slice_in(samplefp)
                tilemask = samplemask[tileslice]
                gdalband = self._gdalband_of_index(band)
                # Use slices on x, y coordinates of mask marts to get band values
                for sly, slx in self._slices_of_mask(tilemask):
                    a = gdalband.ReadAsArray(
                        int(leftx + slx.start),
                        int(topy + sly.start),
                        int(slx.stop - slx.start),
                        int(sly.stop - sly.start),
                    )
                    if a is None:
                        raise ValueError('Could not read array (gdal error: `{}`)'.format(
                            gdal.GetLastErrorMsg()
                        ))
                    samplebands[:, :, dim][tileslice][sly, slx] = a
        return samplebands

    def _set_data_unsafe(self, array, fp, bands, interpolation, mask, op):
        """Push raster values to gdal"""
        if not fp.share_area(self.fp):
            return
        dstfp = self.fp.intersection(fp)
        if array.dtype == np.int8:
            array = array.astype('uint8')
        array, mask = self._remap(
            fp,
            dstfp,
            interpolation=interpolation,
            array=array,
            mask=mask,
            nodata=self.nodata,
            mask_mode='erode',
        )
        if op:
            array = op(array)
        array = array.astype(self.dtype)

        fp = dstfp
        del dstfp

        for tile, band, dim in self._blocks_of_footprint(fp, bands):
            leftx, topy = self.fp.spatial_to_raster(tile.tl)
            tilemask = mask[tile.slice_in(fp)]
            tilearray = array[:, :, dim][tile.slice_in(fp)]
            gdalband = self._gdalband_of_index(band)
            for sl in self._slices_of_mask(tilemask):
                a = tilearray[sl]
                assert a.ndim == 2
                x = int(sl[1].start + leftx)
                y = int(sl[0].start + topy)
                assert x >= 0
                assert y >= 0
                assert x + a.shape[1] <= self.fp.rsizex
                assert y + a.shape[0] <= self.fp.rsizey
                gdalband.WriteArray(a, x, y)

        self._gdal_ds.FlushCache()

    @staticmethod
    def _blocks_of_footprint(fp, bands):
        for i, band in enumerate(bands):
            yield fp, band, i # Todo use tile_count and gdal block size

    @staticmethod
    def _slices_of_vector(vec):
        """Generates slices of oneline mask parts"""
        assert vec.ndim == 1
        diff = np.diff(np.r_[[False], vec, [False]].astype('int'))
        starts = np.where(diff == 1)[0]
        ends = np.where(diff == -1)[0]
        for s, e in zip(starts, ends):
            yield slice(s, e)

    @classmethod
    def _slices_of_mask(cls, mask):
        """Generates slices of mask parts"""
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
                for xslice in cls._slices_of_vector(mask[ystart]):
                    yield yslice, xslice
            if begin_group:
                ystart = y

            # Loop control
            if stop:
                break
            y += 1
