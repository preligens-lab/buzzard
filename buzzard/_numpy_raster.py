import numpy as np

from buzzard._a_stored_raster import AStoredRaster, ABackStoredRaster

class NumpyRaster(AStoredRaster):
    """Proxy to handle numpy array as a raster dataset in buzzard"""

    def __init__(self, ds, fp, array, band_schema, wkt, mode):
        self._arr_shape = array.shape
        self._arr_address = array.__array_interface__['data'][0]
        back = BackNumpyRaster(
            ds._back, fp, array, band_schema, wkt, mode
        )
        super(NumpyRaster, self).__init__(ds=ds, back=back)

    @property
    def array(self):
        """Returns the Raster's full input data as a Numpy array"""
        assert (
            self._arr_address == self._back._arr.__array_interface__['data'][0]
        )
        return self._back._arr.reshape(*self._arr_shape)

class BackNumpyRaster(ABackStoredRaster):
    """Implementation of NumpyRaster"""

    def __init__(self, back_ds, fp, array, band_schema, wkt, mode):
        array = np.atleast_3d(array)
        self._arr = array
        band_count = array.shape[-1]

        if 'nodata' not in band_schema:
            band_schema['nodata'] = [None] * band_count

        if 'interpretation' not in band_schema:
            band_schema['interpretation'] = ['undefined'] * band_count

        if 'offset' not in band_schema:
            band_schema['offset'] = [0.] * band_count

        if 'scale' not in band_schema:
            band_schema['scale'] = [1.] * band_count

        if 'mask' not in band_schema:
            band_schema['mask'] = ['all_valid']

        super(BackNumpyRaster, self).__init__(
            back_ds=back_ds,
            wkt_stored=wkt,
            band_schema=band_schema,
            dtype=array.dtype,
            fp_stored=fp,
            mode=mode,
        )

        self._should_tranform = (
            any(v != 0 for v in band_schema['offset']) or
            any(v != 1 for v in band_schema['scale'])
        )

    def get_data(self, fp, band_ids, dst_nodata, interpolation):
        samplefp = self.build_sampling_footprint(fp, interpolation)
        if samplefp is None:
            return np.full(
                np.r_[fp.shape, len(band_ids)],
                dst_nodata,
                self.dtype
            )
        key = list(samplefp.slice_in(self.fp)) + [self._best_indexers_of_band_ids(band_ids)]
        array = self._arr[key]
        if self._should_tranform:
            array = array * self.band_schema['scale'] + self.band_schema['offset']
        array = self.remap(
            samplefp,
            fp,
            array=array,
            mask=None,
            src_nodata=self.nodata,
            dst_nodata=dst_nodata,
            mask_mode='erode',
            interpolation=interpolation,
        )
        array = array.astype(self.dtype, copy=False)
        return array

    def set_data(self, array, fp, band_ids, interpolation, mask):
        if not fp.share_area(self.fp):
            return
        if not fp.same_grid(self.fp) and mask is None:
            mask = np.ones(fp.shape, bool)

        dstfp = self.fp.intersection(fp)

        # Remap ****************************************************************
        ret = self.remap(
            fp,
            dstfp,
            array=array,
            mask=mask,
            src_nodata=self.nodata,
            dst_nodata=self.nodata or 0,
            mask_mode='erode',
            interpolation=interpolation,
        )
        if mask is not None:
            array, mask = ret
        else:
            array = ret
        del ret
        array = array.astype(self.dtype, copy=False)
        fp = dstfp
        del dstfp

        # Write ****************************************************************
        slices = list(fp.slice_in(self.fp))
        for i in self._indices_of_band_ids(band_ids):
            if mask is not None:
                self._arr[slices + [i]][mask] = array[..., i][mask]
            else:
                self._arr[slices + [i]] = array[..., i]

    def fill(self, value, band_ids):
        for i in self._indices_of_band_ids(band_ids):
            self._arr[..., i] = value

    def delete(self): # pragma: no cover
        raise NotImplementedError('Numpy Raster does no allow deletion, use `close`')

    def close(self):
        super(BackNumpyRaster, self).close()
        del self._arr

    @staticmethod
    def _indices_of_band_ids(band_ids):
        l = []

        for band_id in band_ids:
            if isinstance(band_id, int):
                l.append(band_id - 1)
            else: # pragma: no cover
                raise NotImplementedError("cmon...")
        return l

    @staticmethod
    def _best_indexers_of_band_ids(band_ids):
        l = []

        for band_id in band_ids:
            if isinstance(band_id, int):
                l.append(band_id - 1)
            else: # pragma: no cover
                raise NotImplementedError("cmon...")

        if np.all(np.diff(l) == 1):
            start, stop = l[0], l[-1] + 1
            l = slice(start, stop)
        elif np.all(np.diff(l) == -1):
            start, stop = l[0], l[-1] - 1
            if stop < 0:
                stop = None
            l = slice(start, stop, -1)

        return l
