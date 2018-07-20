from buzzard._a_proxy import *

class AProxyRaster(AProxy):

    @property
    def fp_stored(self):
        return self._back.fp_stored

    @property
    def fp(self):
        return self._back.fp

    @property
    def band_schema(self):
        return dict(self._back.band_schema)

    @property
    def dtype(self):
        return self._back.dtype

    @property
    def nodata(self):
        """Accessor for first band's nodata value"""
        return self._back.nodata

    def __len__(self):
        """Return the number of bands"""
        return len(self._back)

    @property
    def shared_band_index(self):
        return self._back.shared_band_index

    def get_data(self, fp=None, band=1, mask=None, nodata=None, interpolation='cv_area', dtype=None,
                 op=np.rint):
        """Read data located at `fp` in raster file.

        If `nodata` is set in raster or provided as an argument, fp can lie partially or fully
        outside of raster.

        If `allow_interpolation` is enabled in the DataSource constructor, it is then possible to
        use a `fp` that is not aligned with the source raster, interpolation in then used to
        remap source to `fp`. `nodata` values are also handled and spreaded to the output through
        remapping.

        (experimental) An optional `mask` may be provided. GDAL band sampling is only performed near
        `True` pixels. Current implementation might be extremely slow.

        Parameters
        ----------
        fp: Footprint of shape (Y, X)
            If None: return the full raster
            If Footprint: return this window from the raster
        band: band index or sequence of band index (see `Band Indices` below)
        mask: numpy array of shape (Y, X)
        nodata: Number
            Override self.get_nodata()
        interpolation: one of ('cv_area', 'cv_nearest', 'cv_linear', 'cv_cubic', 'cv_lanczos4')
            Resampling method
        dtype: type
            Override gdal output type
        op: None or vector function
            Rounding function following an interpolation when output type is integer

        Returns
        -------
        numpy.ndarray
            of shape (Y, X) or (Y, X, B)

        Band Indices
        ------------
        | index type | index value     | meaning          |
        |------------|-----------------|------------------|
        | int        | -1              | All bands        |
        | int        | 1, 2, 3, ...    | Band `i`         |
        | complex    | -1j             | All bands masks  |
        | complex    | 0j              | Shared mask band |
        | complex    | 1j, 2j, 3j, ... | Mask of band `i` |

        """
        return self._back.get_data(
            fp=fp,
            band=band,
            nodata=nodata,
            interpolation=interpolation,
            dtype=dtype,
            op=op,
        )

class ABackProxyRaster(ABackProxy):

    def __init__(self, band_schema, dtype, fp_stored, **kwargs):
        super(ABackProxyRaster, self).__init__(rect=fp_stored, **kwargs)

        if self.to_work is not None:
            fp = fp_stored.move(*self.to_work([
                fp_stored.tl, fp_stored.tr, fp_stored.br
            ]))
        else:
            fp = fp_stored

        self.shared_band_index = None
        for i, type in enumerate(band_schema['mask'], 1):
            if type == 'per_dataset':
                self.shared_band_index = i
                break

        self.band_schema = band_schema
        self.dtype = dtype
        self.fp_stored = fp_stored

        self.fp = fp

    @property
    def nodata(self):
        return self.get_nodata(1)

    def get_nodata(self, band=1):
        return self.band_schema['nodata'][band - 1]

    def __len__(self):
        return len(self.band_schema['nodata'])

    def get_data(self, fp, band, mask, nodata, interpolation, dtype, op):
        raise NotImplementedError('ABackProxyRaster.get_data is virtual pure')
