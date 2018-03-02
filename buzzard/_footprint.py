""">>> help(Footprint)"""

# pylint: disable=too-many-lines

from __future__ import division, print_function
import logging
import itertools
import numbers

import shapely
import shapely.geometry as sg
import affine
import numpy as np
import scipy
from osgeo import gdal
from osgeo import ogr
from osgeo import osr
from six.moves import filterfalse
import scipy.ndimage as ndi

from buzzard import _tools
from buzzard._tools import conv
from buzzard._env import env
from buzzard._footprint_tile import TileMixin
from buzzard._footprint_intersection import IntersectionMixin

LOGGER = logging.getLogger('buzzard')

class Footprint(TileMixin, IntersectionMixin):
    """Constant object representing the location and size of a spatially localized raster.

    The Footprint
    - is a toolbox class designed to locate a rectangle in both image space and geometry space
    - its main purpose is to simplify the manipulation of windows in rasters
    - has many accessors
    - has many algorithms
    - is a constant object
    - is designed to work with any rectangle in space (like non north-up/west-left rasters)
    - is independent from projections, units and files
    - uses [affine](https://github.com/sgillies/affine) library internally for conversions

    Methods
    -------

    Method category                     | Method names
    ------------------------------------|-----------------------------------------------------------
    Footprint construction              |
        from scratch                    | __init__, of_extent
        from Footprint                  | __and__, intersection, erode, dilate, ...
    Conversion                          | extent, coords, geom, __geo_interface__
    Accessors                           |
        Spatial - Size and vectors      | size, width, height, diagvec, ...
        Spatial - Coordinates           | tl, bl, br, tr, ...
        Spatial - Misc                  | area, length, semiminoraxis, ...
        Raster - Size                   | rsize, rwidth, rheight, ...
        Raster - Indices                | rtl, rbl, rbr, ttr, ...
        Raster - Misc                   | rarea, rlength, rsemiminoraxis, ...
        Affine transformations          | pxsize, pxvec, angle, ...
    Binary predicates                   | __eq__, ...
    Numpy                               | shape, meshgrid_raster, meshgrid_spatial, slice_in, ...
    Coordinates conversions             | spatial_to_raster, raster_to_spatial
    Geometry / Raster conversions       | find_polygons, burn_polygons, ...
    Tiling                              | tile, tile_count, tile_occurrence
    Serialization                       | __str__, ...

    Informations on geo transforms (gt) and affine matrices
    -------------------------------------------------------
    http://www.perrygeo.com/python-affine-transforms.html
    https://pypi.python.org/pypi/affine/1.0

    GDAL ordering
    | c   | a                | b            | f   | d               | e                 |
    |-----|------------------|--------------|-----|-----------------|-------------------|
    | tlx | width of a pixel | row rotation | tly | column rotation | height of a pixel |
    >>> c, a, b, f, d, e = fp.gt
    >>> tlx, dx, rx, tly, ry, dy = fp.gt

    Matrix ordering
    | a                | b            | c   | d               | e                 | f   |
    |------------------|--------------|-----|-----------------|-------------------|-----|
    | width of a pixel | row rotation | tlx | column rotation | height of a pixel | tly |
    >>> a, b, c, d, e, f = fp.aff6
    >>> dx, rx, tlx, ry, dy, tly = fp.aff6
    """

    __slots__ = ['_tl', '_bl', '_br', '_tr', '_aff', '_rsize', '_significant_min']

    # Footprint construction ******************************************************************** **
    # Footprint construction - from scratch ***************************************************** **
    def __init__(self, **kwargs):
        """Constructor

        Parameters
        ----------
        tl: (nbr, nbr)
            raster spatial top left coordinates
        gt: (nbr, nbr, nbr, nbr, nbr)
            geotransforms with GDAL ordering
        size: (nbr, nbr)
            Size of Footprint in space (unsigned)
        rsize: (int, int)
            Size of raster in pixel (unsigned integers)

        Usage 1
        -------
        >>> buzz.Footprint(tl=(0, 10), size=(10, 10), rsize=(100, 100))

        Usage 2
        -------
        >>> buzz.Footprint(gt=(0, .1, 0, 10, 0, -.1), rsize=(100, 100))
        """
        if 'rsize' not in kwargs:
            raise ValueError('Missing `rsize` parameter')
        rsize = np.asarray(kwargs.pop('rsize'), dtype='int32')
        if rsize.shape != (2,):
            raise ValueError('Invalid rsize shape `%s`' % str(rsize.shape))
        if not np.isfinite(rsize).all() or (rsize <= 0).any():
            raise ValueError('Invalid rsize value `%s`' % rsize)

        if 'gt' in kwargs:
            gt = np.asarray(kwargs.pop('gt'), dtype='float64')
            if gt.shape != (6,):
                raise ValueError('Invalid gt shape `%s`' % gt.shape)
            if not np.isfinite(gt).all():
                raise ValueError('Invalid gt value `%s`' % gt)
            c, a, b, f, d, e = gt
        elif 'tl' in kwargs and 'size' in kwargs:
            tl = np.asarray(kwargs.pop('tl'), dtype='float64')
            if tl.shape != (2,):
                raise ValueError('Invalid tl shape `%s`' % tl.shape)
            if not np.isfinite(tl).all():
                raise ValueError('Invalid tl value `%s`' % tl)
            b, d = 0., 0.
            c, f = tl

            size = np.asarray(kwargs.pop('size'), dtype='float64')
            if size.shape != (2,):
                raise ValueError('Invalid size shape `%s`' % size.shape)
            if not np.isfinite(size).all() or (size <= 0).any():
                raise ValueError('Invalid size value `%s`' % size)
            a = size[0] / float(rsize[0])
            e = -size[1] / float(rsize[1])
        else:
            raise ValueError('Provide `size & gt` or `rsize & size & tl`')
        if kwargs:
            raise ValueError('Unknown parameters [{}]'.format(kwargs.keys()))

        if a + b == 0 or d + e == 0:
            raise ValueError('Scale should not be 0')
        if b != 0 or d != 0 or a <= 0 or e >= 0:
            if not env.allow_complex_footprint:
                arr = np.asarray([[a, b, c], [d, e, f]])
                arr = np.array2string(arr, precision=17).replace('\n', ' ')
                raise ValueError((
                    'Creating a non north-up/west-left footprint, '
                    'env.allow_complex_footprint is False, '
                    'affine matrix:{}'
                ).format(arr))
            if env.warnings:
                arr = np.asarray([[a, b, c], [d, e, f]])
                arr = np.array2string(arr, precision=17).replace('\n', ' ')
                LOGGER.warning((
                    'Creating a non north-up/west-left footprint, '
                    'this feature has not been fully tests, '
                    'affine matrix:{}'
                ).format(arr))

        rsizex, rsizey = rsize
        aff = affine.Affine(a, b, c, d, e, f)

        self._tl = c, f
        self._bl = aff * (0, rsizey)
        self._br = aff * (rsizex, rsizey)
        self._tr = aff * (rsizex, 0)
        self._aff = aff
        self._rsize = rsize

        self._tl = np.asarray(self._tl, dtype=np.float64)
        self._bl = np.asarray(self._bl, dtype=np.float64)
        self._br = np.asarray(self._br, dtype=np.float64)
        self._tr = np.asarray(self._tr, dtype=np.float64)
        self._rsize = np.asarray(self._rsize, dtype=env.default_index_dtype)

        rect = _tools.Rect(*self.coords)
        self._significant_min = rect.significant_min((rect.size / self._rsize).min())


    # Footprint construction - from Footprint *************************************************** **
    def __and__(self, other):
        """Returns Footprint.intersection"""
        return self.intersection(other)

    @classmethod
    def of_extent(cls, extent, scale):
        """Create a Footprint from a rectangle extent and a scale

        Parameters
        ----------
        extent: (nbr, nbr, nbr, nbr)
            Spatial coordinates of (minx, maxx, miny, maxy) defining a rectangle
        scale: nbr or (nbr, nbr)
            Resolution of output Footprint
            if nbr: resolution = [a, -a]
            if (nbr, nbr): resolution [a, b]
        """
        # Check extent parameter
        extent = np.asarray(extent, dtype='float64')
        if extent.shape != (4,):
            raise ValueError('Invalid extent shape `{}`'.format(extent.shape))
        if not np.isfinite(extent).all():
            raise ValueError('Invalid extent value `{}`'.format(extent))
        if extent[0] == extent[1] or extent[2] == extent[3]:
            raise ValueError('Empty extent')

        # Check scale parameter
        scale = np.asarray(scale, dtype='float64')
        if scale.ndim == 0:
            scale = np.asarray([scale, -scale], dtype='float64')
        elif scale.shape == (1,):
            scale = np.asarray([scale[0], -scale[0]], dtype='float64')
        elif scale.shape != (2,):
            raise ValueError('scale has shape {} instead of (2,)'.format(scale.shape))
        if (scale == 0).any():
            raise ValueError('scale should be greater than 0')

        # Work
        minx, maxx, miny, maxy = extent
        rect = _tools.Rect(
            tl=(minx, maxy),
            bl=(minx, miny),
            br=(maxx, miny),
            tr=(maxx, maxy),
        )
        pxsize = np.abs(scale)
        significant_min = rect.significant_min(pxsize.min())
        if env.significant <= significant_min:
            raise RuntimeError('`env.significant` of value {} should be at least {}'.format(
                env.significant, significant_min,
            ))

        abstract_grid_density = rect.abstract_grid_density(pxsize.min())
        rsize = np.around(rect.size / pxsize * abstract_grid_density, 0) / abstract_grid_density
        size = rsize * pxsize
        return cls(tl=rect.tl, size=size, rsize=rsize)

    def clip(self, startx, starty, endx, endy):
        """Construct a new Footprint from by clipping self using pixel indices

        To clip using coordinates see `Footprint.intersection`.

        Parameters
        ----------
        startx: int or None
            Same rules as regular python slicing
        starty: int or None
            Same rules as regular python slicing
        endx: int or None
            Same rules as regular python slicing
        endy: int or None
            Same rules as regular python slicing

        Returns
        -------
        Footprint
        """
        startx, endx, _ = slice(startx, endx).indices(self.rsizex)
        starty, endy, _ = slice(starty, endy).indices(self.rsizey)

        rsize = np.asarray(
            [endx - startx, endy - starty]
        )
        tl = self.tl + [startx, starty] * self.pxvec
        size = rsize * self.pxsize
        gt = self.gt
        gt[0] = tl[0]
        gt[3] = tl[1]

        return self.__class__(
            gt=gt,
            rsize=rsize,
        )

    def _morpho(self, scount):
        aff = self._aff * affine.Affine.translation(-scount, -scount)
        return Footprint(
            gt=aff.to_gdal(),
            rsize=(self.rsize + 2 * scount),
        )

    def erode(self, count):
        """Construct a new Footprint from self, eroding all edges by `count` pixels"""
        assert count >= 0
        assert count == int(count)
        return self._morpho(-count)

    def dilate(self, count):
        """Construct a new Footprint from self, dilating all edges by `count` pixels"""
        assert count >= 0
        assert count == int(count)
        return self._morpho(count)

    def intersection(self, *objects, **kwargs):
        """intersection(self, *objects, scale='self', rotation='auto', alignment='auto',
                        homogeneous=False)

        Construct a Footprint bounding the intersection of geometric objects, self being one of the
        of input geometry. Inputs' intersection is always within output Footprint.

        Parameters
        ----------
        *objects: *object
            Any object with a __geo_interface__ attribute defining a geometry, like a Footprint
            or a shapely object.
        scale: one of {'self', 'highest', 'lowest'} or (nbr, nbr) or nbr
            'self': Output Footprint's resolution is the same as self
            'highest': Output Footprint's resolution is the highest one among the input Footprints
            'lowest': Output Footprint's resolution is the lowest one among the input Footprints
            (nbr, nbr): Signed pixel size, aka scale
            nbr: Signed pixel width. Signed pixel height is assumed to be -width
        rotation: one of {'auto', 'fit'} or nbr
            'auto'
                If `scale` designate a Footprint object, its rotation is chosen
                Else, self's rotation is chosen
            'fit': Output Footprint is the rotated minimum bounding rectangle
            nbr: Angle in degree
        alignment: {'auto', 'tl', (nbr, nbr)}
            'auto'
                If `scale` and `rotation` designate the same Footprint object, its alignment
                    is chosen
                Else, 'tl' alignment is chosen
            'tl': Ouput Footprint's alignement is the top left most point of the bounding rectangle
                of the intersection
            (nbr, nbr): Coordinate of a point that lie on the grid.
                This point can be anywhere in space.
        homogeneous: bool
            False: No effect
            True: Raise an exception if all input Footprints do not lie on the same grid as self.

        Returns
        -------
        Footprint
        """
        # Retrieve *objects
        objects = list(objects)
        if not objects:
            raise ValueError('No other Footprint provided arguments')

        def _ensure_shapely(geom):
            if not isinstance(geom, sg.base.BaseGeometry):
                geom = sg.shape(geom.__geo_interface__)
            return geom

        def _partition(pred, iterable):
            t1, t2 = itertools.tee(iterable)
            return list(filterfalse(pred, t1)), list(map(_ensure_shapely, filter(pred, t2)))

        def _classify(obj):
            if isinstance(obj, self.__class__):
                return False
            if hasattr(obj, '__geo_interface__'):
                return True
            raise TypeError("intersection() argument must be a Footprint or a geometry, not %s" % (
                type(obj)
            ))

        footprints, geoms = _partition(_classify, [self] + objects)

        # Retrieve resolution
        resolution = kwargs.pop('scale', 'self')
        if isinstance(resolution, str):
            if resolution not in self._INTERSECTION_RESOLUTIONS:
                raise ValueError('bad resolution parameter')
        else:
            resolution = np.asarray(resolution, dtype='float64')
            if resolution.ndim == 0:
                resolution = np.asarray([resolution, -resolution], dtype='float64')
            elif resolution.shape == (1,):
                resolution = np.asarray([resolution[0], -resolution[0]], dtype='float64')
            elif resolution.shape != (2,):
                raise ValueError('resolution has shape {}'.format(resolution.shape))
            if (resolution == 0).any():
                raise ValueError('resolution should be different than zero')

        # Retrieve rotation
        rotation = kwargs.pop('rotation', 'auto')
        if isinstance(rotation, str):
            if rotation not in self._INTERSECTION_ROTATIONS:
                raise ValueError()
        else:
            rotation = float(rotation)

        # Retrieve alignment
        alignment = kwargs.pop('alignment', 'auto')
        if isinstance(alignment, str):
            if alignment not in self._INTERSECTION_ALIGNMENTS:
                raise ValueError('Unknown alignment value')
        else:
            alignment = np.asarray(alignment, dtype='float64')
            if alignment.shape != (2,):
                raise ValueError('alignment has shape {}'.format(alignment.shape))

        # Retrieve homogeneous
        homogeneous = bool(kwargs.pop('homogeneous', False))
        if homogeneous:
            for fp in footprints:
                if not self.same_grid(fp):
                    raise ValueError(
                        '{} does not lie on the same grid as self: {}'.format(fp, self)
                    )

        if kwargs:
            raise ValueError('Unknown keyword arguments')
        return self._intersection_unsafe(
            footprints, geoms, resolution, rotation, alignment
        )

    def move(self, tl, tr=None, br=None):
        """Create a copy of self moved by an Affine transformation by providing new points.
        `rsize` is always conserved

        Usage cases
        -----------
        | tl    | tr    | br    | Affine transformations possible                                   |
        |-------|-------|-------|-------------------------------------------------------------------|
        | coord | None  | None  | Translation                                                       |
        | coord | coord | None  | Translation, Rotation, Scale x and y uniformly with positive real |
        | coord | coord | coord | Translation, Rotation, Scale x and y independently with reals     |

        Parameters
        ----------
        tl: Footprint
            New top left coordinates
        tr: Footprint
            New top right coordinates
        br: Footprint
            New bottom right coordinates

        Returns
        -------
        Footprint
        """
        tl = np.asarray(tl, dtype=np.float64)
        if tl.shape != (2,):
            raise ValueError('Bad tl shape') # pragma: no cover

        if tr is not None:
            tr = np.asarray(tr, dtype=np.float64)
            if tr.shape != (2,):
                raise ValueError('Bad tr shape') # pragma: no cover
            if br is not None:
                br = np.asarray(br, dtype=np.float64)
                if br.shape != (2,):
                    raise ValueError('Bad br shape') # pragma: no cover
        else:
            if br is not None:
                raise ValueError('If br present, bl should be present too') # pragma: no cover


        if tr is None:
            angle = self.angle
            scale = self.scale
        elif br is None:
            lrvec = tr - tl
            angle = float(np.arctan2(lrvec[1], lrvec[0]) * 180. / np.pi)
            scale = np.linalg.norm(lrvec) / np.linalg.norm(self.lrvec) * self.scale
        else:
            rect = _tools.Rect(tl, tl + (br - tr), br, tr)
            scale = rect.scale(self.rsize)
            angle = rect.angle

            significant_min = rect.significant_min(np.abs(scale).min())
            if env.significant <= significant_min:
                raise RuntimeError('`env.significant` of value {} should be at least {}'.format(
                    env.significant, significant_min,
                ))

            slack_angles = rect.tr_slack_angles
            assert slack_angles[0] < slack_angles[1]
            if np.prod(np.sign(slack_angles - 90)) != -1:
                raise ValueError(
                    'tl-tr-br angle is between {} and {} degree (should be <90 and >90)'.format(*slack_angles)
                )

        aff = (
            affine.Affine.translation(*tl) *
            affine.Affine.rotation(angle) *
            affine.Affine.scale(*scale)
        )
        return self.__class__(
            gt=aff.to_gdal(),
            rsize=self.rsize,
        )

    # Export ************************************************************************************ **
    @property
    def extent(self):
        """Get the Footprint's extent (`x` then `y`)

        Example
        -------
        >>> minx, maxx, miny, maxy = fp.extent
        >>> plt.imshow(arr, extent=fp.extent)
        """
        points = np.r_["1,0,2", self.coords]
        return np.asarray([
            points[:, 0].min(), points[:, 0].max(),
            points[:, 1].min(), points[:, 1].max(),
        ])

    @property
    def bounds(self):
        """Get the Footprint's bounds (`min` then `max`)

        Example
        -------
        >>> minx, miny, maxx, maxy = fp.bounds
        """
        points = np.r_["1,0,2", self.coords]
        return np.asarray([
            points[:, 0].min(), points[:, 1].min(),
            points[:, 0].max(), points[:, 1].max(),
        ])

    @property
    def coords(self):
        """Get corners coordinates

        Example
        -------
        >>> tl, bl, br, tr = fp.coords
        """
        return np.asarray(
            [self.tl, self.bl, self.br, self.tr]
        )

    @property
    def poly(self):
        """Convert self to shapely.geometry.Polygon"""
        return sg.Polygon([
            self.tl, self.bl, self.br, self.tr, self.tl
        ])

    @property
    def __geo_interface__(self):
        return {
            'type': 'Polygon',
            'coordinates': [[
                [self.tlx, self.tly],
                [self.blx, self.bly],
                [self.brx, self.bry],
                [self.trx, self.try_],
                [self.tlx, self.tly],
            ]],
        }

    # Accessors ********************************************************************************* **
    # Accessors - Spatial - Size and vectors **************************************************** **
    @property
    def size(self):
        """Spatial distances: (||raster left - raster right||, ||raster top - raster bottom||)"""
        return np.abs(~affine.Affine.rotation(self.angle) * self.diagvec, dtype=np.float64)

    @property
    def sizex(self):
        """Spatial distance: ||raster left - raster right||"""
        return float(self.size[0])

    @property
    def sizey(self):
        """Spatial distance: ||raster top - raster bottom||"""
        return float(self.size[1])

    @property
    def width(self):
        """Spatial distance: ||raster left - raster right||, alias for sizex"""
        return float(self.sizex)

    @property
    def height(self):
        """Spatial distance: ||raster top - raster bottom||, alias for sizey"""
        return float(self.sizey)

    @property
    def w(self):
        """Spatial distance: ||raster left - raster right||, alias for sizex"""
        return float(self.sizex)

    @property
    def h(self):
        """Spatial distance: ||raster top - raster bottom||, alias for sizey"""
        return float(self.sizey)

    @property
    def lrvec(self):
        """Spatial vector: (raster right - raster left)"""
        return self.tr - self.tl

    @property
    def tbvec(self):
        """Spatial vector: (raster bottom - raster top)"""
        return self.bl - self.tl

    @property
    def diagvec(self):
        """Spatial vector: (raster bottom right - raster top left)"""
        return self.br - self.tl

    # Accessors - Spatial - Coordinates ********************************************************* **
    @property
    def tl(self):
        """Spatial coordinates: raster top left (x, y)"""
        return self._tl

    @property
    def tlx(self):
        """Spatial coordinate: raster top left (x)"""
        return float(self._tl[0])

    @property
    def tly(self):
        """Spatial coordinate: raster top left (y)"""
        return float(self._tl[1])

    @property
    def bl(self):
        """Spatial coordinates: raster bottom left (x, y)"""
        return self._bl

    @property
    def blx(self):
        """Spatial coordinate: raster bottom left (x)"""
        return float(self._bl[0])

    @property
    def bly(self):
        """Spatial coordinate: raster bottom left (y)"""
        return float(self._bl[1])

    @property
    def br(self):
        """Spatial coordinates: raster bottom right (x, y)"""
        return self._br

    @property
    def brx(self):
        """Spatial coordinate: raster bottom right (x)"""
        return float(self._br[0])

    @property
    def bry(self):
        """Spatial coordinate: raster bottom right (y)"""
        return float(self._br[1])

    @property
    def tr(self):
        """Spatial coordinates: raster top right (x, y)"""
        return self._tr

    @property
    def trx(self):
        """Spatial coordinate: raster top right (x)"""
        return float(self._tr[0])

    @property
    def try_(self):
        """Spatial coordinate: raster top right (y)"""
        return float(self._tr[1])

    @property
    def t(self):
        """Spatial coordinates: raster top center (x, y)"""
        return np.array([self.tx, self.ty], dtype=np.float64)

    @property
    def tx(self):
        """Spatial coordinate: raster top center (x)"""
        return (self.tlx + self.trx) / 2.

    @property
    def ty(self):
        """Spatial coordinate: raster top center (y)"""
        return (self.tly + self.try_) / 2.

    @property
    def l(self):
        """Spatial coordinates: raster center left (x, y)"""
        return np.array([self.lx, self.ly], dtype=np.float64)

    @property
    def lx(self):
        """Spatial coordinate: raster center left (x)"""
        return (self.tlx + self.blx) / 2.

    @property
    def ly(self):
        """Spatial coordinate: raster center left (y)"""
        return (self.tly + self.bly) / 2.

    @property
    def b(self):
        """Spatial coordinates: raster bottom center (x, y)"""
        return np.array([self.bx, self.by], dtype=np.float64)

    @property
    def bx(self):
        """Spatial coordinate: raster bottom center (x)"""
        return (self.blx + self.brx) / 2.

    @property
    def by(self):
        """Spatial coordinate: raster bottom center (y)"""
        return (self.bly + self.bry) / 2.

    @property
    def r(self):
        """Spatial coordinates: raster center right (x, y)"""
        return np.array([self.rx, self.ry], dtype=np.float64)

    @property
    def rx(self):
        """Spatial coordinate: raster center right (x)"""
        return (self.brx + self.trx) / 2.

    @property
    def ry(self):
        """Spatial coordinate: raster center right (y)"""
        return (self.bry + self.try_) / 2.

    @property
    def c(self):
        """Spatial coordinates: raster center (x, y)"""
        return np.array([self.cx, self.cy], dtype=np.float64)

    @property
    def cx(self):
        """Spatial coordinate: raster center (x)"""
        return (self.tx + self.bx) / 2

    @property
    def cy(self):
        """Spatial coordinate: raster center (y)"""
        return (self.ly + self.ry) / 2

    # Accessors - Spatial - Misc **************************************************************** **
    @property
    def semiminoraxis(self):
        """Spatial distance: half-size of the smaller side"""
        return float(np.min(self.size) / 2.)

    @property
    def semimajoraxis(self):
        """Spatial distance: half-size of the bigger side"""
        return float(np.max(self.size) / 2.)

    @property
    def area(self):
        """Area: pixel count"""
        return np.prod(self.size)

    @property
    def length(self):
        """Length: circumference of the outer ring"""
        return np.sum(self.size) * 2

    # Accessors - Raster - Size ***************************************************************** **
    @property
    def rsize(self):
        """Pixel quantities: (pixel per line, pixel per column)"""
        return self._rsize

    @property
    def rsizex(self):
        """Pixel quantity: pixel per line"""
        return int(self._rsize[0])

    @property
    def rsizey(self):
        """Pixel quantity: pixel per column"""
        return int(self._rsize[1])

    @property
    def rwidth(self):
        """Pixel quantity: pixel per line, alias for rsizex"""
        return int(self.rsizex)

    @property
    def rheight(self):
        """Pixel quantity: pixel per column, alias for rsizey"""
        return int(self.rsizey)

    @property
    def rw(self):
        """Pixel quantity: pixel per line, alias for rsizex"""
        return int(self.rsizex)

    @property
    def rh(self):
        """Pixel quantity: pixel per column, alias for rsizey"""
        return int(self.rsizey)

    # Accessors - Raster - Indices ************************************************************** **
    @property
    def rtl(self):
        """Indices: raster top left pixel (x=0, y=0)"""
        return np.array([0, 0], dtype=env.default_index_dtype)

    @property
    def rtlx(self):
        """Index: raster top left pixel (x=0)"""
        return 0

    @property
    def rtly(self):
        """Index: raster top left pixel (y=0)"""
        return 0

    @property
    def rbl(self):
        """Indices: raster bottom left pixel (x=0, y)"""
        return np.array([self.rblx, self.rbly], dtype=env.default_index_dtype)

    @property
    def rblx(self):
        """Index: raster bottom left pixel (x=0)"""
        return 0

    @property
    def rbly(self):
        """Index: raster bottom left pixel (y)"""
        return self.rsizey - 1

    @property
    def rbr(self):
        """Indices: raster bottom right pixel (x, y)"""
        return np.array([self.rbrx, self.rbry], dtype=env.default_index_dtype)

    @property
    def rbrx(self):
        """Index: raster bottom right pixel (x)"""
        return self.rsizex - 1

    @property
    def rbry(self):
        """Index: raster bottom right pixel (y)"""
        return self.rsizey - 1

    @property
    def rtr(self):
        """Indices: raster top right pixel (x, y=0)"""
        return np.array([self.rtrx, self.rtry], dtype=env.default_index_dtype)

    @property
    def rtrx(self):
        """Index: raster top right pixel (x)"""
        return self.rsizex - 1

    @property
    def rtry(self):
        """Index: raster top right pixel (y=0)"""
        return 0

    @property
    def rt(self):
        """Indices: raster top center pixel (x truncated, y=0)"""
        return np.array([self.rtx, self.rty], dtype=env.default_index_dtype)

    @property
    def rtx(self):
        """Index: raster top center pixel (x truncated)"""
        return int((self.rsizex - 1) / 2)

    @property
    def rty(self):
        """Index: raster top center pixel (y=0)"""
        return 0

    @property
    def rl(self):
        """Indices: raster center left pixel (x=0, y truncated)"""
        return np.array([self.rlx, self.rly], dtype=env.default_index_dtype)

    @property
    def rlx(self):
        """Index: raster center left pixel (x=0)"""
        return 0

    @property
    def rly(self):
        """Index: raster center left pixel (y truncated)"""
        return int((self.rsizey - 1) / 2)

    @property
    def rb(self):
        """Indices: raster bottom center pixel (x truncated, y)"""
        return np.array([self.rbx, self.rby], dtype=env.default_index_dtype)

    @property
    def rbx(self):
        """Index: raster bottom center pixel (x truncated)"""
        return int((self.rsizex - 1) / 2)

    @property
    def rby(self):
        """Index: raster bottom center pixel (y)"""
        return self.rsizey - 1

    @property
    def rr(self):
        """Indices: raster center right pixel (x, y truncated)"""
        return np.array([self.rrx, self.rry], dtype=env.default_index_dtype)

    @property
    def rrx(self):
        """Index: raster center right pixel (x)"""
        return self.rsizex - 1

    @property
    def rry(self):
        """Index: raster center right pixel (y truncated)"""
        return int((self.rsizey - 1) / 2)

    @property
    def rc(self):
        """Indices: raster center pixel (x truncated, y truncated)"""
        return np.array([self.rcx, self.rcy], dtype=env.default_index_dtype)

    @property
    def rcx(self):
        """Index: raster center pixel (x truncated)"""
        return int((self.rsizex - 1) / 2)

    @property
    def rcy(self):
        """Index: raster center pixel (y truncated)"""
        return int((self.rsizey - 1) / 2)

    # Accessors - Raster - Misc ***************************************************************** **
    @property
    def rsemiminoraxis(self):
        """Pixel quantity: half pixel count (truncated) of the smaller side"""
        return int(np.min(self.rsize) / 2.)

    @property
    def rsemimajoraxis(self):
        """Pixel quantity: half pixel count (truncated) of the bigger side"""
        return int(np.max(self.rsize) / 2.)

    @property
    def rarea(self):
        """Pixel quantity: pixel count"""
        return int(np.prod(self.rsize))

    @property
    def rlength(self):
        """Pixel quantity: pixel count in the outer ring"""
        inner_area = max(0, self.rsizex - 2) * max(0, self.rsizey - 2)
        return self.rarea - inner_area

    # Accessors - Affine transformations ******************************************************** **
    @property
    def gt(self):
        """First 6 numbers of the affine transformation matrix, GDAL ordering"""
        return np.array(self._aff.to_gdal(), dtype=np.float64)

    @property
    def aff33(self):
        """The affine transformation matrix"""
        return np.asarray(self._aff, dtype=np.float64).reshape(3, 3)

    @property
    def aff23(self):
        """Top two rows of the affine transformation matrix"""
        return self.aff33[:2]

    @property
    def aff6(self):
        """First 6 numbers of the affine transformation matrix, left-right/top-bottom ordering"""
        return self.aff23.reshape(6)

    @property
    def affine(self):
        """Underlying affine object"""
        return self._aff

    @property
    def scale(self):
        """Spatial vector: scale used in the affine transformation, np.abs(scale) == pxsize"""
        aff = ~affine.Affine.rotation(self.angle)
        tl = np.asarray(aff * self.tl)
        br = np.asarray(aff * self.br)
        return np.asarray((br - tl) / self.rsize, dtype=np.float64)

    @property
    def angle(self):
        """Angle in degree: rotation used in the affine transformation, (0 is north-up)"""
        lrvec = self.lrvec
        return float(np.arctan2(lrvec[1], lrvec[0]) * 180. / np.pi)

    @property
    def pxsize(self):
        """Spatial distance: ||pixel bottom right - pixel top left|| (x, y)"""
        return self.size / self.rsize

    @property
    def pxsizex(self):
        """Spatial distance: ||pixel right - pixel left|| (x)"""
        return self.sizex / self.rsizex

    @property
    def pxsizey(self):
        """Spatial distance: ||pixel bottom - pixel top|| (y)"""
        return self.sizey / self.rsizey

    @property
    def pxvec(self):
        """Spatial vector: (pixel bottom right - pixel top left)"""
        return self.diagvec / self.rsize

    @property
    def pxtbvec(self):
        """Spatial vector: (pixel bottom left - pixel top left)"""
        return self.tbvec / self.rheight

    @property
    def pxlrvec(self):
        """Spatial vector: (pixel top right - pixel top left)"""
        return self.lrvec / self.rwidth

    # Binary predicates ************************************************************************* **
    # Binary predicates - geometry ************************************************************** **
    def __eq__(self, other):
        """Returns self.equals"""
        return self.equals(other)

    def __ne__(self, other):
        """Returns not self.equals"""
        return not self.equals(other)

    def share_area(self, other):
        """Binary predicate: Does other share area with self

        Parameters
        ----------
        other: Footprint or shapely object

        Returns
        -------
        bool
        """
        a = self.poly
        b = other.poly
        return not a.disjoint(b) and not a.touches(b)

    def equals(self, other):
        """Binary predicate: Is other Footprint equal to self

        Parameters
        ----------
        other: Footprint

        Returns
        -------
        bool
        """
        if env.significant <= self._significant_min:
            raise RuntimeError('`env.significant` of value {} should be at least {}'.format(
                env.significant, self._significant_min,
            ))
        if env.significant <= other._significant_min:
            raise RuntimeError('`env.significant` of value {} should be at least {}'.format(
                env.significant, other._significant_min,
            ))
        if (self.rsize != other.rsize).any():
            return False

        largest_coord = np.abs(np.r_[self.coords, other.coords]).max()
        spatial_precision = largest_coord * 10 ** -env.significant
        return (np.abs(self.coords - other.coords) < spatial_precision).all()

    def same_grid(self, other):
        """Binary predicate: Does other Footprint lie on the same grid as self

        Parameters
        ----------
        other: Footprint

        Returns
        -------
        bool
        """
        if env.significant <= self._significant_min:
            raise RuntimeError('`env.significant` of value {} should be at least {}'.format(
                env.significant, self._significant_min,
            ))
        if env.significant <= other._significant_min:
            raise RuntimeError('`env.significant` of value {} should be at least {}'.format(
                env.significant, other._significant_min,
            ))
        largest_coord = np.abs(np.r_[self.coords, other.coords]).max()
        spatial_precision = largest_coord * 10 ** -env.significant

        rdx, rdy = np.around(~self.affine * other.tl)
        errors = other.tl - (self.pxtbvec * rdy + self.pxlrvec * rdx) - self.tl
        if (np.abs(errors) >= spatial_precision).any():
            return False

        errors = self.tl + other.pxtbvec * self.rheight - self.bl
        if (np.abs(errors) >= spatial_precision).any():
            return False
        errors = self.tl + other.pxlrvec * self.rwidth - self.tr
        if (np.abs(errors) >= spatial_precision).any():
            return False
        errors = other.tl + self.pxtbvec * other.rheight - other.bl
        if (np.abs(errors) >= spatial_precision).any():
            return False
        errors = other.tl + self.pxlrvec * other.rwidth - other.tr
        if (np.abs(errors) >= spatial_precision).any():
            return False
        return True

    # Numpy ************************************************************************************* **
    @property
    def shape(self):
        """Pixel quantities: (pixel per column, pixel per line)"""
        return np.flipud(self._rsize)

    @property
    def meshgrid_raster(self):
        """Compute indice matrices

        Returns
        -------
        (x, y): (np.ndarray, np.ndarray)
            Raster indices matrices
            with shape = self.shape
            with dtype = env.default_index_dtype
        """
        return np.meshgrid(
            np.asarray(range(self.rsizex), dtype=env.default_index_dtype),
            np.asarray(range(self.rsizey), dtype=env.default_index_dtype),
            copy=False,
        )

    @property
    def meshgrid_spatial(self):
        """Compute coordinate matrices

        Returns
        -------
        (x, y): (np.ndarray, np.ndarray)
            Spatial coordinate matrices
            with shape = self.shape
            with dtype = float32
        """
        x, y = self.meshgrid_raster
        return (
            (x * self._aff.a + y * self._aff.b + self._aff.c).astype(np.float64),
            (x * self._aff.d + y * self._aff.e + self._aff.f).astype(np.float64),
        )

    def meshgrid_raster_in(self, other, dtype=None, op=np.floor):
        """Compute raster coordinate matrices of `self` in `other` referential

        Parameters
        ----------
        other: Footprint
        dtype: None or convertible to np.dtype
            Output dtype
            If None: Use buzz.env.default_index_dtype
        op: None or function operating on a vector
            Function to apply before casting output to dtype
            If None: Do not transform data before casting

        Returns
        -------
        (x, y): (np.ndarray, np.ndarray)
            Raster coordinate matrices
            with shape = self.shape
            with dtype = dtype
        """
        # Check other parameter
        if not isinstance(other, self.__class__):
            raise TypeError('other should be a Footprint') # pragma: no cover

        # Check dtype parameter
        if dtype is None:
            dtype = env.default_index_dtype
        else:
            dtype = conv.dtype_of_any_downcast(dtype)

        # Check op parameter
        if not isinstance(np.zeros(1, dtype=dtype)[0], numbers.Integral):
            op = None

        xy = other.spatial_to_raster(np.dstack(self.meshgrid_spatial), dtype=dtype, op=op)
        return xy[..., 0], xy[..., 1]

    def slice_in(self, other, clip=False):
        """Compute location of `self` inside `other` with slice objects.
        If other and self do not have the same rotation, operation is undefined

        Parameters
        ----------
        other: Footprint
        clip: bool
            False
                Does nothing
            True
                Clip the slices to other bounds. If other and self do not share area,
                at least one of the returned slice will have `slice.start == slice.stop`

        Returns
        -------
        (yslice, xslice): (slice, slice)

        Example
        -------
        Burn `small` into `big` if `small` is within `big`
        >>> big_data[small.slice_in(big)] = small_data

        Burn `small` into `big` where overlapping
        >>> big_data[small.slice_in(big, clip=True)] = small_data[big.slice_in(small, clip=True)]
        """
        if not isinstance(other, self.__class__):
            raise TypeError('other should be a Footprint') # pragma: no cover
        startx, starty = other.spatial_to_raster(self.tl)
        endx, endy = other.spatial_to_raster(self.br)
        if clip:
            startx = startx.clip(0, other.rsizex)
            endx = endx.clip(0, other.rsizex)
            starty = starty.clip(0, other.rsizey)
            endy = endy.clip(0, other.rsizey)
        return slice(starty, endy), slice(startx, endx)

    # Coordinates conversions ******************************************************************* **
    def spatial_to_raster(self, xy, dtype=None, op=np.floor):
        """Convert xy spatial coordinates to raster xy indices

        Parameters
        ----------
        xy: sequence of numbers of shape (..., 2)
            Spatial coordinates
        dtype: None or convertible to np.dtype
            Output dtype
            If None: Use buzz.env.default_index_dtype
        op: None or vectorized function
            Function to apply before casting output to dtype
            If None: Do not transform data before casting

        Returns
        -------
        out_xy: np.ndarray
            Raster indices
            with shape = np.asarray(xy).shape
            with dtype = dtype

        Prototype inspired from
        https://mapbox.github.io/rasterio/api/rasterio.io.html#rasterio.io.TransformMethodsMixin.index
        """
        # Check xy parameter
        xy = np.asarray(xy)
        if xy.shape[-1] != 2:
            return ValueError('An array of shape (..., 2) was expected') # pragma: no cover

        # Check dtype parameter
        if dtype is None:
            dtype = env.default_index_dtype
        else:
            dtype = conv.dtype_of_any_downcast(dtype)

        # Check op parameter
        if not isinstance(np.zeros(1, dtype=dtype)[0], numbers.Integral):
            op = None


        if env.significant <= self._significant_min:
            raise RuntimeError('`env.significant` of value {} should be at least {}'.format(
                env.significant, self._significant_min,
            ))
        largest_coord = np.abs(self.coords).max()
        spatial_precision = largest_coord * 10 ** -env.significant
        smallest_reso = self.pxsize.min()
        pixel_precision = spatial_precision / smallest_reso
        abstract_grid_density = np.floor(1 / pixel_precision)

        workshape = int(xy.size / 2), 2
        xy2 = np.empty(workshape, 'float64')
        xy2[:, :] = xy.reshape(workshape)
        aff = ~self._aff
        xy2[:, 0], xy2[:, 1] = (
            xy2[:, 0] * aff.a + xy2[:, 1] * aff.b + aff.c,
            xy2[:, 0] * aff.d + xy2[:, 1] * aff.e + aff.f,
        )
        xy2 = np.around(xy2 * abstract_grid_density, 0) / abstract_grid_density # Should move this line in if?
        if op is not None:
            xy2 = op(xy2)
        return xy2.astype(dtype).reshape(xy.shape)

    def raster_to_spatial(self, xy):
        """Convert xy raster coordinates to spatial coordinates

        Parameters
        ----------
        xy: sequence of numbers of shape (..., 2)
           Raster coordinages

        Returns
        -------
        out_xy: np.ndarray
            Spatial coordinates
            with shape = np.asarray(xy).shape
            with dtype = dtype

        """
        # Check xy parameter
        xy = np.asarray(xy)
        if xy.shape[-1] != 2:
            raise ValueError('An array of shape (..., 2) was expected') # pragma: no cover

        workshape = int(xy.size / 2), 2
        xy2 = np.empty(workshape, 'float64')
        xy2[:, :] = xy.reshape(workshape)
        aff = self._aff
        xy2[:, 0], xy2[:, 1] = (
            xy2[:, 0] * aff.a + xy2[:, 1] * aff.b + aff.c,
            xy2[:, 0] * aff.d + xy2[:, 1] * aff.e + aff.f,
        )
        return xy2.reshape(xy.shape)

    # Geometry / Raster conversions ************************************************************* **
    def find_lines(self, arr, output_offset='middle'):
        """Experimental function!

        Create a list of line-strings from a mask. Works with connectivity 4 and 8. Should work fine
        when several disconnected components

        See `shapely.ops.linemerge` for details concerning output connectivity

        Parameters
        ----------
        arr: np.ndarray of bool of shape (self.shape)
        output_offset: 'middle' or (nbr, nbr)
            Coordinate offset in meter
            if `middle`: substituted by `self.pxvec / 2`

        Returns
        -------
        list of shapely.geometry.LineString

        Caveats
        -------
        All standalone pixels contained in arr will be ignored.

        Exemple
        -------
        >>> import buzzard as buzz
        >>> import numpy as np
        >>> import networkx as nx

        >>> with buzz.Env(warnings=0, allow_complex_footprint=1):
        ...     a = np.asarray([
        ...         [0, 1, 1, 1, 0],
        ...         [0, 1, 0, 0, 0],
        ...         [0, 1, 1, 1, 0],
        ...         [0, 1, 0, 0, 0],
        ...         [0, 1, 1, 0, 0],
        ...
        ...     ])
        ...     fp = buzz.Footprint(gt=(0, 1, 0, 0, 0, 1), rsize=(a.shape))
        ...     lines = fp.find_lines(a, (0, 0))
        ...
        ...     # Display input / output
        ...     print(fp)
        ...     print(a.astype(int), '\n')
        ...     for i, l in enumerate(lines, 1):
        ...         print(f'edge-id:{i} of type:{type(l)} and length:{l.length}')
        ...         print(fp.burn_lines(l).astype(int) * i, '\n')
        ...
        ...     # Build a networkx graph
        ...     g = nx.Graph([(l.coords[0], l.coords[-1]) for l in lines])
        ...     print(repr(g.degree))
        ...
        Footprint(tl=(0.000000, 0.000000), scale=(1.000000, 1.000000), angle=0.000000, rsize=(5, 5))
        [[0 1 1 1 0]
         [0 1 0 0 0]
         [0 1 1 1 0]
         [0 1 0 0 0]
         [0 1 1 0 0]]

        edge-id:1 of type:<class 'shapely.geometry.linestring.LineString'> and length:2.0
        [[0 0 0 0 0]
         [0 0 0 0 0]
         [0 1 1 1 0]
         [0 0 0 0 0]
         [0 0 0 0 0]]

        edge-id:2 of type:<class 'shapely.geometry.linestring.LineString'> and length:3.0
        [[0 0 0 0 0]
         [0 0 0 0 0]
         [0 2 0 0 0]
         [0 2 0 0 0]
         [0 2 2 0 0]]

        edge-id:3 of type:<class 'shapely.geometry.linestring.LineString'> and length:4.0
        [[0 3 3 3 0]
         [0 3 0 0 0]
         [0 3 0 0 0]
         [0 0 0 0 0]
         [0 0 0 0 0]]

        DegreeView({(3.0, 2.0): 1, (1.0, 2.0): 3, (2.0, 4.0): 1, (3.0, 0.0): 1})
        """
        if arr.shape != tuple(self.shape):
            raise ValueError('Incompatible shape between array:%s and self:%s' % (
                arr.shape, self.shape
            )) # pragma: no cover
        arr = arr.astype(bool)
        arr = arr.astype('uint8')

        if output_offset == 'middle':
            output_offset = self.pxvec / 2
        else:
            output_offset = np.asarray(output_offset, 'float64')
            if output_offset.shape != (2,):
                raise ValueError(
                    '`output_offset` should be "middle" or a sequence of float of length 2'
                ) # pragma: no cover

        count = np.sum(arr)
        yx_lst = np.stack(arr.nonzero(), -1)
        index_lst = np.arange(count)
        index = np.empty(self.shape, dtype=int)
        index[arr != 0] = index_lst

        convolve = lambda arr, kernel: scipy.ndimage.convolve(arr, kernel, mode='constant', cval=0)
        has_top = convolve(arr, [[0, 0, 0], [0, 0, 0], [0, 1, 0]]) * arr
        has_right = convolve(arr, [[0, 0, 0], [1, 0, 0], [0, 0, 0]]) * arr
        has_left = convolve(arr, [[0, 0, 0], [0, 0, 1], [0, 0, 0]]) * arr
        has_topright = convolve(arr, [[0, 0, 0], [0, 0, 0], [1, 0, 0]]) * arr
        has_topleft = convolve(arr, [[0, 0, 0], [0, 0, 0], [0, 0, 1]]) * arr
        has_topright = (has_topright.astype('i1') - has_right - has_top).clip(0, 1).astype('u1')
        has_topleft = (has_topleft.astype('i1') - has_left - has_top).clip(0, 1).astype('u1')

        def _build_neighbors_in_direction(mask, yx_vector):
            has_indices = mask[yx_lst[:, 0], yx_lst[:, 1]].nonzero()[0]
            neig_yx = yx_lst[has_indices] + yx_vector
            neig = index[neig_yx[:, 0], neig_yx[:, 1]]
            return np.c_[neig, has_indices]

        edges_indices = np.vstack([
            _build_neighbors_in_direction(has_top, (-1, 0)),
            _build_neighbors_in_direction(has_right, (0, 1)),
            _build_neighbors_in_direction(has_topright, (-1, 1)),
            _build_neighbors_in_direction(has_topleft, (-1, -1)),
        ])
        if not edges_indices:
            return []

        lines = [
            shapely.geometry.LineString([
                self.raster_to_spatial(np.flipud(yx_lst[n1])) + output_offset,
                self.raster_to_spatial(np.flipud(yx_lst[n2])) + output_offset,
            ])
            for (n1, n2) in edges_indices
        ]
        mline = shapely.ops.linemerge(lines)
        if isinstance(mline, shapely.geometry.LineString):
            mline = sg.MultiLineString([mline])

        # Temporary check for badness, until further testing of this method
        check = arr.copy()
        check[:] = 0

        for l in mline:
            coords = np.asarray(l) - output_offset
            coords = self.spatial_to_raster(coords)
            x = coords[:, 0]
            y = coords[:, 1]
            check[y, x] = 1

        # Burning size one components since they are not transformed to lines
        for sly, slx in ndi.find_objects(ndi.label(arr)[0]):
            if sly.stop - sly.start == 1 and slx.stop - slx.start == 1:
                check[sly, slx] = 1

        assert (check == arr).all()

        if mline.is_empty:
            return []
        if isinstance(mline, shapely.geometry.LineString):
            return [mline]
        if isinstance(mline, shapely.geometry.MultiLineString):
            return list(mline.geoms)
        assert False # pragma: no cover

    def burn_lines(self, obj, labelize=False):
        """Experimental function!

        Create a 2d image from lines

        Parameters
        ----------
        obj: shapely line or nested iterators over shapely lines
        labelize: bool
            if `False`: Create a boolean mask
            if `True`: Create an integer matrix containing lines indices from order in input

        Returns
        ----------
        np.ndarray
            of bool or uint8 or int
            of shape (self.shape)
        """
        lines = list(_line_iterator(obj))

        # https://svn.osgeo.org/gdal/trunk/autotest/alg/rasterize.py

        sr_wkt = 'LOCAL_CS["arbitrary"]'
        sr = osr.SpatialReference(sr_wkt)

        if labelize:
            if len(lines) > 255:
                dtype = conv.dtype_of_any_downcast('int')
            else:
                dtype = conv.dtype_of_any_downcast('uint8')
        else:
            dtype = conv.dtype_of_any_downcast('bool')
        gdt = conv.gdt_of_any_equiv(dtype) # Set to downcast

        target_ds = gdal.GetDriverByName('MEM').Create(
            '', int(self.rsizex), int(self.rsizey), 1, gdt
        )
        target_ds.SetGeoTransform(self.gt)
        target_ds.SetProjection(sr_wkt)

        rast_ogr_ds = ogr.GetDriverByName('Memory').CreateDataSource('wrk')
        rast_mem_lyr = rast_ogr_ds.CreateLayer('line', srs=sr)
        val_field = ogr.FieldDefn('val', ogr.OFTInteger64)
        rast_mem_lyr.CreateField(val_field)

        for i, line in enumerate(lines, 1):
            feat = ogr.Feature(rast_mem_lyr.GetLayerDefn())
            wkt_geom = line.wkt
            feat.SetGeometryDirectly(ogr.Geometry(wkt=wkt_geom))
            feat.SetFieldInteger64(0, i)
            rast_mem_lyr.CreateFeature(feat)
        err = gdal.RasterizeLayer(target_ds, [1], rast_mem_lyr, options=["ATTRIBUTE=val"])
        if err != 0:
            raise Exception(
                'Got non-zero result code from gdal.RasterizeLayer (%s)' % gdal.GetLastErrorMsg()
            )
        arr = target_ds.GetRasterBand(1).ReadAsArray()
        return arr.astype(dtype)

    def find_polygons(self, mask):
        """Experimental function!
        Create a list of polygons from a mask.

        Parameters
        ----------
        arr: np.ndarray of bool of shape (self.shape)

        Returns
        -------
        list of shapely.geometry.Polygon

        Caveats
        -------
        Some inputs that may produce invalid polygons (see below) are fixed with the
        `shapely.geometry.Polygon.buffer` method.
        0 0 0 0 0 0 0
        0 1 1 1 0 0 0
        0 1 1 1 1 0 0
        0 1 1 1 0 1 0  <- Hole near edge, should create a self touching polygon without holes.
        0 1 1 1 1 1 1     A valid polygon with one hole is returned instead.
        0 1 1 1 1 1 1
        0 0 0 0 0 0 0
        """
        if mask.shape != tuple(self.shape):
            raise ValueError('Mask shape%s incompatible with self shape%s' % (
                mask.shape, tuple(self.shape)
            )) # pragma: no cover
        mask = mask.astype('uint8').clip(0, 1)
        sr_wkt = 'LOCAL_CS["arbitrary"]'
        sr = osr.SpatialReference(sr_wkt)

        source_ds = gdal.GetDriverByName('MEM').Create(
            '', int(self.rsizex), int(self.rsizey), 1, gdal.GDT_Byte
        )
        source_ds.SetGeoTransform(self.gt)
        source_ds.SetProjection(sr_wkt)
        source_ds.GetRasterBand(1).WriteArray(mask, 0, 0)

        ogr_ds = ogr.GetDriverByName('Memory').CreateDataSource('wrk')
        ogr_lyr = ogr_ds.CreateLayer('poly', srs=sr)
        field_defn = ogr.FieldDefn('elev', ogr.OFTReal)
        ogr_lyr.CreateField(field_defn)

        gdal.Polygonize(
            srcBand=source_ds.GetRasterBand(1),
            maskBand=source_ds.GetRasterBand(1),
            outLayer=ogr_lyr,
            iPixValField=0,
        )
        del source_ds

        def _polygon_iterator():
            feat = ogr_lyr.GetNextFeature()
            while feat is not None:
                geometry = feat.geometry()
                geometry = conv.shapely_of_ogr(geometry)
                if not geometry.is_valid:
                    geometry = geometry.buffer(0)
                yield geometry
                feat = ogr_lyr.GetNextFeature()

        return list(_polygon_iterator())

    def burn_polygons(self, obj, all_touched=False):
        """Experimental function!
        Create a 2d image from polygons

        Parameters
        ----------
        obj: shapely polygon or nested iterators over shapely polygons
        all_touched: bool
            Burn all polygons touched

        Returns
        ----------
        np.ndarray
            of bool or uint8 or int
            of shape (self.shape)

        Examples
        --------
        >>> burn_polygons(poly)
        >>> burn_polygons([poly, poly])
        >>> burn_polygons([poly, poly, [poly, poly], multipoly, poly])
        """

        polys = list(_poly_iterator(obj))

        # https://svn.osgeo.org/gdal/trunk/autotest/alg/rasterize.py

        sr_wkt = 'LOCAL_CS["arbitrary"]'
        sr = osr.SpatialReference(sr_wkt)

        target_ds = gdal.GetDriverByName('MEM').Create(
            '', int(self.rsizex), int(self.rsizey), 1, gdal.GDT_Byte
        )
        target_ds.SetGeoTransform(self.gt)
        target_ds.SetProjection(sr_wkt)

        rast_ogr_ds = ogr.GetDriverByName('Memory').CreateDataSource('wrk')
        rast_mem_lyr = rast_ogr_ds.CreateLayer('poly', srs=sr)

        for poly in polys:
            feat = ogr.Feature(rast_mem_lyr.GetLayerDefn())
            wkt_geom = poly.wkt
            feat.SetGeometryDirectly(ogr.Geometry(wkt=wkt_geom))
            rast_mem_lyr.CreateFeature(feat)
        if all_touched:
            options = ["ALL_TOUCHED=TRUE"]
        else:
            options = []
        err = gdal.RasterizeLayer(target_ds, [1], rast_mem_lyr, burn_values=[1], options=options)
        if err != 0:
            raise Exception('Got non-zero result code from gdal.RasterizeLayer')
        arr = target_ds.GetRasterBand(1).ReadAsArray()
        return arr.astype(bool)

    # Tiling ************************************************************************************ **
    def tile(self, size, overlapx=0, overlapy=0,
             boundary_effect='extend', boundary_effect_locus='br'):
        """Tile a Footprint to a matrix of Footprint

        Parameters
        ----------
        size : (int, int)
            Tile width and tile height, in pixel
        overlapx : int
            Width of a tile overlapping with each direct horizontal neighbors, in pixel
        overlapy : int
            Height of a tile overlapping with each direct vertical neighbors, in pixel
        boundary_effect : {'extend', 'exclude', 'overlap', 'shrink', 'exception'}
            Behevior at boundary effect locus
            'extend'
                Preserve tile size
                Preserve overlapx and overlapy
                Sacrifice global bounds, results in tiles partially outside bounds at locus (if necessary)
                Preserve tile count
                Preserve boundary pixels coverage
            'overlap'
                Preserve tile size
                Sacrifice overlapx and overlapy, results in tiles overlapping more at locus (if necessary)
                Preserve global bounds
                Preserve tile count
                Preserve boundary pixels coverage
            'exclude'
                Preserve tile size
                Preserve overlapx and overlapy
                Preserve global bounds
                Sacrifice tile count, results in tiles excluded at locus (if necessary)
                Sacrifice boundary pixels coverage at locus (if necessary)
            'shrink'
                Sacrifice tile size, results in tiles shrinked at locus (if necessary)
                Preserve overlapx and overlapy
                Preserve global bounds
                Preserve tile count
                Preserve boundary pixels coverage
            'exception'
                Raise an exception if tiles at locus do not lie inside the global bounds
        boundary_effect_locus : {'br', 'tr', 'tl', 'bl'}, optional
            Locus of the boundary effects
            'br' : Boundary effect occurs at the bottom right corner of the raster,
                top left coordinates are preserved
            'tr' : Boundary effect occurs at the top right corner of the raster,
                bottom left coordinates are preserved
            'tl' : Boundary effect occurs at the top left corner of the raster,
                bottom right coordinates are preserved
            'bl' : Boundary effect occurs at the bottom left corner of the raster,
                top right coordinates are preserved

        Returns
        -------
        np.ndarray
            of dtype=object (Footprint)
            of shape (M, N)
                with M the line count
                with N the column count
        """
        size = np.asarray(size, dtype=int)
        overlapx = int(overlapx)
        overlapy = int(overlapy)

        if size.shape != (2,):
            raise ValueError('size.shape(%s) should be (2,)' % str(size.shape))
        if (size <= 0).any():
            raise ValueError('size(%s) values should satisfy value > 0' % str(tuple(size)))
        if not 0 <= overlapx < size[0]:
            raise ValueError('overlapx(%d) should satisfy 0 <= overlapx < size[0](%d)' % (
                overlapx, size[0]
            ))
        if not 0 <= overlapy < size[1]:
            raise ValueError('overlapy(%d) should satisfy 0 <= overlapy < size[1](%d)' % (
                overlapy, size[1]
            ))
        if boundary_effect not in self._TILE_BOUNDARY_EFFECTS:
            raise ValueError('boundary_effect(%s) should be one of %s' % (
                boundary_effect, self._TILE_BOUNDARY_EFFECTS
            ))
        if boundary_effect_locus not in self._TILE_BOUNDARY_EFFECT_LOCI:
            raise ValueError('boundary_effect_locus(%s) should be one of %s' % (
                boundary_effect_locus, self._TILE_BOUNDARY_EFFECT_LOCI
            ))
        return self._tile_unsafe(size, overlapx, overlapy, boundary_effect, boundary_effect_locus)

    def tile_count(self, rowcount, colcount, overlapx=0, overlapy=0,
                   boundary_effect='extend', boundary_effect_locus='br'):
        """Tile a Footprint to a matrix of Footprint

        Parameters
        ----------
        rowcount : int
            Tile count per row
        colcount : int
            Tile count per column
        overlapx : int
            Width of a tile overlapping with each direct horizontal neighbors, in pixel
        overlapy : int
            Height of a tile overlapping with each direct vertical neighbors, in pixel
        boundary_effect : {'extend', 'exclude', 'overlap', 'shrink', 'exception'}, optional
            Behevior at boundary effect locus
            'extend'
                Preserve tile size
                Preserve overlapx and overlapy
                Sacrifice global bounds, results in tiles partially outside bounds at locus (if necessary)
                Preserve tile count
                Preserve boundary pixels coverage
            'overlap'
                Preserve tile size
                Sacrifice overlapx and overlapy, results in tiles overlapping more at locus (if necessary)
                Preserve global bounds
                Preserve tile count
                Preserve boundary pixels coverage
            'exclude'
                Preserve tile size
                Preserve overlapx and overlapy
                Preserve global bounds
                Preserve tile count
                Sacrifice boundary pixels coverage at locus (if necessary)
            'shrink'
                Sacrifice tile size, results in tiles shrinked at locus (if necessary)
                Preserve overlapx and overlapy
                Preserve global bounds
                Preserve tile count
                Preserve boundary pixels coverage
            'exception'
                Raise an exception if tiles at locus do not lie inside the global bounds
        boundary_effect_locus : {'br', 'tr', 'tl', 'bl'}, optional
            Locus of the boundary effects
            'br' : Boundary effect occurs at the bottom right corner of the raster,
                top left coordinates are preserved
            'tr' : Boundary effect occurs at the top right corner of the raster,
                bottom left coordinates are preserved
            'tl' : Boundary effect occurs at the top left corner of the raster,
                bottom right coordinates are preserved
            'bl' : Boundary effect occurs at the bottom left corner of the raster,
                top right coordinates are preserved

        Returns
        -------
        np.ndarray
            of dtype=object (Footprint)
            of shape (M, N)
                with M the line count
                with N the column count
        """
        rowcount = int(rowcount)
        colcount = int(colcount)
        overlapx = int(overlapx)
        overlapy = int(overlapy)

        if rowcount <= 0:
            raise ValueError('rowcount(%s) should satisfy rowcount > 0' % rowcount)
        if colcount <= 0:
            raise ValueError('colcount(%s) should satisfy colcount > 0' % colcount)
        if overlapx < 0:
            raise ValueError('overlapx(%s) should satisfy overlapx >= 0' % overlapx)
        if overlapy < 0:
            raise ValueError('overlapy(%s) should satisfy overlapy >= 0' % overlapy)
        if boundary_effect not in self._TILE_BOUNDARY_EFFECTS:
            raise ValueError('boundary_effect(%s) should be one of %s' % (
                boundary_effect, self._TILE_BOUNDARY_EFFECTS
            ))
        if boundary_effect_locus not in self._TILE_BOUNDARY_EFFECT_LOCI:
            raise ValueError('boundary_effect_locus(%s) should be one of %s' % (
                boundary_effect_locus, self._TILE_BOUNDARY_EFFECT_LOCI
            ))

        sizex_float = (self.rsizex + overlapx * (rowcount - 1)) / rowcount
        sizey_float = (self.rsizey + overlapy * (colcount - 1)) / colcount
        if boundary_effect in ['extend', 'overlap', 'shrink']:
            sizex = int(np.ceil(sizex_float))
            sizey = int(np.ceil(sizey_float))
        elif boundary_effect == 'exclude':
            sizex = int(np.floor(sizex_float))
            sizey = int(np.floor(sizey_float))
        elif boundary_effect == 'exception':
            sizex = int(np.floor(sizex_float))
            if sizex != sizex_float:
                gap = int((sizex_float - sizex) * rowcount)
                raise ValueError(
                    ('There is a gap of %d pixel in the x direction, ' +
                     '`gap:%d %% (sizex:%d - overlapx:%d) == 0` was required') % (
                         (gap, gap, sizex, overlapx)))
            sizey = int(np.floor(sizey_float))
            if sizey != sizey_float:
                gap = int((sizey_float - sizey) * colcount)
                raise ValueError(
                    ('There is a gap of %d pixel in the y direction, ' +
                     '`gap:%d %% (sizey:%d - overlapy:%d) == 0` was required') % (
                         (gap, gap, sizey, overlapy)))
        else:
            assert False # pragma: no cover
        if sizex <= overlapx:
            raise ValueError('rowcount(%d) with overlapx(%d) would not fit in %d pixels' % (
                rowcount, overlapx, self.rw,
            ))
        if sizey <= overlapy:
            raise ValueError('colcount(%d) with overlapy(%d) would not fit in %d pixels' % (
                colcount, overlapy, self.rw,
            ))

        outsidex = sizex + (rowcount - 1) * (sizex - overlapx) - self.rsizex
        if outsidex >= (sizex - overlapx):
            raise ValueError('rowcount(%d) with overlapx(%d) would not fit in %d pixels' % (
                rowcount, overlapx, self.rw,
            ))
        outsidey = sizey + (colcount - 1) * (sizey - overlapy) - self.rsizey
        if outsidey >= (sizey - overlapy):
            raise ValueError('colcount(%d) with overlapy(%d) would not fit in %d pixels' % (
                colcount, overlapy, self.rw,
            ))

        size = np.asarray((sizex, sizey), dtype=int)
        tiles = self._tile_unsafe(size, overlapx, overlapy, boundary_effect, boundary_effect_locus)
        if boundary_effect == 'exclude':
            if boundary_effect_locus == 'br':
                tiles = tiles[0:colcount, 0:rowcount]
            elif boundary_effect_locus == 'tl':
                tiles = tiles[-colcount:, -rowcount:]
            elif boundary_effect_locus == 'tr':
                tiles = tiles[-colcount:, 0:rowcount]
            elif boundary_effect_locus == 'bl':
                tiles = tiles[0:colcount, -rowcount:]
            else:
                assert False # pragma: no cover
        return tiles

    def tile_occurrence(self, size, pixel_occurrencex, pixel_occurrencey,
                        boundary_effect='extend', boundary_effect_locus='br'):
        """Tile a Footprint to a matrix of Footprint
        Each pixel occur `pixel_occurrencex * pixel_occurrencey` times overall in the output

        Parameters
        ----------
        size : (int, int)
            Tile width and tile height, in pixel
        pixel_occurrencex: int
            Number of occurence of each pixel in a line of tile
        pixel_occurrencey: int
            Number of occurence of each pixel in a column of tile
        boundary_effect : {'extend', 'exclude', 'overlap', 'shrink', 'exception'}, optional
            Behevior at boundary effect locus
            'extend'
                Preserve tile size
                Preserve overlapx and overlapy
                Sacrifice global bounds
                    Results in tiles partially outside bounds at locus (if necessary)
                Preserve tile count
                Preserve boundary pixels coverage
            'overlap'
                Preserve tile size
                Sacrifice overlapx and overlapy
                    Results in tiles overlapping more at locus (if necessary)
                Preserve global bounds
                Preserve tile count
                Preserve boundary pixels coverage
            'exclude'
                Preserve tile size
                Preserve overlapx and overlapy
                Preserve global bounds
                Sacrifice tile count, results in tiles excluded at locus (if necessary)
                Sacrifice boundary pixels coverage at locus (if necessary)
            'shrink'
                Sacrifice tile size, results in tiles shrinked at locus (if necessary)
                Preserve overlapx and overlapy
                Preserve global bounds
                Preserve tile count
                Preserve boundary pixels coverage
            'exception'
                Raise an exception if tiles at locus do not lie inside the global bounds
        boundary_effect_locus : {'br', 'tr', 'tl', 'bl'}, optional
            Locus of the boundary effects
            'br' : Boundary effect occurs at the bottom right corner of the raster
                top left coordinates are preserved
            'tr' : Boundary effect occurs at the top right corner of the raster,
                bottom left coordinates are preserved
            'tl' : Boundary effect occurs at the top left corner of the raster,
                bottom right coordinates are preserved
            'bl' : Boundary effect occurs at the bottom left corner of the raster,
                top right coordinates are preserved

        Returns
        -------
        np.ndarray
            of dtype=object (Footprint)
            of shape (M, N)
                with M the line count
                with N the column count
        """
        size = np.asarray(size, dtype=int)
        pixel_occurrencex = int(pixel_occurrencex)
        pixel_occurrencey = int(pixel_occurrencey)

        if size.shape != (2,):
            raise ValueError('size.shape(%s) should be (2,)' % str(size.shape))
        if (size <= 0).any():
            raise ValueError('size(%s) values should satisfy value > 0' % str(tuple(size)))

        if not pixel_occurrencex > 0:
            raise ValueError(
                'pixel_occurrencex should satisfy pixel_occurrencex(%s) > 0' % pixel_occurrencex
            )
        elif not size[0] % pixel_occurrencex == 0:
            raise ValueError(
                'pixel_occurrencex should satisty size[0](%s) % pixel_occurrencex(%s) == 0'
            )
        if not pixel_occurrencey > 0:
            raise ValueError(
                'pixel_occurrencey should satisfy pixel_occurrencey(%s) > 0' % pixel_occurrencey
            )
        elif not size[1] % pixel_occurrencey == 0:
            raise ValueError(
                'pixel_occurrencey should satisty size[1](%s) % pixel_occurrencey(%s) == 0'
            )

        if boundary_effect not in self._TILE_OCCURRENCE_BOUNDARY_EFFECTS:
            raise ValueError('boundary_effect(%s) should be one of %s' % (
                boundary_effect, self._TILE_OCCURRENCE_BOUNDARY_EFFECTS
            ))
        if boundary_effect_locus not in self._TILE_BOUNDARY_EFFECT_LOCI:
            raise ValueError('boundary_effect_locus(%s) should be one of %s' % (
                boundary_effect_locus, self._TILE_BOUNDARY_EFFECT_LOCI
            ))

        occurrence = np.asarray([pixel_occurrencex, pixel_occurrencey], dtype=int)
        stride = (size / occurrence).astype(int)
        overlap = size - stride
        big_tl = self.tl - self.pxvec * overlap
        big_rsize = self.rsize + np.asarray(overlap) * 2
        big_size = big_rsize * self.pxsize
        big_fp = self.__class__(tl=big_tl, size=big_size, rsize=big_rsize)
        tiles = big_fp._tile_unsafe(
            size, overlap[0], overlap[1], boundary_effect, boundary_effect_locus
        )
        return tiles

    # Serialization ***************************************************************************** **
    def __str__(self):
        if self.angle == 0 and (self.scale * (1, -1) > 0).all():
            tup = tuple(np.r_[
                self.tl, self.br, self.size, self.rsize,
            ])
            return "Footprint(tl=(%f, %f), br=(%f, %f), size=(%f, %f), rsize=(%d, %d))" % tup
        else:
            tup = tuple(np.r_[
                self.tl, self.scale, self.angle, self.rsize,
            ])
            return "Footprint(tl=(%f, %f), scale=(%f, %f), angle=%f, rsize=(%d, %d))" % tup

    def __repr__(self):
        return "Footprint(gt=%s, rsize=(%d, %d))" % (tuple(self.gt), self.rsize[0], self.rsize[1])

    def __reduce__(self):
        return (_restore, (self.gt, self.rsize))

    # The end *********************************************************************************** **
    # ******************************************************************************************* **

def _line_iterator(obj):
    if isinstance(obj, (sg.LineString)):
        yield obj
    elif isinstance(obj, (sg.MultiLineString)):
        for obj2 in obj.geoms:
            yield obj2
    elif isinstance(obj, (sg.Polygon)):
        yield sg.LineString(obj.exterior)
        for obj2 in obj.interiors:
            yield sg.LineString(obj2)
    elif isinstance(obj, (sg.MultiPolygon)):
        for obj2 in obj.geoms:
            yield sg.LineString(obj2.exterior)
            for obj3 in obj2.interiors:
                yield sg.LineString(obj3)
    else:
        try:
            tup = tuple(obj)
        except TypeError:
            raise TypeError('Could not use type %s' % type(obj))
        else:
            for obj2 in tup:
                for line in _line_iterator(obj2):
                    yield line

def _poly_iterator(obj):
    if isinstance(obj, (sg.Polygon)):
        yield obj
    elif isinstance(obj, (sg.MultiPolygon)):
        for obj2 in obj.geoms:
            yield obj2
    else:
        try:
            tup = tuple(obj)
        except TypeError:
            raise TypeError('Could not use type %s' % type(obj))
        else:
            for obj2 in tup:
                for poly in _poly_iterator(obj2):
                    yield poly

def _restore(gt, rsize):
    return Footprint(gt=gt, rsize=rsize)

def _angle_between(a, b, c):
    return np.arccos(np.dot(
        (a - b) / np.linalg.norm(a - b),
        (c - b) / np.linalg.norm(c - b),
    )) / np.pi * 180.
