"""
Multi-threaded, back pressure management, caching
"""

from pathlib import Path
import os
import glob
import uuid
import hashlib
import rtree.index
from osgeo import osr, gdal

import numpy as np
import buzzard as buzz
from buzzard._backend_raster import BackendRaster
from buzzard._tools import conv

# get_uname, qeinfo, qrinfo


def is_tiling_valid(fp, tiles):
    tiles = list(tiles)
    assert isinstance(tiles[0], buzz.Footprint)

    if any(not tile.same_grid(fp) for tile in tiles):
        print("not same grid")
        return False

    idx = rtree.index.Index()
    bound_inset = np.r_[
        1 / 4,
        1 / 4,
        -1 / 4,
        -1 / 4,
    ]

    tls = fp.spatial_to_raster([tile.tl for tile in tiles])
    rsizes = np.array([tile.rsize for tile in tiles])

    if np.any(tls < 0):
        print("tiles out of fp")
        return False

    if np.any(tls[:, 0] + rsizes[:, 0] > fp.rw):
        print("tiles out of fp")
        return False

    if np.any(tls[:, 1] + rsizes[:, 1] > fp.rh):
        print("tiles out of fp")
        return False

    if np.prod(rsizes, axis=1).sum() != fp.rarea:
        print("tile area wrong")
        print(rsizes.shape)
        print(np.prod(rsizes, axis=1).sum())
        print(np.prod(rsizes, axis=1).shape)
        print(fp.rarea)
        return False

    for i, (tl, rsize) in enumerate(zip(tls, rsizes)):
        bounds = (*tl, *(tl + rsize))
        bounds += bound_inset

        if len(list(idx.intersection(bounds))) > 0:
            print("tiles overlap")
            return False

        else:
            idx.insert(i, bounds)

    return True


def md5(fname):
    hash_md5 = hashlib.md5()
    with open(fname, "rb") as f:
        for chunk in iter(lambda: f.read(4096), b""):
            hash_md5.update(chunk)
    return hash_md5.hexdigest()


class BackendCachedRaster(BackendRaster):
    """
    Cached implementation of raster
    """
    def __init__(self,
                 ds,
                 footprint,
                 dtype,
                 nbands,
                 nodata,
                 srs,
                 computation_function,
                 overwrite,
                 cache_dir,
                 cache_tiles,
                 io_pool,
                 computation_pool,
                 primitives,
                 to_collect_of_to_compute,
                 computation_tiles,
                 merge_pool,
                 merge_function,
                 debug_callbacks):

        self._ds = ds
        self._cache_dir = cache_dir
        if cache_tiles is None:
            raise ValueError("cache tiles must be provided")
        if not isinstance(cache_tiles, np.ndarray):
            raise ValueError("cache tiles must be in np array")
        self._cache_tiles = cache_tiles

        self._indices_of_cache_tiles = {
            self._cache_tiles[index]: index
            for index in np.ndindex(*self._cache_tiles.shape)
        }

        assert is_tiling_valid(footprint, cache_tiles.flat)

        if cache_dir is not None:
            os.makedirs(cache_dir, exist_ok=True)
            if overwrite:
                for path in glob.glob(cache_dir + "/*_[a-f0-9]*.tif"):
                    os.remove(path)

        if computation_tiles is None:
            computation_tiles = cache_tiles

        # Array used to track the state of cahce tiles:
        # None: not yet met
        # False: met, has to be written
        # True: met, already written and valid
        self._cache_checksum_array = np.empty(cache_tiles.shape, dtype=object)

        self._cache_idx = rtree.index.Index()
        cache_fps = list(cache_tiles.flat)
        if not isinstance(cache_fps[0], buzz.Footprint):
            raise ValueError("cache tiles must be footprints")
        pxsizex = min(fp.pxsize[0] for fp in cache_fps)
        bound_inset = np.r_[
            pxsizex / 4,
            pxsizex / 4,
            pxsizex / -4,
            pxsizex / -4,
        ]

        for i, fp in enumerate(cache_fps):
            self._cache_idx.insert(i, fp.bounds + bound_inset)

        # self._cache_priority_dict = {fp: index for index, fp in enumerate(sorted(cache_fps, key=lambda fp: (-fp.tly, fp.tlx)))}
        # self._priority_to_cache_fp_dict = {index: fp for index, fp in enumerate(sorted(cache_fps, key=lambda fp: (-fp.tly, fp.tlx)))}

        self._computation_tiles = computation_tiles

        self._computation_idx = rtree.index.Index()
        computation_fps = list(computation_tiles.flat)
        assert isinstance(computation_fps[0], buzz.Footprint)
        pxsizex = min(fp.pxsize[0] for fp in computation_fps)
        bound_inset = np.r_[
            pxsizex / 4,
            pxsizex / 4,
            pxsizex / -4,
            pxsizex / -4,
        ]

        for i, fp in enumerate(computation_fps):
            self._computation_idx.insert(i, fp.bounds + bound_inset)

        # self._compute_priority_dict = {fp: index for index, fp in enumerate(sorted(computation_fps, key=lambda fp: (-fp.tly, fp.tlx)))}
        # self._priority_to_compute_fp_dict = {index: fp for index, fp in enumerate(sorted(computation_fps, key=lambda fp: (-fp.tly, fp.tlx)))}


        super().__init__(footprint, dtype, nbands, nodata, srs,
                         computation_function,
                         io_pool,
                         computation_pool,
                         primitives,
                         to_collect_of_to_compute,
                         max_computation_size=None,
                         merge_pool=merge_pool,
                         merge_function=merge_function,
                         debug_callbacks=debug_callbacks
                        )


    def _check_cache_file(self, footprint):
        cache_tile_paths = self._get_cache_tile_path(footprint)
        result = False
        if cache_tile_paths:
            for cache_path in cache_tile_paths:
                checksum_dot_tif = cache_path.split('_')[-1]
                file_checksum = checksum_dot_tif.split('.')[0]
                if md5(cache_path) == file_checksum:
                    result = True
                else:
                    os.remove(cache_path)

        return result


    def _get_cache_tile_path_prefix(self, cache_tile):
        """
        Returns a string, which is a path to a cache tile from its fp
        """
        tile_index = self._indices_of_cache_tiles[cache_tile]
        path = str(
            Path(self._cache_dir) /
            "fullsize_{:05d}_{:05d}_tilesize_{:05d}_{:05d}_tilepxindex_{:05d}_{:05d}_tileindex_{:05d}_{:05d}".format(
                *self._full_fp.rsize,
                *cache_tile.rsize,
                *self._full_fp.spatial_to_raster(cache_tile.tl),
                tile_index[0],
                tile_index[1]
            )
        )
        return path


    def _get_cache_tile_path(self, cache_tile):
        """
        Returns a string, which is a path to a cache tile from its fp
        """
        prefix = self._get_cache_tile_path_prefix(cache_tile)
        files_paths = glob.glob(prefix + "*_[a-f0-9]*.tif")
        return files_paths


    def _read_cache_data(self, cache_tile, produce_fp, produced_data, bands):
        """
        reads cache data
        """
        # print(self.h, "reading")

        filepaths = self._get_cache_tile_path(cache_tile)

        assert len(filepaths) == 1, len(filepaths)
        filepath = filepaths[0]

        # Open a raster datasource
        options = ()
        gdal_ds = gdal.OpenEx(
            filepath,
            conv.of_of_mode('r') | conv.of_of_str('raster'),
            ['GTiff'],
            options,
        )
        if gdal_ds is None:
            raise ValueError('Could not open `{}` with `{}` (gdal error: `{}`)'.format(
                filepath, 'GTiff', gdal.GetLastErrorMsg()
            ))

        assert produce_fp.same_grid(cache_tile)

        to_read_fp = produce_fp.intersection(cache_tile)

        rtlx, rtly = cache_tile.spatial_to_raster(to_read_fp.tl)

        assert rtlx >= 0 and rtlx < cache_tile.rsizex
        assert rtly >= 0 and rtly < cache_tile.rsizey

        for band in bands:
            a = gdal_ds.GetRasterBand(band).ReadAsArray(
                int(rtlx),
                int(rtly),
                int(to_read_fp.rsizex),
                int(to_read_fp.rsizey),
                buf_obj=produced_data[to_read_fp.slice_in(produce_fp, clip=True) + (band - 1, )]
            )
            if a is None:
                raise ValueError('Could not read array (gdal error: `{}`)'.format(
                    gdal.GetLastErrorMsg()
                ))


    def _write_cache_data(self, cache_tile, data):
        """
        writes cache data
        """
        # print(self.h, "writing ")
        sr = self.wkt_origin
        filepath = os.path.join(self._cache_dir, str(uuid.uuid4()))

        dr = gdal.GetDriverByName("GTiff")
        if os.path.isfile(filepath):
            err = dr.Delete(filepath)
            if err:
                raise Exception('Could not delete %s' % filepath)

        options = ()
        gdal_ds = dr.Create(
            filepath, cache_tile.rsizex, cache_tile.rsizey, self.nbands, conv.gdt_of_any_equiv(self.dtype), options
        )
        if gdal_ds is None:
            raise Exception('Could not create gdal dataset (%s)' % gdal.GetLastErrorMsg())
        if sr is not None:
            gdal_ds.SetProjection(osr.GetUserInputAsWKT(sr))
        gdal_ds.SetGeoTransform(cache_tile.gt)


        if self.nodata is not None:
            for i in range(self.nbands):
                gdal_ds.GetRasterBand(i + 1).SetNoDataValue(self.nodata)

         # band_schema = None
        # band_schema = None

        # Check array shape
        array = np.asarray(data)

        if array.shape[:2] != tuple(cache_tile.shape):
            raise ValueError('Incompatible shape between array:%s and fp:%s' % (
                array.shape, cache_tile.shape
            )) # pragma: no cover

        # Normalize and check array shape
        if array.ndim == 2:
            array = array[:, :, np.newaxis]
        elif array.ndim != 3:
            raise ValueError('Array has shape %d' % array.shape) # pragma: no cover
        if array.shape[-1] != self.nbands:
            raise ValueError('Incompatible band count between array:%d and band:%d' % (
                array.shape[-1], self.nbands
            )) # pragma: no cover

        # Normalize array dtype
        array = array.astype(self.dtype)

        if array.dtype == np.int8:
            array = array.astype('uint8')

        def _blocks_of_footprint(fp, bands):
            for i, band in enumerate(bands):
                yield fp, band, i # Todo use tile_count and gdal block size

        bands = list(range(1, self.nbands + 1))

        for tile, band, dim in _blocks_of_footprint(cache_tile, bands):
            tilearray = array[:, :, dim][tile.slice_in(cache_tile)]
            assert np.array_equal(tilearray.shape[0:2], cache_tile.shape)
            gdalband = gdal_ds.GetRasterBand(band)
            gdalband.WriteArray(tilearray)

        gdal_ds.FlushCache()
        del gdalband
        del gdal_ds

        file_hash = md5(filepath)

        new_file_path = self._get_cache_tile_path_prefix(cache_tile) + "_" + file_hash + ".tif"

        os.rename(filepath, new_file_path)



    def _update_graph_from_query(self, new_query):
        """
        Updates the dependency graph from the new queries
        """

        # [
        #    [to_collect_p1_1, ..., to_collect_p1_n],
        #    ...,
        #    [to_collect_pp_1, ..., to_collect_pp_n]
        # ]
        # with p # of primitives and n # of to_compute fps

        # initializing to_collect dictionnary
        new_query.to_collect = {key: [] for key in self._primitive_functions.keys()}
        new_query.to_discard = {key: [] for key in self._primitive_functions.keys()}

        self._graph.add_node(
            id(new_query),
            linked_queries=set([new_query]),
        )

        for to_produce, _, to_produce_uid in new_query.to_produce:
            # print(self.h, qrinfo(new_query), f'{"to_produce":>15}', to_produce_uid)
            self._graph.add_node(
                to_produce_uid,
                footprint=to_produce,
                futures=[],
                in_data=None,
                type="to_produce",
                linked_to_produce=set([to_produce_uid]),
                linked_queries=set([new_query]),
                is_flat=new_query.is_flat,
                bands=new_query.bands
            )
            to_read_tiles = self._to_read_of_to_produce(to_produce)

            self._graph.add_edge(id(new_query), to_produce_uid)

            for to_read in to_read_tiles:
                to_read_uid = str(uuid.uuid4())
                # to_read_uid = get_uname()
                # print(self.h, qrinfo(new_query), f'{"to_read":>15}', to_read_uid)

                self._graph.add_node(
                    to_read_uid,
                    footprint=to_read,
                    future=None,
                    type="to_read",
                    pool=self._io_pool,
                    linked_to_produce=set([to_produce_uid]),
                    linked_queries=set([new_query]),
                    bands=new_query.bands
                )
                self._graph.add_edge(to_produce_uid, to_read_uid)

                # if the tile is not written, writing it
                if not self._is_written(to_read):
                    to_write = to_read

                    to_write_uid = str(repr(to_write) + "to_write")
                    # print(self.h, qrinfo(new_query), f'{"to_write":>15}', to_write_uid)
                    if to_write_uid in self._graph.nodes():
                        self._graph.nodes[to_write_uid]["linked_to_produce"].add(to_produce_uid)
                        self._graph.nodes[to_write_uid]["linked_queries"].add(new_query)
                    else:
                        self._graph.add_node(
                            to_write_uid,
                            footprint=to_write,
                            future=None,
                            type="to_write",
                            pool=self._io_pool,
                            in_data=None,
                            linked_to_produce=set([to_produce_uid]),
                            linked_queries=set([new_query]),
                            bands=new_query.bands
                        )
                    self._graph.add_edge(to_read_uid, to_write_uid)

                    to_merge = to_write
                    to_merge_uid = str(repr(to_merge) + "to_merge")
                    # print(self.h, qrinfo(new_query), f'{"to_merge":>15}', to_merge_uid)
                    if to_merge_uid in self._graph.nodes():
                        self._graph.nodes[to_merge_uid]["linked_to_produce"].add(to_produce_uid)
                        self._graph.nodes[to_merge_uid]["linked_queries"].add(new_query)
                    else:
                        self._graph.add_node(
                            to_merge_uid,
                            footprint=to_merge,
                            future=None,
                            futures=[],
                            type="to_merge",
                            pool=self._merge_pool,
                            in_data=[],
                            in_fp=[],
                            linked_to_produce=set([to_produce_uid]),
                            linked_queries=set([new_query]),
                            bands=new_query.bands
                        )
                    self._graph.add_edge(to_write_uid, to_merge_uid)

                    to_compute_multi = self._to_compute_of_to_write(to_write)

                    for to_compute in to_compute_multi:
                        to_compute_uid = str(repr(to_compute) + "to_compute")
                        # print(self.h, qrinfo(new_query), f'{"to_compute":>15}', to_compute_uid)
                        if to_compute not in new_query.to_compute:
                            new_query.to_compute.append(to_compute)
                        if to_compute_uid in self._graph.nodes():
                            self._graph.nodes[to_compute_uid]["linked_to_produce"].add(to_produce_uid)
                            self._graph.nodes[to_compute_uid]["linked_queries"].add(new_query)
                        else:
                            self._graph.add_node(
                                to_compute_uid,
                                footprint=to_compute,
                                future=None,
                                type="to_compute",
                                pool=self._computation_pool,
                                in_data=None,
                                linked_to_produce=set([to_produce_uid]),
                                linked_queries=set([new_query]),
                                bands=new_query.bands
                            )

                            if self._to_collect_of_to_compute is not None:
                                multi_to_collect = self._to_collect_of_to_compute(to_compute)

                                if multi_to_collect.keys() != self._primitive_functions.keys():
                                    raise ValueError("to_collect keys do not match primitives")

                                for key in multi_to_collect:
                                    new_query.to_collect[key].append(multi_to_collect[key])

                        self._graph.add_edge(to_merge_uid, to_compute_uid)

        new_query.collected = self._collect_data(new_query.to_collect)


    def _to_read_of_to_produce(self, fp):
        to_read_list = self._cache_idx.intersection(fp.bounds)
        return [list(self._cache_tiles.flat)[i] for i in to_read_list]

    def _is_written(self, cache_fp):
        return self._cache_checksum_array[self._indices_of_cache_tiles[cache_fp]]

    def _to_compute_of_to_write(self, fp):
        to_compute_list = self._computation_idx.intersection(fp.bounds)
        return [list(self._computation_tiles.flat)[i] for i in to_compute_list]
