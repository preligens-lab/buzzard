"""doc/exemples.ipynb dep"""

import os

import boto3
import buzzard as buzz
from osgeo import gdal
import shapely
import scipy.ndimage
import numpy as np
import cv2

EPSG29100 = '+proj=poly +lat_0=0 +lon_0=-54 +x_0=5000000 +y_0=10000000 +ellps=GRS67 +units=m +no_defs '

ROOFS_ALL_PATH_S3 = 'AOI_1_Rio/srcData/buildingLabels/Rio_Buildings_Public_AOI_v2.geojson' # 151MB
# MULTISPECTRAL_PATH_S3 = 'AOI_1_Rio/srcData/mosaic_8band/013022223130.tif' # 401M
RGB_PATH_S3 = 'AOI_1_Rio/srcData/mosaic_3band/013022223130.tif' # 61MB

BUCKET_NAME = 'spacenet-dataset'

def prepare(directory, aws_access_key_id, aws_secret_access_key):
    """Download dataset from `s3://spacenet-dataset` and transform those files from `wgs84` to a
    local `epsg29100`

    Why is this file importing gdal?
    --------------------------------
    `wgs84` files are poorly supported by `Buzzard`, the library was initially designed to work
    with metric projections. The use of `gdal.Warp ` inside buzzard.DataSource is planned for a
    future version.
    """

    s3 = boto3.resource(
        's3',
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key,
    )
    bucket = s3.Bucket(BUCKET_NAME)

    roofs_all_wgs84_path = os.path.join(directory, 'roofs_all_wgs84.geojson')
    # multispectral_wgs84_path = os.path.join(directory, '8bands_wgs84.tif')
    rgb_wgs84_path = os.path.join(directory, '3bands_wgs84.tif')

    roofs_epsg29100_path = os.path.join(directory, 'roofs_epsg29100.shp')
    # multispectral_epsg29100_path = os.path.join(directory, '8bands_epsg29100.tif')
    rgb_epsg29100_path = os.path.join(directory, '3bands_epsg29100.tif')

    # Download ********************************************************************************** **
    def _download(srcpath, dstpath):
        srcpath_txt = os.path.join('s3://', BUCKET_NAME, srcpath)
        print("Downloading: {}".format(srcpath_txt), flush=True)
        if os.path.isfile(dstpath):
            print("  {} already exits".format(dstpath), flush=True)
            return
        bucket.download_file(srcpath, dstpath, {'RequestPayer':'requester'})
        print("Downloaded: {}\n  to: {}".format(srcpath_txt, dstpath), flush=True)

    _download(ROOFS_ALL_PATH_S3, roofs_all_wgs84_path)
    # _download(MULTISPECTRAL_PATH_S3, multispectral_wgs84_path)
    _download(RGB_PATH_S3, rgb_wgs84_path)

    # Reproj rasters **************************************************************************** **
    def _reproj_raster(srcpath, dstpath):
        print("Transforming: {}".format(srcpath), flush=True)
        if os.path.isfile(dstpath):
            print("  {} already exits".format(dstpath), flush=True)
            return
        gds_src = gdal.OpenEx(srcpath)
        opt = gdal.WarpOptions(
            format='GTiff', dstSRS=EPSG29100,
            resampleAlg=gdal.GRIORA_NearestNeighbour, errorThreshold=0,
            multithread=True,
            warpOptions=['NUM_THREADS=ALL_CPUS']
        )
        gds = gdal.Warp(dstpath, gds_src, options=opt)
        del gds, gds_src
        print("Transformed: {}\n  to: {}".format(srcpath, dstpath), flush=True)

    # _reproj_raster(multispectral_wgs84_path, multispectral_epsg29100_path)
    _reproj_raster(rgb_wgs84_path, rgb_epsg29100_path)

    # Reproj vectors **************************************************************************** **
    def _reproj_and_clip_vector(srcpath, dstpath):
        print("Transforming: {}".format(srcpath), flush=True)
        if os.path.isfile(dstpath):
            print("  {} already exits".format(dstpath), flush=True)
            return
        with buzz.Env(significant=10, allow_complex_footprint=True, warnings=False):
            ds = buzz.DataSource(sr_work='WGS84', analyse_transformation=False)
            ds.open_vector('src', srcpath, driver='GeoJSON')
            ds.open_raster('raster', rgb_wgs84_path)
            ds.create_vector(
                'dst'OB, dstpath, 'polygon', driver='ESRI Shapefile', sr=EPSG29100,
            )

            # Iterate over all geoJSON geometries overlapping with raster
            #   Ignoring geoJSON fields
            #   Save all polygons to Shapefile
            for geom in ds.src.iter_data(None, mask=ds.raster.fp, clip=True):
                if isinstance(geom, shapely.geometry.Polygon):
                    ds.dst.insert_data(geom)
                elif isinstance(geom, shapely.geometry.MultiPolygon):
                    for poly in geom.geoms:
                        ds.dst.insert_data(poly)
        print("Transformed: {}\n  to: {}".format(srcpath, dstpath), flush=True)

    _reproj_and_clip_vector(roofs_all_wgs84_path, roofs_epsg29100_path)

    return {
        '3bands_epsg29100': rgb_epsg29100_path,
        # '3bands_wgs84': rgb_wgs84_path,
        # '8bands_epsg29100': multispectral_epsg29100_path,
        # '8bands_wgs84': multispectral_wgs84_path,
        # 'roofs_all_wgs84': roofs_all_wgs84_path,
        'roofs_epsg29100': roofs_epsg29100_path,
    }

def create_text_mask(text, font_face=cv2.FONT_HERSHEY_SIMPLEX, font_scale=2, thickness=2):
    """Build a binary image with text drawn in it"""
    color = [1]

    (w, h), _ = cv2.getTextSize(
        text, fontFace=font_face, fontScale=font_scale, thickness=thickness
    )
    border = 30
    dst = np.zeros((h + border, w + border), dtype='uint8')
    cv2.putText(
        dst, text=text, org=(border // 2, h + border // 2),
        fontFace=font_face, fontScale=font_scale,
        thickness=thickness, color=color
    )

    ymask = dst.any(1).cumsum()
    ymask = (ymask != 0) & (ymask != ymask[-1])
    xmask = dst.any(0).cumsum()
    xmask = (xmask != 0) & (xmask != xmask[-1])
    dst = dst[ymask][:, xmask]

    return dst.astype(bool)
