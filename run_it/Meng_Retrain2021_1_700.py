from datetime import datetime
import functools
from io import StringIO
from math import ceil, log
import os
import time
import urllib.parse as urlparse

import boto3
import click
import numpy as np
import pandas as pd
import rasterio as rstr
from rasterio.crs import CRS
import rasterio.shutil as rshutil
from rasterio.transform import Affine
import rasterio.warp as rwarp
import yaml

import geopyspark as gps
from pyrasterframes import *
from pyrasterframes.types import NoDataFilter
from pyrasterframes.rasterfunctions import assembleTile, erodePixels, localSlope, localNbhdOp, explodeTiles
from pyspark.ml import Pipeline
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.feature import VectorAssembler
from pyspark.mllib.evaluation import MulticlassMetrics, BinaryClassificationMetrics
from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import col, lit
import pyspark.sql.functions as F
from pyspark.sql.types import FloatType, StructType, StructField, StringType, IntegerType, BooleanType

from pyspark.sql.functions import isnan, when, count, col

_GDAL_DATA = '/usr/local/share/gdal'

def cell_size(layout):
    """
    Determine the geodetic dimensions of a pixel in a tile of a given layout

    Args:
        layout (gps.LayoutDefinition)

    Returns:
        (float, float) tuple
    """
    ex = layout.extent
    w = float(abs(ex.xmax - ex.xmin))
    h = float(abs(ex.ymax - ex.ymin))
    tl = layout.tileLayout
    return (w / (tl.layoutCols * tl.tileCols), h / (tl.layoutRows * tl.tileRows))


def extent_for_cell(layout, cell):
    """
    Compute the geodetic extent of a specific tile in a layout

    Args:
        layout (``gps.LayoutDefinition``)
        cell (``gps.SpatialKey`` or ``gps.SpaceTimeKey``)
    Returns:
        ``gps.Extent``
    """
    if isinstance(cell, (gps.SpatialKey, gps.SpaceTimeKey)):
        col = cell.col
        row = cell.row
    elif isinstance(cell, tuple):
        col = cell[0]
        row = cell[1]
    else:
        raise TypeError("extent_for_cell() expects SpatialKey, SpaceTimeKey, or tuple")

    w = (layout.extent.xmax - layout.extent.xmin) / layout.tileLayout.layoutCols
    h = (layout.extent.ymax - layout.extent.ymin) / layout.tileLayout.layoutRows
    x0 = layout.extent.xmin + col * w
    y0 = layout.extent.ymax - (row + 1) * h

    return gps.Extent(x0, y0, x0 + w, y0 + h)


def buffered_cell_extent(layout, px_buffer, cell):
    """
    Compute the extent of a cell in a layout with a buffer

    This funtion computes the extent of a cell and adds ``px_buffer`` worth of area
    on all sides.  That is, if the tile dimension in a given layout is n x n pixels,
    then this function returns the extent for a ``(n + 2 * px_buffer)`` square region
    centered on the given cell.

    Args:
        layout (``gps.LayoutDefinition``)
        px_buffer (int): number of pixels to pad the border of the extent with
        cell (``gps.SpatialKey`` or ``gps.SpaceTimeKey``): identifier of the desired
            layout cell

    Returns:
        ``gps.Extent``
    """
    ex = extent_for_cell(layout, cell)
    cx, cy = cell_size(layout)
    return gps.Extent(ex.xmin - cx * px_buffer, ex.ymin - cy * px_buffer, ex.xmax + cx * px_buffer,
                      ex.ymax + cy * px_buffer)


def read_buffered_window(uri, metadata, px_buffer, cell, resampling):
    """
    Read a region from a source image, reprojecting in the process.

    Given the metadata for a layer (including a layout), a cell id, and a buffer
    size, read a window from the supplied image while reprojecting the source image
    to the CRS specified by the metadata.

    In concept, we specify a grid in the target CRS using the cell and resolution in
    the metadata, padding with px_buffer additional cells on all sides of the cell.
    For each cell center, we read from the source image after back-projecting the
    target location to the source image's coordinate system and applying the chosen
    resampling kernel.

    Args:
        uri (str): The source image URI; any scheme interpretable by GDAL is allowed
        metadata (``gps.Metadata``): The source layer metadata
        px_buffer (int): The number of pixels worth of padding that should be added
            to the extent to be read and added to the tile.
        cell (``gps.SpatialKey`` or ``gps.SpaceTimeKey``): The key of the target
            region
        resampling (``rasterio.warp.Resampling``): The resampling method to be used
            while reading from the source image

    Returns:
        ``gps.Tile``
    """
    if ("GDAL_DATA" not in os.environ) and '_GDAL_DATA' in vars():
        os.environ["GDAL_DATA"] = _GDAL_DATA

    layout = metadata.layout_definition
    dest_bounds = buffered_cell_extent(layout, px_buffer, cell)
    res = cell_size(metadata.layout_definition)
    dest_transform = Affine(res[0], 0, dest_bounds.xmin, 0, -res[1], dest_bounds.ymax)
    dest_width = max(int(ceil((dest_bounds.xmax - dest_bounds.xmin) / res[0])), 1)
    dest_height = max(int(ceil((dest_bounds.ymax - dest_bounds.ymin) / res[1])), 1)
    dest_transform, dest_width, dest_height = rwarp.aligned_target(dest_transform, dest_width, dest_height, res)

    with rstr.open(uri) as src:
        tile = np.zeros(
            (src.count, layout.tileLayout.tileCols + 2 * px_buffer, layout.tileLayout.tileRows + 2 * px_buffer))
        rwarp.reproject(
            source=rstr.band(src, list(range(1, src.count + 1))),
            destination=tile,
            src_transform=src.transform,
            src_crs=src.crs,
            src_nodata=src.nodata,
            dst_transform=dest_transform,
            dst_crs=CRS.from_epsg(4326),
            dst_nodata=src.nodata,
            resampling=resampling,
            num_threads=1)

    return gps.Tile.from_numpy_array(tile)


def write_cog(layout, location, data, filename_template):
    """
    Write a GPS tile out as a Cloud-Optimized GeoTIFF

    Uses GDAL to write a COG out to disk, utilizing the given layout and location
    to generate the GeoTIFF header.

    Args:
        layout (``gps.LayoutDefinition``): The layout for the source layer
        location (``gps.SpatialKey`` or ``gps.SpaceTimeKey``): The cell identifier
            in the layer corresponding to the image data
        data (``gps.Tile``): The image data
        filename_template (str): A pattern giving the destination for the target
            image.  Contains two '{}' tokens which will be ``.format``ed with the
            column and row of the ``location``, in that order.  May be an S3 uri or
            local path.
    """
    bands, w, h = data.cells.shape
    nodata = data.no_data_value
    dtype = data.cells.dtype
    cw, ch = cell_size(layout)
    ex = extent_for_cell(layout, location)
    overview_level = int(log(w) / log(2) - 8)

    with rstr.io.MemoryFile() as memfile:
        with memfile.open(driver='GTiff',
                          count=bands,
                          width=w,
                          height=h,
                          transform=Affine(cw, 0.0, ex.xmin,
                                           0.0, -ch, ex.ymax),
                          crs=rstr.crs.CRS.from_epsg(4326),
                          nodata=nodata,
                          dtype=dtype,
                          compress='lzw',
                          tiled=True) as mem:
            windows = list(mem.block_windows(1))
            for _, w in windows:
                segment = data.cells[:, w.row_off:(w.row_off + w.height), w.col_off:(w.col_off + w.width)]
                mem.write(segment, window=w)
                mask_value = np.all(segment != nodata, axis=0).astype(np.uint8) * 255
                mem.write_mask(mask_value, window=w)

            overviews = [2 ** j for j in range(1, overview_level + 1)]
            mem.build_overviews(overviews, rwarp.Resampling.nearest)
            mem.update_tags(ns='rio_oveview', resampling=rwarp.Resampling.nearest.value)

            uri = urlparse.urlparse(filename_template)
            if uri.scheme == 's3':
                boto3.resource('s3').Bucket(uri.netloc).upload_fileobj(memfile,
                                                                       uri.path.format(location.col, location.row)[1:])
            else:
                rshutil.copy(mem, filename_template.format(location.col, location.row), copy_src_overviews=True)


def write_bytes_to_s3(uri, byte_array):
    """
    For writing plain geotiff that can be read by R, Python, and QGIS 2.18
    """

    parsed = urlparse.urlparse(uri)
    boto3.resource('s3').Object(parsed.netloc, parsed.path[1:]).put(Body=byte_array)


def read_training_mask_image(bucket, prefix, record):
    """
    Fetch a mask image from S3

    Reads an image file from a location on S3

    Args:
        bucket (str): Name of the source S3 bucket
        prefix (str): Prefix of the S3 key
        record (``pyspark.sql.Row`` or dict): Structure with ``name``,  ``row``, and
            ``col`` members.

    Returns:
        ``(gps.SpatialKey, gps.Tile)`` tuple
    """
    if('level' in record):
        uri = "s3://{}/{}/{}/{}".format(bucket, prefix, record.level, record.lbname)
    else:
        uri = "s3://{}/{}/{}_{}_{}.tif".format(bucket, prefix, record.name, record.col, record.row)
    # uri = "s3://{}/{}/{}_{}_{}.tif".format(bucket, prefix, record.name, record.col, record.row)
    spatial_key = gps.SpatialKey(int(record.col), int(record.row))

    with rstr.open(uri) as dataset:
        img = dataset.read()
        nodata = dataset.nodata

    return (spatial_key, gps.Tile.from_numpy_array(img, no_data_value=nodata))


def get_masks_from_incoming_names(incoming_names, bucket, prefix, metadata, target_column='mask'):
    """
    Constructs a RasterFrame layer from a DataFrame of desired sites and layout
    information

    Given a DataFrame with ``name``, ``col``, and ``row`` columns, create a tiled
    raster layer from images stored in files with a URI pattern of
    ``s3://<bucket>/<prefix>/<name>_<col>_<row>.tif``.  Assumes mask images have the
    same dimension and CRS as required by the provided metadata.

    Args:
        incoming_names (``pyspark.sql.DataFrame``)
        bucket (str): S3 bucket name
        prefix (str): S3 prefix location containing source images
        metadata (``gps.Metadata``): Metadata describing target layer
        target_column (str): The name of the column in the output RasterFrame to
            store image data

    Returns:
        ``pyrasterframes.RasterFrame``
    """
    return gps.TiledRasterLayer.from_numpy_rdd(gps.LayerType.SPATIAL,
                                               incoming_names.rdd.map(
                                                   lambda row: read_training_mask_image(bucket, prefix, row)
                                               ),
                                               metadata) \
        .to_rasterframe(1) \
        .withColumnRenamed('tile', target_column)


def build_feature_frame(uri_frame, season, metadata, resampling=rwarp.Resampling.bilinear):
    """
    Produce a RasterFrame from remote imagery, including derived features

    Takes a DataFrame with three columns: ``col``, ``row``, and one containing a URI
    under the name provided by season.  Reads windows from the images given in the
    URIs and constructs a RasterFrame with three columns for each band of the source
    image: the raw values, a 5x5 average filter, and the slope.  The resulting
    feature columns are named according to '<season>_<kind>_<band #>' where kind is
    'raw', 'avg', or 'slope'.

    Note:
        The windowed read is buffered and may trigger a reprojection to the CRS
        specified in the metadata using the provided resampling mode.  These reads
        may introduce no data values if the desired region (plus buffer) extends
        beyond the viable image data in the source image.  Be sure the requested
        region is a subset of the image data.

    Args:
        uri_frame (``pyspark.sql.DataFrame``)
        season (str)
        metadata (``gps.Metadata``): Metadata for the target layer
        resampling (``rasterio.warp.Resampling``)

    Returns:
        ``pyrasterframes.RasterFrame``
    """

    def remap(c, kind):
        if c.startswith('tile_'):
            i = int(c.split('_')[1])
            return '{}_{}_{}'.format(season, kind, i)

        return '{}_{}_0'.format(season, kind)

    image_rdd = uri_frame.rdd \
        .map(lambda r: \
                 (gps.SpatialKey(int(r.col), int(r.row)),
                  read_buffered_window(r[season],
                                       metadata,
                                       5,
                                       (int(r.col), int(r.row)),
                                       resampling)
                  )
             )

    try:
        bands = image_rdd.first()[1].cells.shape[0]
    except:
        bands = 1

    frame = gps.TiledRasterLayer.from_numpy_rdd(gps.LayerType.SPATIAL,
                                                image_rdd,
                                                metadata).to_rasterframe(bands)

    cw, ch = cell_size(metadata.layout_definition)
    columns = [c for c in frame.columns if c.startswith('tile')]

    result = frame.select(['spatial_key'] +
                          [erodePixels(c, 5).alias(remap(c, 'raw')) for c in columns] +
                          [localNbhdOp(c, 11, 'mean').alias(remap(c, 'avg')) for c in columns] +
                          [erodePixels(localNbhdOp(c, 5, 'stddev'), 3).alias(remap(c, 'std')) for c in columns])

    return result


def features_from_uri_frame(uris, metadata, feature_names):
    """
    Reads images and generates features for 'OS' and 'GS' seasons

    A utility wrapper around ``build_feature_frame`` which creates the feature
    dataframes for the OS and GS features and joins the results.

    Args:
        uris (``pyspark.DataFrame``): A dataframe containing columns ``col``,
        ``row``, ``OS``, and ``GS``
        metadata (``gps.Metadata``)
        feature_names (list of str): The names of the expected features in the
            resulting RasterfFrame

    Returns:
        ``pyrasterframes.RasterFrame``
    """
    return build_feature_frame(uris, 'GS', metadata) \
        .alias('gs') \
        .join(build_feature_frame(uris, 'OS', metadata).alias('os'),
              (col('gs.spatial_key.col') == col('os.spatial_key.col')) &
              (col('gs.spatial_key.row') == col('os.spatial_key.row'))) \
        .select(['gs.spatial_key'] + feature_names)


def gather_data(all_uris, names, metadata, feature_names, s3_bucket, label_path='labels', include_masks=False):
    """
    Assembles a complete RasterFrame including all image features and (optionally)
    mask images

    This is a wrapper function around ``features_from_uri_frame`` and
    ``get_masks_from_incoming_names``.  Coerces data from project-specific formats
    to the form required by the utility code above.

    Args:
        all_uris (``pyspark.DataFrame``): A dataframe with columns ``col``,
            ``row``, ``GS``, and ``OS``, the latter two supplying image URIs that
            cover the  the listed col and row in the layout for the corresponding
            season
        names (``pyspark.DataFrame``): A dataframe with columns ``col``, ``row``,
            and ``name``, the latter giving the country-specific grid id
        metadata (``gps.Metadata``): The metadata for the target layer
        feature_names (list of str): The names of the desired feature data
        s3_bucket (str): The name of the bucket containing the project data
        include_masks (bool): Whether to include mask data for the desired cells

    Returns:
        ``pyrasterframes.RasterFrame``
    """
    uris = all_uris \
        .join(names.alias('sites'), (col('sites.col') == col('gs.col')) & (col('sites.row') == col('gs.row'))) \
        .select(col('sites.name').alias('name'), 'gs.col', 'gs.row', 'GS', 'OS')
    features = features_from_uri_frame(uris, metadata, feature_names)

    if not include_masks:
        return features.select('spatial_key', explodeTiles(*feature_names)).repartition('column_index', 'row_index')

    masks = get_masks_from_incoming_names(names, s3_bucket, label_path, metadata)
    return features.join(masks.alias('masks'),
                         (col('masks.spatial_key.col') == features.spatial_key.col) &
                         (col('masks.spatial_key.row') == features.spatial_key.row)) \
        .select(['masks.spatial_key', 'masks.mask'] + feature_names) \
        .select('spatial_key', explodeTiles(*(['mask'] + feature_names))) \
        .repartition('column_index', 'row_index')


def ml_pipeline(feature_names, label_column, output_column='features'):
    """
    Produce the requisite Pipeline object for the machine learning process
    """
    ndFilter = NoDataFilter()
    ndFilter.setInputCols(feature_names + [label_column])

    assembler = VectorAssembler(inputCols=feature_names, outputCol=output_column)

    classifier = RandomForestClassifier(labelCol=label_column, featuresCol=assembler.getOutputCol()) \
        .setSubsamplingRate(0.1) \
        .setMaxDepth(15) \
        .setNumTrees(60)
    # .setMaxDepth(10)\
    # .setNumTrees(100)\

    return Pipeline(stages=[ndFilter, assembler, classifier])


def balance_samples(spark, feature_df, label_col):
    """
    Create balanced sample of input data

    For a binary target column, sample the rows with replacement so as to create a
    synthetic data set with even representation for each class.  This expects the
    labeled column to have values of 0 or 1.

    Args:
        spark (``pyspark.sql.SparkSession``)
        feature_df (``pyspark.sql.DataFrame``): The source data
        label_col (str): The name of the column to rebalance the data by
    """
    counts = dict(map(lambda r: (r[label_col], r['count']), feature_df.groupBy(label_col).count().collect()))
    n_samples = functools.reduce(lambda a, b: a + b, counts.values())
    fractions = {1: float(n_samples) / counts[1] * 0.5, 0: float(n_samples) / counts[0] * 0.5}
    rdd = feature_df.rdd.map(lambda r: (r[label_col], r)).sampleByKey(True, fractions=fractions)
    return spark.createDataFrame(rdd.values(), feature_df.schema)


def parse_yaml_from_s3(bucket, prefix):
    s3 = boto3.resource('s3')
    obj = s3.Bucket(bucket).Object(prefix).get()['Body'].read()
    return yaml.load(obj)


def pd_df_to_s3_csv(df, bucket, prefix):
    csv_buffer = StringIO()
    df.to_csv(csv_buffer, index=False)
    boto3.resource('s3').Object(bucket, prefix).put(Body=csv_buffer.getvalue())


'''Config'''
config_filename = 'config_1_Meng2021_Retrain12515_700.yaml'
random_seed = None
output_all_images = False
s3_bucket = "activemapper"
run_id = 0

conf = gps.geopyspark_conf(appName="CVML Model Iteration", master="yarn").set("spark.dynamicAllocation.enabled", True).set("spark.ui.enabled", True).set("spark.hadoop.yarn.timeline-service.enabled", False).set("spark.sql.broadcastTimeout", "20000").set("spark.executor.instances", "59").set("spark.executor.cores", "5").set("spark.executor.memory", "7g").set("spark.driver.memory", "7g").set("spark.driver.cores", "5").set("spark.sql.shuffle.partitions", "590").set("spark.default.parallelism", "590")
spark = SparkSession.builder.config(conf=conf).getOrCreate().withRasterFrames()
logger = spark._jvm.org.apache.log4j.Logger.getLogger("Main")

params = parse_yaml_from_s3(s3_bucket, config_filename)['learner1']
label_path = parse_yaml_from_s3(s3_bucket, config_filename)['labeller']['consensus_directory'][1:-1]
s3_prefix = params['prefix']
s3_prefix = s3_prefix[0:-1] if s3_prefix.endswith('/') else s3_prefix

catalog_prefix = params['image_catalog']
catalog_prefix_fix = params['image_catalog_fix']

feature_names = functools.reduce(lambda a, b: a + b, [["{}_raw_{}".format(season, n),
                                                       "{}_avg_{}".format(season, n),
                                                       "{}_std_{}".format(season, n)]
                                                      for season in ["GS", "OS"] for n in range(1, 5)])

master_layout = gps.LayoutDefinition(gps.Extent(-17.541, -35.46, 51.459, 37.54), gps.TileLayout(13800, 14600, 200, 200))
master_metadata = gps.Metadata(gps.Bounds(gps.SpatialKey(0, 0), gps.SpatialKey(13800, 14600)),
                               "+proj=longlat +datum=WGS84 +no_defs ",
                               gps.CellType.INT8,
                               master_layout.extent,
                               master_layout)

'''Train Source'''
print("Reading source tables")
checkpoint = time.time()
f_pool = spark\
     .read\
     .option('inferScheme', True)\
     .option('header', True)\
     .csv('s3n://{}/{}/{}'.format(s3_bucket, s3_prefix, params['pool']))\
     .repartition('col', 'row')

qs_in = spark \
    .read \
    .option('inferScheme', True) \
    .option('header', True) \
    .csv('s3n://{}/{}/{}'.format(s3_bucket, s3_prefix, params['qs'])) \
    .repartition('col', 'row')

incoming_200 = spark.read\
                .option('header', True)\
                .schema(StructType([
                    StructField('name', StringType()),
                    StructField('run', IntegerType()),
                    StructField('iteration', IntegerType()),
                    StructField('processed', BooleanType()),
                    StructField('usage', StringType()),
                    StructField('label', StringType())
                ]))\
                .csv('s3n://{}/{}/{}'.format(s3_bucket, s3_prefix, params['incoming_names']))
incoming_500 = spark.read\
                .option('header', True)\
                .schema(StructType([
                    StructField('name', StringType()),
                    StructField('run', IntegerType()),
                    StructField('iteration', IntegerType()),
                    StructField('processed', BooleanType()),
                    StructField('usage', StringType()),
                    StructField('label', StringType())
                ]))\
                .csv('s3n://{}/{}/{}'.format(s3_bucket, s3_prefix, params['incoming_names_static']))

all_names_200 = f_pool.join(incoming_200.select('name', 'usage'),
                        f_pool.name == incoming_200.name,
                        how='left')\
                  .select(f_pool.name.alias('name'), 'col', 'row', 'usage')
all_names_500 = f_pool.join(incoming_500.select('name', 'usage'),
                    f_pool.name == incoming_500.name,
                    how='left')\
              .select(f_pool.name.alias('name'), 'col', 'row', 'usage')
all_names_500 = all_names_500.filter(all_names_500.name != "GH0105923")
incoming = incoming_200.union(incoming_500)
test_names = f_pool.join(incoming.select('name'), 'name', 'left_anti').withColumn("usage",lit("test"))
num_test_images = test_names.count()
image_catalog = spark.read\
                      .option('inferScheme', True)\
                      .option('header', True)\
                      .csv('s3n://{}/{}'.format(s3_bucket, catalog_prefix))\
                      .repartition('col', 'row')
all_image_uris = image_catalog\
                 .filter(image_catalog['season'] == 'GS')\
                 .alias('gs')\
                 .join(image_catalog.filter(image_catalog['season'] == 'OS').alias('os'),
                       (col('gs.col') == col('os.col')) & (col('gs.row') == col('os.row')))\
                 .select(col('gs.col'), col('gs.row'), col('gs.uri').alias('GS'), col('os.uri').alias('OS'))

print("Reading training labels & building training features")
# gather data from label fix
training_data01 = gather_data(all_image_uris,
                            all_names_500.filter(all_names_500.usage == 'train'),
                            master_metadata,
                            feature_names,
                            s3_bucket,
                            'labels_fix',
                            include_masks=True)
# gather data from label
training_data02 = gather_data(all_image_uris,
                            all_names_200.filter(all_names_200.usage == 'train'),
                            master_metadata,
                            feature_names,
                            s3_bucket,
                            label_path,
                            include_masks=True)
# merge data
training_data = training_data01.union(training_data02)

print("Balancing data")
checkpoint = time.time()
balanced_data = balance_samples(spark, training_data, 'mask')
# balanced_data.show()
print("Elapsed time for balancing data: {}s".format(time.time() - checkpoint))

print("Training model")
checkpoint = time.time()
pipeline = ml_pipeline(feature_names, 'mask')
model = pipeline.fit(balanced_data)
print(model)
print("Elapsed time for training the model: {}s".format(time.time() - checkpoint))

'''Validation'''
aoi = 1
runid_val = 0  # aoi3 = 0
fn_f_pool_val = 'f_pool_{}.csv'.format(aoi)
incoming_names_val = 'incoming_names_{}.csv'.format(aoi)
f_pool_val = spark\
     .read\
     .option('inferScheme', True)\
     .option('header', True)\
     .csv('s3n://{}/{}/{}'.format(s3_bucket, s3_prefix, fn_f_pool_val))\
     .repartition('col', 'row')
incoming_val = spark.read\
                .option('header', True)\
                .schema(StructType([
                    StructField('name', StringType()),
                    StructField('run', IntegerType()),
                    StructField('iteration', IntegerType()),
                    StructField('processed', BooleanType()),
                    StructField('usage', StringType()),
                    StructField('label', StringType())
                ]))\
                .csv('s3n://{}/{}/{}'.format(s3_bucket, s3_prefix, incoming_names_val))

incoming_val = incoming_val.filter(incoming_val['run']==runid_val).filter(incoming_val['label']==True)
test_names_val = f_pool_val.join(incoming_val.select('name'), 'name', 'left_anti').withColumn("usage",lit("test"))
all_names_val = f_pool_val.join(incoming_val.select('name', 'usage'),
                        f_pool_val.name == incoming_val.name,
                        how='left')\
                  .select(f_pool_val.name.alias('name'), 'col', 'row', 'usage')
num_test_images_val = test_names_val.count()
print('Number of test images:', num_test_images_val)

catalog_prefix_val = 'planet/planet_catalog_{}_fix.csv'.format(aoi)
image_catalog_val = spark.read\
                      .option('inferScheme', True)\
                      .option('header', True)\
                      .csv('s3n://{}/{}'.format(s3_bucket, catalog_prefix_val))\
                      .repartition('col', 'row')
all_image_uris_val = image_catalog_val\
                 .filter(image_catalog_val['season'] == 'GS')\
                 .alias('gs')\
                 .join(image_catalog_val.filter(image_catalog_val['season'] == 'OS').alias('os'),
                       (col('gs.col') == col('os.col')) & (col('gs.row') == col('os.row')))\
                 .select(col('gs.col'), col('gs.row'), col('gs.uri').alias('GS'), col('os.uri').alias('OS'))
print("Elapsed time for reading source tables: {}s".format(time.time() - checkpoint))

fn_outmetrics = 'incoming_metrics_Meng2021_Retrain700_{}.csv'.format(aoi)
label_path = 'labels_fix'

'''Validating model results'''
checkpoint = time.time()
validation_data = gather_data(all_image_uris_val,
                              all_names_val.filter(all_names_val.usage == 'validate'),
                              master_metadata,
                              feature_names,
                              s3_bucket,
                              label_path,
                              include_masks=True)

valid_fit = model.transform(validation_data).select('prediction', 'probability', 'mask')

metrics = MulticlassMetrics(valid_fit.rdd.map(lambda r: (r.prediction, r.mask)))
confusion_matrix = metrics.confusionMatrix().toArray().flatten().tolist() #left to right, top to bottom
tss = 1.0 * confusion_matrix[3] / (confusion_matrix[3] + confusion_matrix[2]) + \
      1.0 * confusion_matrix[0] / (confusion_matrix[0] + confusion_matrix[1]) - 1
binmetrics = BinaryClassificationMetrics(valid_fit.rdd.map(lambda r: (float(r['probability'][1]), r['mask'])))

last_iteration = incoming.agg(F.max('iteration')).collect()[0][0]
report = pd.DataFrame({
    'run':            [run_id],
    'iteration':      [last_iteration + 1],
    'tss':            [tss],
    'accuracy':       [metrics.accuracy],
    'precision':      [metrics.precision(1.0)],
    'recall':         [metrics.recall(1.0)],
    'fpr':            [metrics.falsePositiveRate(1.0)],
    'tpr':            [metrics.truePositiveRate(1.0)],
    'AUC':            [binmetrics.areaUnderROC],
    'aoi':            [aoi],
    'iteration_time': [datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S.%f')]
    })
# TODO: allow target location to be derived from params (local or s3)
# added because of an error where incoming_metrics.csv contained different iteration number (10)
# than expected by DB (4). Ryan's guess is that this is due to multiple test clusters overwriting csv
# print("############Old Iteration Metrics  to overwrite###########")
# incoming_previous = pd.read_csv(os.path.join("s3://",s3_bucket,s3_prefix,params['metrics']))
# print(incoming_previous.to_string())
# print("############New Iteration Metrics to use to overwrite###########")
# print(report.to_string())
# pd_df_to_s3_csv(report, s3_bucket, os.path.join(s3_prefix,params['metrics']))
pd_df_to_s3_csv(report, s3_bucket, os.path.join(s3_prefix,fn_outmetrics))
print("Elapsed time for validating and saving metrics to s3: {}s".format(time.time() - checkpoint))

'''Prediction'''
print("Classifying test data")
checkpoint = time.time()
filtered_names = test_names.filter(test_names.usage == "test")
filtered_names.cache()
filtered_names.show()
secondelement = F.udf(lambda v: float(v[1]), FloatType())
print("Elapsed time for classifying test grids: {}s".format(time.time() - checkpoint))

# Redefine Image catalog
catalog_prefix_fix_classify = 'planet/planet_catalog_{}_fix.csv'.format(aoi)
# Redefine f_pool file
fn_f_pool_classify = 'f_pool_{}.csv'.format(aoi)
f_pool_classify = spark \
    .read \
    .option('inferScheme', True) \
    .option('header', True) \
    .csv('s3n://{}/{}/{}'.format(s3_bucket, s3_prefix, fn_f_pool_classify)) \
    .repartition('col', 'row')

aoi_name = aoi
complete_catalog = True
probability_images = 0
if probability_images > 0 or complete_catalog:
    logger.warn("Write catalog of {} probability images".format(probability_images))
    checkpoint = time.time()

    if complete_catalog:
        # new catalog
        image_catalog_fix = spark.read \
            .option('inferScheme', True) \
            .option('header', True) \
            .csv('s3n://{}/{}'.format(s3_bucket, catalog_prefix_fix_classify)) \
            .repartition('col', 'row')
        all_image_uris_fix = image_catalog_fix \
            .filter(image_catalog_fix['season'] == 'GS') \
            .alias('gs') \
            .join(image_catalog_fix.filter(image_catalog_fix['season'] == 'OS').alias('os'),
                  (col('gs.col') == col('os.col')) & (col('gs.row') == col('os.row'))) \
            .select(col('gs.col'), col('gs.row'), col('gs.uri').alias('GS'), col('os.uri').alias('OS'))

        #recollect all pixels for all testing images
        compreh_names = f_pool_classify.join(qs_in, ['name', 'col', 'row', 'name_col_row'], 'outer')
        features_compreh = gather_data(all_image_uris_fix,
                                        compreh_names,
                                        master_metadata,
                                        feature_names,
                                        s3_bucket)
        fitted_compreh = model.transform(features_compreh)\
             .select('spatial_key', 'column_index', 'row_index', 'probability', 'prediction')
        grouped_compreh = fitted_compreh.groupBy('spatial_key')
        # added to test sampling
        assembled = grouped_compreh.agg(
        assembleTile('column_index',
                     'row_index',
                     secondelement('probability'),
                     master_layout.tileLayout.tileCols,
                     master_layout.tileLayout.tileRows,
                     'float32'
        ).alias('probability')
        )
        layer = gps.TiledRasterLayer.from_rasterframe(assembled.asRF())
    else:
        print('Do nothing')

    coarse_layout = gps.LayoutDefinition(gps.Extent(-17.541, -35.46, 51.459, 37.54),
                                         gps.TileLayout(1380, 1460, 2000, 2000))
    # we multiply by 100 to select digits that will be kept after converting from float to int.
    # range of int8 is to 128, so we can only preserve 2 sig figs
    output_tiles = (layer * 100).convert_data_type(gps.CellType.INT8) \
        .tile_to_layout(coarse_layout) \
        .to_geotiff_rdd(storage_method=gps.StorageMethod.TILED)

    # change params['image_output_pattern']
    # cog_location = '/tmp/image_{}_{}.tif' if 'image_output_pattern' not in params else params['image_output_pattern']
    print('Generate output images')
    opt_path_pattern = 's3://activemapper/classified-images/aoi1_whole_022021/image_c{}_r{}_{}_run{}_iteration{}.tif'
    cog_location = '/tmp/image_{}_{}.tif' if 'image_output_pattern' not in params else opt_path_pattern
    output_tiles.foreach(lambda pair: write_bytes_to_s3(
        cog_location.format(pair[0].col, pair[0].row, aoi_name, run_id, str(last_iteration + 1)), pair[1]))
    print("Elapsed time for writing catalog of probability images: {}s".format(time.time() - checkpoint))




