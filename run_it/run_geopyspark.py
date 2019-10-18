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

# sample invocation:
#
# spark-submit \
#    --master yarn \
#    --packages io.astraea:pyrasterframes:0.7.3-GT2,org.apache.hadoop:hadoop-aws:2.7.3,org.apache.logging.log4j:log4j-core:2.11.1 \
#    --jars /opt/jars/geotrellis-backend-assembly-0.4.2.jar \
#    --conf spark.executorEnv.GDAL_DATA=/usr/local/lib64/python3.4/site-packages/rasterio/gdal_data \
#    /tmp/run_geopyspark.py --probability-images 10 --random-seed 42 activemapper run_id aoi_id

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
    return gps.Extent(ex.xmin - cx * px_buffer, ex.ymin - cy * px_buffer, ex.xmax + cx * px_buffer, ex.ymax + cy * px_buffer)

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

    tile = np.zeros((4, layout.tileLayout.tileCols + 2 * px_buffer, layout.tileLayout.tileRows + 2 * px_buffer))

    with rstr.open(uri) as src:
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
                          transform=Affine(cw,  0.0, ex.xmin,
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
                boto3.resource('s3').Bucket(uri.netloc).upload_fileobj(memfile, uri.path.format(location.col, location.row)[1:])
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
    uri = "s3://{}/{}/{}_{}_{}.tif".format(bucket, prefix, record.name, record.col, record.row)
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
                                               metadata)\
                               .to_rasterframe(1)\
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

    image_rdd = uri_frame.rdd\
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
                          [erodePixels(localNbhdOp(c, 5, 'stddev'),3).alias(remap(c, 'std')) for c in columns])

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
    return build_feature_frame(uris, 'GS', metadata)\
        .alias('gs')\
        .join(build_feature_frame(uris, 'OS', metadata).alias('os'),
              (col('gs.spatial_key.col') == col('os.spatial_key.col')) &
              (col('gs.spatial_key.row') == col('os.spatial_key.row')))\
        .select(['gs.spatial_key'] + feature_names)

def gather_data(all_uris, names, metadata, feature_names, s3_bucket, include_masks=False, validate = False):
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
    uris = all_uris\
           .join(names.alias('sites'), (col('sites.col') == col('gs.col')) & (col('sites.row') == col('gs.row')))\
           .select(col('sites.name').alias('name'), 'gs.col', 'gs.row', 'GS', 'OS')
    features = features_from_uri_frame(uris, metadata, feature_names)

    if not include_masks:
        return features.select('spatial_key', explodeTiles(*feature_names)).repartition('column_index', 'row_index')

    if validate:
        masks = get_masks_from_incoming_names(names, s3_bucket, 'labels', metadata)
    else:
        masks = get_masks_from_incoming_names(names, s3_bucket, 'labels/label_low', metadata)

    return features.join(masks.alias('masks'),
                         (col('masks.spatial_key.col') == features.spatial_key.col) &
                         (col('masks.spatial_key.row') == features.spatial_key.row))\
                   .select(['masks.spatial_key', 'masks.mask'] + feature_names)\
                   .select('spatial_key', explodeTiles(*(['mask'] + feature_names)))\
                   .repartition('column_index', 'row_index')

def ml_pipeline(feature_names, label_column, output_column='features'):
    """
    Produce the requisite Pipeline object for the machine learning process
    """
    ndFilter = NoDataFilter()
    ndFilter.setInputCols(feature_names + [label_column])

    assembler = VectorAssembler(inputCols=feature_names, outputCol=output_column)

    classifier = RandomForestClassifier(labelCol=label_column, featuresCol=assembler.getOutputCol())\
                 .setSubsamplingRate(0.1)\
                 .setMaxDepth(15)\
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
    fractions = {1: float(n_samples)/counts[1] * 0.5, 0: float(n_samples)/counts[0] * 0.5}
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

def execute(spark, logger, s3_bucket, run_id, aoi_name, complete_catalog, probability_images, seed, config_filename):
    """The primary script

    Args:
        spark (``pyspark.sql.SparkSession``)
        logger (``py4j.JavaObject``)
        s3_bucket (str): Name of the S3 bucket to search for configuration objects
            and save results to
        run_id (str): The identifier of the current run
        aoi_id (str): The identifier for the current area of interest
        probability_images (int): The number of tiles to save the generated
            probability images for
        seed (int): A random seed used to sample the probability images, for
            reproducability

    Required external inputs:
        <s3_bucket>/cvmapper_config.yaml
            under ``learner`` key:
                    prefix: The S3 prefix under which CSVs can be read and written
                    pool: Name of CSV file under s3_bucket/prefix giving the
                        comprehensive list of active grid cells
                    incoming_names: Name of CSV file under s3_bucket/prefix giving
                        list of cells used for training/validation
                    image_catalog: Name of CSV file under s3_bucket giving catalog
                        of imagery
                    image_output_pattern: URI pattern used for output of probability
                        images.  Must contain two '{}' tokens to be replaced by the
                        column and row for the relevant cell
                    outgoing: S3 URI to save the CSV of worst-performing cells to

        location pool:
            A CSV of ``name``, ``col``, ``row`` for each grid cell under
            consideration.  Identified by ``pool`` parameter above.

        incoming names:
            CSV containing (at least) ``name``, ``iteration``, and ``usage``
            columns.  Every name in this file must also be contained in the image
            pool.  Location of this file given in YAML file.

        image catalog:
            A CSV minimally containing ``col``, ``row``, ``season``, and ``uri``
            columns.  Season is either 'GS' or 'OS'.  Every grid cell in the
            location pool must be contained here, and must have an entry for both
            seasons.  URI points to TIFF that completely covers listed cell with
            valid image data (no NODATA values).

    Note:

        Grid cells are defined according to the master_layout object, which
        specifies a rectangular extent in long/lat coords.  This extent is
        subdivided into cells (in this case, 13792 columns and 14477 rows).
        Each cell is then given a pixel resolution (in this case 200x200, but
        whatever is chosen must match the resolution of the label images
        provided in the ``s3://<s3_bucket>/<prefix>/<name>_<col>_<row>.tif``
        files identified by the incoming names CSV).  When we refer to tiles,
        we mean image chips of the stated resolution, indexed by
        ``gps.SpatialKey`` objects.  The key is a col/row pair where row=0,
        col=0 corresponds to the chip in the upper left corner of the bounding
        extent.

    Note:

        Grid cell names for the output probability images
        (`image_output_pattern`) are relative to a different, coarser layout.
        These grid cell ids need not be clearly defined, since the output of
        this process is simply a bucket of COGs for display using another
        tool.  However, see the `coarse_layout` definition below for specific
        details of the layout.

    """
    params = parse_yaml_from_s3(s3_bucket, config_filename)['learner']

    s3_prefix = params['prefix']
    s3_prefix = s3_prefix[0:-1] if s3_prefix.endswith('/') else s3_prefix

    catalog_prefix = params['image_catalog'].format(aoi_name)

    feature_names = functools.reduce(lambda a, b: a + b, [["{}_raw_{}".format(season, n),
                                                           "{}_avg_{}".format(season, n),
                                                           "{}_std_{}".format(season, n)]
                                                          for season in ["GS", "OS"] for n in range(1, 5)])

    master_layout = gps.LayoutDefinition(gps.Extent(-17.541, -35.46, 51.459, 37.54),
                                         gps.TileLayout(13800, 14600, 200, 200))
    master_metadata = gps.Metadata(gps.Bounds(gps.SpatialKey(0, 0), gps.SpatialKey(13800, 14600)),
                                   "+proj=longlat +datum=WGS84 +no_defs ",
                                   gps.CellType.INT8,
                                   master_layout.extent,
                                   master_layout)

    ####################################
    logger.warn("Reading source tables")

    checkpoint = time.time()
    f_pool = spark\
         .read\
         .option('inferScheme', True)\
         .option('header', True)\
         .csv('s3n://{}/{}/{}'.format(s3_bucket, s3_prefix, params['pool'].format(aoi_name)))\
         .repartition('col', 'row')

    pool_ref = spark\
         .read\
         .option('inferScheme', True)\
         .option('header', True)\
         .csv('s3n://{}/{}/{}'.format(s3_bucket, s3_prefix, params['pool_ref'].format(aoi_name)))\
         .repartition('col', 'row')

    qs_in = spark \
        .read \
        .option('inferScheme', True) \
        .option('header', True) \
        .csv('s3n://{}/{}/{}'.format(s3_bucket, s3_prefix, params['qs'].format(aoi_name))) \
        .repartition('col', 'row')

    incoming = spark.read\
                    .option('header', True)\
                    .schema(StructType([
                        StructField('name', StringType()),
                        StructField('run', IntegerType()),
                        StructField('iteration', IntegerType()),
                        StructField('processed', BooleanType()),
                        StructField('usage', StringType()),
                        StructField('label', StringType())
                    ]))\
                    .csv('s3n://{}/{}/{}'.format(s3_bucket, s3_prefix, params['incoming_names'].format(aoi_name)))


    # merge incoming_names and incoming_names_static
    incoming = incoming.union(spark.read \
        .option('header', True) \
        .schema(StructType([
        StructField('name', StringType()),
        StructField('run', IntegerType()),
        StructField('iteration', IntegerType()),
        StructField('processed', BooleanType()),
        StructField('usage', StringType()),
        StructField('label', StringType())
    ])) \
        .csv('s3n://{}/{}/{}'.format(s3_bucket, s3_prefix, params['incoming_names_static'])))

    incoming = incoming.filter(incoming['run']==params['runid']).filter(incoming['label']==True)
    test_names = pool_ref.join(incoming.select('name'), 'name', 'left_anti').withColumn("usage",lit("test"))


    all_names = f_pool.join(incoming.select('name', 'usage'),
                            f_pool.name == incoming.name,
                            how='left')\
                      .select(f_pool.name.alias('name'), 'col', 'row', 'usage')
    num_test_images = test_names.count()
    print("number of test_names: {}".format(num_test_images))

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
    logger.warn("Elapsed time for reading source tables: {}s".format(time.time() - checkpoint))
    ####################################
    logger.warn("Reading training labels & building training features")

    checkpoint = time.time()
    training_data = gather_data(all_image_uris,
                                all_names.filter(all_names.usage == 'train'),
                                master_metadata,
                                feature_names,
                                s3_bucket,
                                include_masks=True)
    training_data.show()
    logger.warn("Elapsed time for reading training labels and feature building: {}s".format(time.time() - checkpoint))

    ####################################
    logger.warn("Balancing data")

    checkpoint = time.time()
    balanced_data = balance_samples(spark, training_data, 'mask')
    balanced_data.show()
    logger.warn("Elapsed time for balancing data: {}s".format(time.time() - checkpoint))

    ####################################
    logger.warn("Training model")

    checkpoint = time.time()
    pipeline = ml_pipeline(feature_names, 'mask')
    model = pipeline.fit(balanced_data)
    print(model)
    logger.warn("Elapsed time for training the model: {}s".format(time.time() - checkpoint))

    ####################################
    logger.warn("Validating model results")

    checkpoint = time.time()
    validation_data = gather_data(all_image_uris,
                                  all_names.filter(all_names.usage == 'validate'),
                                  master_metadata,
                                  feature_names,
                                  s3_bucket,
                                  include_masks=True,
                                  validate = True)

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
        'aoi':            [aoi_name],
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
    pd_df_to_s3_csv(report, s3_bucket, os.path.join(s3_prefix,params['metrics'].format(aoi_name)))
    logger.warn("Elapsed time for validating and saving metrics to s3: {}s".format(time.time() - checkpoint))

    ####################################
    # logger.warn("Classifying test data and produce maps")

    # checkpoint = time.time()
    # filtered_names = test_names.filter(test_names.usage == "test")
    # # filtered_names.cache()
    # # filtered_names.show()
    # test_features = gather_data(all_image_uris,
    #                             filtered_names,
    #                             master_metadata,
    #                             feature_names,
    #                             s3_bucket)
    #
    # test_features_sample = test_features.sample(True, 0.1)
    #
    # fitted = model.transform(test_features_sample).select('spatial_key', 'column_index', 'row_index', 'probability', 'prediction')
    # # fitted.cache()
    # # fitted.show()
    # grouped = fitted.groupBy('spatial_key')
    #
    # # don't want to use following UDF, but indication is that there is a bug in pyspark preventing vector accesses:
    # # https://stackoverflow.com/questions/44425159/access-element-of-a-vector-in-a-spark-dataframe-logistic-regression-probability
    # # (This did not work without the UDF!)
    firstelement = F.udf(lambda v: float(v[0]), FloatType())
    # added this UDF to select the probability of field rather than no field to write to probability images
    secondelement = F.udf(lambda v: float(v[1]), FloatType())
    # certainty = grouped\
    #         .agg(F.avg(F.pow(firstelement(fitted.probability) - lit(0.5), 2.0)).alias('certainty')).cache()
    # certainty.show()
    # logger.warn("Elapsed time for classifying test grids: {}s".format(time.time() - checkpoint))

    ####################################
    if probability_images > 0 or complete_catalog:
        logger.warn("Write catalog of {} probability images".format(probability_images))
        checkpoint = time.time()
        

        if complete_catalog:
            #recollect all pixels for all testing images
            compreh_names = pool_ref.join(qs_in, ['name', 'col', 'row', 'name_col_row'], 'outer')
            features_compreh = gather_data(all_image_uris,
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
            # sampling testing images (num = probability_images)
            filtered_names_sample = filtered_names\
                .sample(False, min(1.0, float(probability_images) / float(num_test_images)), seed=seed)\
                .join(image_catalog.filter(image_catalog['season'] == 'GS'), ['col', 'row'])\
                .select('scene_id')\
                .dropDuplicates()\
                .join(image_catalog.filter(image_catalog['season'] == 'GS'), 'scene_id')\
                .join(pool_ref.union(qs_in), ['col','row'])\
                .select('name', 'col', 'row', 'name_col_row')

            #re-collect all pixels within sampled images
            features_images = gather_data(all_image_uris,
                            filtered_names_sample,
                            master_metadata,
                            feature_names,
                            s3_bucket)
            #reclassify sampled testing images
            fitted_images = model.transform(features_images)\
                    .select('spatial_key', 'column_index', 'row_index', 'probability', 'prediction')
            grouped_sample = fitted_images.join(filtered_names_sample,(col('spatial_key.col') == col('col')) & (col('spatial_key.row') == col('row'))).groupby('spatial_key')
            assembled = grouped_sample.agg(
                assembleTile('column_index',
                         'row_index',
                         secondelement('probability'),
                         master_layout.tileLayout.tileCols,
                         master_layout.tileLayout.tileRows,
                         'float32'
                ).alias('probability')
            )
            layer = gps.TiledRasterLayer.from_rasterframe(assembled.asRF())

        coarse_layout = gps.LayoutDefinition(gps.Extent(-17.541, -35.46, 51.459, 37.54), gps.TileLayout(1380, 1460, 2000, 2000))
        # we multiply by 100 to select digits that will be kept after converting from float to int. 
        # range of int8 is to 128, so we can only preserve 2 sig figs
        output_tiles = (layer*100).convert_data_type(gps.CellType.INT8)\
                            .tile_to_layout(coarse_layout)\
                            .to_geotiff_rdd(storage_method=gps.StorageMethod.TILED)

        cog_location = '/tmp/image_{}_{}.tif' if 'image_output_pattern' not in params else params['image_output_pattern']
        output_tiles.foreach(lambda pair: write_bytes_to_s3(cog_location.format(aoi_name, pair[0].col, pair[0].row, aoi_name, run_id, str(last_iteration+1)), pair[1]))
        logger.warn("Elapsed time for writing catalog of probability images: {}s".format(time.time() - checkpoint))

    ####################################
    # logger.warn("Identify worst performing cells")
    # checkpoint = time.time()
    # # TODO: Determine which images to take
    # worst_keys_rdd = certainty\
    #          .sort('certainty')\
    #          .select('spatial_key')\
    #          .limit(round(certainty.count()*0.05))\
    #          .rdd.takeSample(False, (params['number_outgoing_names']))
    # worst_keys = spark.createDataFrame(worst_keys_rdd)
    # outgoing_names = worst_keys\
    #                  .join(pool_ref, (col('spatial_key.col') == col('col')) & (col('spatial_key.row') == col('row')))\
    #                  .select('name')\
    #                  .withColumn('run', lit(run_id))\
    #                  .withColumn('iteration', lit(last_iteration + 1))\
    #                  .withColumn('processed', lit(False))\
    #                  .withColumn('usage', lit('train'))\
    #                  .toPandas()
    # uri = urlparse.urlparse(params['outgoing'].format(aoi_name))
    # pd_df_to_s3_csv(outgoing_names, uri.netloc, uri.path[1:])
    # logger.warn("Elapsed time for sorting certainty, converting to Pandas Dataframe, and saving to s3: {}s".format(time.time() - checkpoint))

@click.command()
@click.option('--config-filename', default='cvmapper_config.yaml', help='The name of the config to use.')
@click.option('--probability-images', default=0, help='Write this many probability images to location specified in YAML file')
@click.option('--random-seed', default=None, type=int, help='Use this seed for sampling probability images')
@click.option('--output-all-images', is_flag=True, help='Produce the entire catalog of probability images')
@click.argument('bucket')
@click.argument('run')
@click.argument('aoi')
def main(config_filename, probability_images, random_seed, output_all_images, bucket, run, aoi):
    start_time = time.time()
    conf = gps\
           .geopyspark_conf(appName="Learner Model Iteration", master="yarn") \
           .set("spark.dynamicAllocation.enabled", True) \
           .set("spark.ui.enabled", True) \
           .set("spark.hadoop.yarn.timeline-service.enabled", False)
    spark = SparkSession\
            .builder\
            .config(conf=conf)\
            .getOrCreate()\
            .withRasterFrames()
    logger = spark._jvm.org.apache.log4j.Logger.getLogger("Main")

    execute(spark, logger, bucket, run, aoi, output_all_images, probability_images, random_seed, config_filename)
    logger.warn("Total run time: {}s".format(time.time() - start_time))

    spark.stop()

if __name__ == "__main__":
    main()
