# `learner`
This repository contains the machine learning component of the Mapping Africa active learning system. It is composed of two scripts, each of which is automatically run as an Elastic Map Reduce (EMR) step on an EMR cluster. This simply means that each script is run automatically after the cluster is done being created. Scripts used to create the cluster are contained within the terraform folder of the mapper repository (the terraform folder in this repo is a present for convinience and will eventually be removed).

**Note: this readme is in need of an update**

## Profiling `learner`
Profiling is an important part of understanding how quickly a program runs and what steps take the most time, as well as debugging errors. In AWS, this is done mainly with two online tools, Ganglia - for examining hardware utilization - and the Resource Manager - for examining logs and durations of Spark tasks (seperate steps of the `learner` program). Setting yourself up to examine the cluster as it runs involves three steps.

1. Setting up FoxyProxy - allowing your browser to access the 2 online tools
    - Get FoxyProxy Standard for Chrome. It is free: https://chrome.google.com/webstore/search/foxy%20proxy
    - Next import the .fpx file with parameters that allow you to connect to the instance (you can get this from Ryan)
    - then, in your browser, go to the FoxyProxy plugin icon in the upper righthand corner. 
    - click "Use proxy emr-socks-proxy" for all URLs
3. Logging into the EMR console and clicking Enable Web Connection, under the summary tab for the cluster of interest
4. Copying and running the ssh script to open an ssh connection to the cluster (you will need the pem key identified in that ssh line), then the links to Ganglia and Resource Manager should appear
5. Clicking on Ganglia will show you graphs describing hardware utilization for the whole cluster, and you can inspect graphs of individual nodes
6. Clicking the Resource Manager will take you to a table, look to the lower right of the table for "Tracking UI" and "ApplicationMaster", click "ApplicationMaster" and you will be taken to the Spark Tracking UI
* the ssh tunnel will keep running until you close it with CNTRL+C

## Guide to profiling `learner` (in the works)
The two important tabs of the Resource Manager's Tracking UI are Jobs and Stages. The Jobs tab has shows completed stages which is a rough way to gauge where the program is at across all executors. The steps below pertain to how to profile specific parts of the `learner` code across all executors. Can hopefully be used to show how long training time and testing time take seperately.

### Viewing Spark master logs
1. First navigate to the EMR console for your cluster
2. Then, navigate to the Steps tab
3. If the cluster has finished bootstrapping and has been running Spark jobs for a bit you should see a link to stderr next to the Spark step, click on it 
4. In that file, CNTRL+F for `WARN MAIN` - this will show you the output of logger messages, specicially the name of the step and below it, the elapsed time it took to complete that step
* if you terminate a cluster (either with the console or terraform), a `stderr.gz` will be generated after termination completes. CNTRL+F for "elapsed time" to get insight about how long different parts of the step take. 
* we can match up the elapsed times of the cluster steps with the durations in the Resource Manager to match the opaque Spark names with different parts of the step


## run_it/run_geopyspark.py
This script runs a single iteration of the model and deposits metrics and uncertain names on s3. It is run after the EMR step that copies over the `learner` scripts is complete. It parses the master config file on s3 as well as command line arguments that are supplied via the call of the EMR step. This EMR step is constructed in the mapper repo's terraform folder in the emr.tf file. Each of these (the EMR step and config file) should be checked and edited before a given model run. Command line options may be specified to output probability images, the whole probability map, to set the runid and descriptive AOI string associated with a given model run, and a random seed.

The following are descriptive note to supplement understanding of the functions in this file. Be sure to also read the docstrings and code. 

#### Much of the functionality is provided by three packages
1. **pyspark**
    - library for distributed data analysis. under the hood, rasterframes and geopyspark use the pyspark package. in some cases we need to use this package directly 
2. **geopyspark (GPS)**: 
    - helps lubricate the conversion between numpy arrays and Spark Dataframes (which come from the pyspark package). Spark Dataframes are distributed dataframes, meaning computations on these dataframes can be parallelized across processors on a single node or even across multiple nodes on a cluster.
    - we use it to read in windowed portions of Planet scenes. these windowed portions align with the supplied master grid
    - also used to create the layout of raster layers. A GPS layout contains information about the projection, image resolution, and grid resolution of a tiling system. **Currently this is hardcoded to match our master grid and should be edited to allow for different image inputs and to test how an adjustable grid for the same image input impacts model accuracy**
3. **rasterframes**: 
    - applies standard ML algorithms to raster data in the form of a rasterframe. model training and testing is distributed just like a pyspark dataframe is distributed
    - outputs accuracy metrics such as tss, tpr/tpr, auc, which are used to track model performance across runs and iterations
    - currently installed from geotrellis fork of the locationtech main repository used to develop the package. this is because ad hoc changes had to be made to the rasterframes package for this project in order to get on the fly focal operations (mean, slope filters) to work

## Notes, to be cleaned up into documentation
- `gps.TileLayout` defines number of grids to divide the extent into
- since we wanted 0.005 grid cell resolution we need to specify number of chunks for first two arguments (columns, rows) of `gps.TileLayout`
- pixels per image should match size of image masks, final two arguments for `gps.TileLayout`
- is Su spitting out masks with this master grid resolution?
- `gps.Metadata` every grid cell has key value, gps.SpatiaKey defines tange of key values
- **justin has updated s3 config file, needs to be updated config template mapper**
    - `image_catalog` deviates from scheme, includes prefix and filename, can be easily changed
    - `image_output_pattern` is the probability images path
    - column and row identifiers are not going to be the `master_layout`, larger cogs. Done to `coarse_layout which` can be changed. has same extant and resolution and tweaked for less chunks
- prob outputs are cogs, we just need to figure out how to ingest them into raster foundry
- click options updated, can now supply sampling of prob images
    - no way to generate complete catalog of probability images, justin will make a flag for this
- justin will add flag to output final prediction map
- justin will leave RF model storage to us
- can be saved after `model = pipeline.fit(...)`
- they forked pyrasterframes and we use it. they want to pull it into main line. no python documentation. will be in coming months
- they will tell us how to fix when changes pulled into main pyrasterframes repo
- **where do we get forked pyrasterframes?**
    - in `bootstrap.sh` from terraform it pulls from geotrellis/rasterframes not locationtech which is mainline version
    - `rasterframes_sha` in variables.tf is defined in variables.tf, specifies commit to use for rasterframes
    - can make pulls to geotrellis/rasterframes so that we can use it
- **in `build_feature_frame` frame.select builds features in build_feature_frame, value of 2 is hard coded to window size**
    - `localNbhd0p` discards border region
    - throws away 1/2 of the window size on the border
    - `localNbhd0p` could take `buffer*2+1` instead of hardcoded 5 for window size
    - operations documented in rasterframes code python docs coming in some months
    - to make 7x7 neighborhood operation, would need to read in buffer of 3 instead of 2 in read_buffered_window and then change localNbhdop to 7 from 5. both erodePixels would bump up from 2 to 3
    - recommendation for testing: look at tile dimensions in raster frames. if tile dims correct you got it correct
    - `remap` names columns correctly to feature type
- **features beyond map algebra like max and means may require adding to rasterframes**
- **how difficult to add another data source like SRTM?**
    - suggeston, start with gather_data
    - where all img sources get combined together
    - do additional join to join SRTM to the other features
    - also can use img reading functions, like read_buffered_window, cna work with the SRTM if it is at reasonably close resolution
    - may need to modify to deal with coarse SRTM vs high res Planet
- **COGs and NN resampling**
    - imgs must be stored as square chunks
    - imgs written in pieces called segments (square chunks)
    - COGs have optional multi-res pyramid, doesn't need to do work to show you img you are querying
    - overviews only encountered when img not in native resolution
    - we can see which resampling method looks better on rasterfoundry
    - cubic not advised
- **UDF is user defined spark function, needed to access probabilities, runs slower but doesn't matter too much for performance (justin was a bit unsure). used for firstelement instead of fitted.probability[0]**
- rasterframes tile exploder takes rasterframe with rows of tiles to dataframe of floats or integers of what is contained in tile
- rasterframe has column spatial key and tile key for rows
- spatial key row index tile index and value at those indices dataframe
- assemble is inverse of explode
- rasterframes makes easy to use rasters in ML
- GPS helps lubricate conversion from numpy array to Spark Dataframes
- perform layout of raster layers andd
- output final image catalog
- geopyspark requires you to build spark conf object, pyspark builds it automatically. it has requirements for spark session. so doe srasterframes. this is why we use spark submit isntead of pyspark submit
- some conf line in old not necessary, first 2 conf lines can go away
- if we change rasterframes, --packages needs to change from custom implementation. this may get changed to mainline version
- Justin will help us streamline this, will let us know if/when changes merged to mainline

