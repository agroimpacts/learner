# `learner`
This repository contains the machine learning component of the Mapping Africa active learning system. It is composed of two scripts, each of which is automatically run as an Elastic Map Reduce (EMR) step on an EMR cluster. This simply means that each script is run automatically after the cluster is done being created. Scripts used to create the cluster are contained within the terraform folder of the [labeller](https://github.com/agroimpacts/labeller) repository.

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

### Citation

Estes, L.D., Ye, S., Song, L., Luo, B., Eastman, J.R., Meng, Z., Zhang, Q., McRitchie, D., Debats, S.R., Muhando, J., Amukoa, A.H., Kaloo, B.W., Makuru, J., Mbatia, B.K., Muasa, I.M., Mucha, J., Mugami, A.M., Mugami, J.M., Muinde, F.W., Mwawaza, F.M., Ochieng, J., Oduol, C.J., Oduor, P., Wanjiku, T., Wanyoike, J.G., Avery, R. & Caylor, K. (2021) High resolution, annual maps of the characteristics of smallholder-dominated croplands at national scales. EarthArxiv https://doi.org/10.31223/X56C83

### Acknowledgements

The primary support for this work was provided by Omidyar Network’s Property Rights Initiative, now PLACE. Computing support was provided by the AWS Cloud Credits for Research program and the Amazon Sustainability Data Initiative. Azavea provided significant contributions in developing learner and its interactions with [labeller](https://github.com/agroimpacts/labeller).