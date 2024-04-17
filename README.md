# READ ME:
The principal code provided in this repository is a workflow for using the 'terra' R package to extract raster data to points or polygons based on specific time intervals. It was originally written to assist in linking public health cohort data to environmental exposure data in the form of daily or monthly raster data. A user supplies a CSV, SHAPEFILE, or GDB feature class containing point or polygon features with start and end observation dates, and a set of raster layers with the file name ending with an observation date. The workflow identifies which rasters fall within a given input feature's observation period and extracts the raster values corresponding to the feature's location. For Polygon features, a weighting raster can be supplied to generate a weighted average within the feature, e.g. a population-weighted average temperature exposure within a census tract. Care should be taken to ensure that the input features and raster data are in the same coordinate reference system as those transformations are not handled here. 
This workflow is implemented with `batchtools` in R to run in parallel on a compute cluster using the SLURM job manager, or in parallel or serial locally. 

This github repository contains up to 5 files that are necessary for the workflow, depending on your implementation:
1. ParallelXXXXX_processingtemplate.R
3. Functions_RasterExtraction.R
4. slurm.tmpl
5. batchtools.conf.R
6. Recombineoutputs.R

In addition, a user will need to supply the following:
1. A directory of one or more child directories containing the rasters to be extracted. The child directory names will be used to differentiate variables timeseries rasters. For example, the following tree would allow for the extraction of the variables NO2, Ozone, and PM_2.5:
```
└── ~\AirPollution/
    ├── Methane
    ├── NO2/
    │   └── Daily/
    │       ├── 2001/
    │       │   ├── NO2_Daily_20010101.tif
    │       │   ├── NO2_Daily_20010102.tif
    │       │   └── NO2_Daily_20010103.tif
    │       └── 2002/
    │           ├── NO2_Daily_20020101.tif
    │           ├── NO2_Daily_20020102.tif
    │           └── NO2_Daily_20020103.tif
    ├── Ozone/
    │   └── Daily/
    │       ├── 2001/
    │       │   ├── NO2_Daily_20010101.tif
    │       │   ├── NO2_Daily_20010102.tif
    │       │   └── NO2_Daily_20010103.tif
    │       └── 2002/
    │           ├── NO2_Daily_20020101.tif
    │           ├── NO2_Daily_20020102.tif
    │           └── NO2_Daily_20020103.tif
    └── PM_25/
        └── Daily/
            ├── 2001/
            │   ├── NO2_Daily_20010101.tif
            │   ├── NO2_Daily_20010102.tif
            │   └── NO2_Daily_20010103.tif
            └── 2002/
                ├── NO2_Daily_20020101.tif
                ├── NO2_Daily_20020102.tif
                └── NO2_Daily_20020103.tif
```
  
2. A file containing the features to which the rasters will be extracted. This can be a CSV, shapefile, or GDB feature class. The file should contain at a minimum the following fields:
   ```
   IDfield<-A Field in extraction layer specifying IDs for features
   startdatefield<- A field name in extraction layer specifying first date of observations
   enddatefield<- A field name in extraction layer specifying last date of observations

   For CSV's, also include
   Xfield<- A Field containing the X coordinate (Longitude), in decimal degrees
   Yfield<- A Field containing the Y coordinate (Latitude), in decimal degrees
   ```
4. If desired, A directory containing one or more weights raster layers. For example to weight by population over age of 65, these weights will be summed and scaled to sum to 1 within each feature. 
   ```
   └── ~\Population/
       ├── US_M_population_over65.tif
       └── US_F_population_over65.tif
   ```
The ParallelXXXXX_processingtemplate.R contains all organizational information required for `batchtools` to set up and execute your processing jobs; they are fairly standard implementations of `batchtools` workflows with additional user inputs specific to the raster extraction procedure outlined here. Next, you will need an R file containing one or more functions you wish to run. These functions can be placed directly in the ParallelXXXXX_processingtemplate.R or sourced from an external file. In this workflow, the required raster extraction functions are sourced from an external file called `Functions_RasterExtraction.R`. Thirdly, depending on whether you are implementing the workflow on a computing cluster with the SLURM job manager, or locally with either multisocket or interactive workflows. You may also need an R configuration file, and a `brew` `slurm.tmpl` template.   
# The Workflow:
The following example explains each file used in the workflow implemented on a computing cluster with the SLURM job manager. The process is nearly identical for submitting to a local multi-socket cluster or if run in series on your local machine. 
1. Log into whatever machine you will be using for your computing.
2. Get your input features and raster data organized as necessary. 
3. Open a terminal and download the appropriate Parallel Processing Template from this repository
   
    ```
    wget xxxxxxxxx
    ```
4. Open your favorite unix text editor or open the Parallel Processing Template in RStudio and update the required inputs, adjust the resources in batchtools::submitJobs() as necessary
5. Run the R script ParallelXXXX_processingtemplate.R as normal
```
sbR -v 4.3.0 -o "12hour" ParallelXXXX_processingtemplate.R

```
Or hit 'run' in your RStudio session if running locally
6.  
## 1. Download the Parallel Processing Template, update the necessary inputs
There are 3 versions of this template depending on your implementation. 
1. ParallelSLURM_processingtemplate.R- for implementing on a compute cluster with the SLURM job scheduler
2. ParallelSocket_processingtemplate.R- For multicored local machine implementations
3. Parallel_processingtemplate.R- For singlecore/serial jobs on a local machine
The Parallel Processing Template is located here: 
Download in Unix with ```wget xxxxxxx```

The following is an example of how to update the required inputs. To use the raster extraction processes outlined here, the following inputs are required. 
```
##---- REQUIRED INPUTS ----##
PROJECT_NAME<-"Polygon_PRISM_LINKAGE" # string with a project name
rasterdir<- "~/PRISM_data/an" # string with a file path to raster covariates to extract- function will try to pull variable names from first level subdirectories i.e /PRISM/an/ppt or /PRISM/an/tmean
extractionlayer = "~/sites_10M.shp" # string with path to spatial layer to use for extraction. Can be a CSV or SHP or GDB 
layername = "sites_10M" # Layer name used when extraction layer is an SHP or GDB
IDfield<-"ORIG_FID" # Field in extraction layer specifying IDs for features, can be unique or not, used to chunk up batch jobs
Xfield<- "X" #A Field containing the X coordinate (Longitude), in decimal degrees, only for CSV
Yfield<- "Y" # A Field containing the Y coordinate (Latitude), in decimal degrees, only for CSV
startdatefield = "start_date" # Field name in extraction layer specifying first date of observations
enddatefield = "end_date" # Field name in extraction layer specifying last date of observations
predays = 0 # Integer specifying how many days preceding 'startingdatefield' to extract data. i.e. 365 will mean data extraction will begin 1 year before startdatefield
weights = NA # string specifying file path to raster weights, should only be used when extraction layer is a polygon layer
```

Next, the script will load the required packages which include `batchtools`, `terra`, and `tools`, and download the slurm.tmpl file and batchtools.conf.R file. Both these files are located in this repository and can be downloaded manually if desired. Otherwise it's handled by the script. At this stage a new registry directory is also created which will hold all the necessary outputs and files.
You shouldn't need to modify any of this information.
```
##---- Required Packages
library(batchtools)
require(terra)
require(tools)

##REQUIRED##
##---- Initialize batchtools configuration files and template
if(!file.exists("slurm.tmpl")){
  download.file("https://raw.githubusercontent.com/WillhKessler/GCMC_RScripts/main/slurm.tmpl","slurm.tmpl")
}else{
  print("template exists")
}
if(!file.exists("batchtools.conf.R")){
  download.file("https://raw.githubusercontent.com/WillhKessler/GCMC_RScripts/main/batchtools.conf.R", "batchtools.conf.R")
}else{
  print("conf file exists")
}

#Create a temporary registry item
if(file.exists(paste(PROJECT_NAME,"Registry",sep="_"))){
  reg = loadRegistry(paste(PROJECT_NAME,"Registry",sep="_"),writeable = TRUE,conf.file="batchtools.conf.R")
}else{
  reg = makeRegistry(file.dir = paste(PROJECT_NAME,"Registry",sep="_"), seed = 42,conf.file="batchtools.conf.R")
}
``` 
The next section contains any functions necessary for your processing job. Here, the desired functions are sourced from the file `Functions_RasterExtraction.R` which is contained in this repository. For this workflow, you do not need to modify anything in this section. The sourced function is called `extract.rast` as shown in the next section. 
```
##########Input PROCESSING HERE####################################################
## Call Desired functions from Functions_RasterExtraction source file
## The desired functions are mapped in creating the jobs via batchMap
source("https://raw.githubusercontent.com/WillhKessler/GCMC_Rscripts/main/Functions_RasterExtraction.R")
```
The final section is where the processing jobs are constructed and submitted to the cluster, or your local machine for processing. First, the user inputs and processing function(s) are inserted into a matrix of all possible combinations using the `batchgrid()` function. This function is specific to this workflow and is essentially a wrapper for `expand.grid()` This constitutes all the jobs that will be run. The jobs are then chunked and finally submitted using the specified resources. Changing the specified resources will be necessary based on the conventions of your compute cluster. In this example, there is a partition or queue called `linux01` where the jobs will be submitted with each requested machine having 1 CPU, for 1 task with 80GB of RAM. The jobs will run up to the walltime specified.
All user inputs must be explicitly specified to ensure they are passed on to child processes on the cluster. 
```
##############################################################
##---- Set up the batch processing jobs
##---- Use the 'batchgrid' function to create a grid of variable combinations to process over. function considers input rasters, input features, and any weighting layers

batchgrid = function(rasterdir,extractionlayer,layername,IDfield,Xfield,Yfield,startdatefield,enddatefield,predays,weightslayers){
  require("tools")
  
  ##---- Set up the batch processing jobs
  pvars = list.dirs(path = rasterdir,full.names = FALSE,recursive = FALSE)
  
  if(file_ext(extractionlayer)=="csv"){
    feature<-unique(read.csv(extractionlayer)[,IDfield])
    layername = NA
    weightslayers = NA
  }else if(file_ext(extractionlayer) %in% c("shp","gdb")){
    require('terra')
    vectorfile<- vect(x=extractionlayer,layer=layername)
    feature<- unlist(unique(values(vectorfile[,IDfield])))
    Xfield = NA
    Yfield = NA
  }
  
  output<- expand.grid(vars = pvars,
                     piece = feature,
                     rasterdir = rasterdir,
                     extractionlayer = extractionlayer,
                     layername = layername,
                     IDfield = IDfield,
                     Xfield = Xfield,
                     Yfield = Yfield,
                     startdatefield = startdatefield,
                     enddatefield = enddatefield,
                     predays = predays,
                     weightslayers = weightslayers,
                     stringsAsFactors = FALSE)
  return(output)
}

##----  Make sure registry is empty
clearRegistry(reg)

##----  create jobs from variable grid
jobs<- batchMap(fun = extract.rast,
                batchgrid(rasterdir = rasterdir,
                          extractionlayer = extractionlayer,
                          layername = layername,
                          IDfield = IDfield, 
                          Xfield = Xfield,
                          Yfield = Yfield,
                          startdatefield = startdatefield,
                          enddatefield = enddatefield,
                          predays = predays,
                          weightslayers = weights),
                reg = reg)
jobs$chunk<-chunk(jobs$job.id,chunk.size = 90)
setJobNames(jobs,paste(abbreviate(PROJECT_NAME),jobs$job.id,sep=""),reg=reg)

getJobTable()
getStatus()

##---- Submit jobs to scheduler
done <- batchtools::submitJobs(jobs, 
                               reg=reg, 
                               resources=list(partition="linux01", walltime=3600000, ntasks=1, ncpus=1, memory=80000))
getStatus()

waitForJobs() # Wait until jobs are completed
```
`waitForJobs()` will often return FALSE when using SLURM as the jobs can sometimes get lost by the manager. But don't worry, they're still running. However, due to this quirk, we don't rely on this script for subsequent processing. 

## Functions_RasterExtraction.R
This file is the source for the raster extraction procedure implemented in this workflow. This file isn't downloaded directly, but `ParallelXXXXX_processingtemplate.R` sources functions from this file. 

## slurm.tmpl
This is a brew template which is a bash script telling the slurm job scheduler how to allocate resources. Double `##` are comments and not interpreted by the bash script. Anything denoted by a single `#SBATCH` is interpreted by SLURM and passed to the unix system in the bash script.  
Notice here we are loading R module 4.3.0 which on my system contains all the necessary packages
Further note that all the information passed to SLURM with `#SBATCH` should be specified in the ParallelXXXXXX_processingtemplate.R

```
#!/bin/bash -l

## Job Resource Interface Definition
##
## ntasks [integer(1)]:       Number of required tasks,
##                            Set larger than 1 if you want to further parallelize
##                            with MPI within your job.
## ncpus [integer(1)]:        Number of required cpus per task,
##                            Set larger than 1 if you want to further parallelize
##                            with multicore/parallel within each task.
## walltime [integer(1)]:     Walltime for this job, in seconds.
##                            Must be at least 60 seconds.
## memory   [integer(1)]:     Memory in megabytes for each cpu.
##                            Must be at least 100 (when I tried lower values my
##                            jobs did not start at all).
##
## Default resources can be set in your .batchtools.conf.R by defining the variable
## 'default.resources' as a named list.

<%
# relative paths are not handled well by Slurm
log.file = fs::path_expand(log.file)
-%>


#SBATCH --job-name=<%= job.name %>
#SBATCH --output=<%= log.file %>
#SBATCH --error=<%= log.file %>
#SBATCH --time=<%= ceiling(resources$walltime / 60) %>
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=<%= resources$ncpus %>
#SBATCH --mem-per-cpu=<%= resources$memory %>
#SBATCH --partition=<%= resources$partition %>
#SBATCH --exclude=chandl03.bwh.harvard.edu,chandl02.bwh.harvard.edu,chanslurm-compute-test

<%= if (!is.null(resources$partition)) sprintf(paste0("#SBATCH --partition='", resources$partition, "'")) %>
<%= if (array.jobs) sprintf("#SBATCH --array=1-%i", nrow(jobs)) else "" %>

## Initialize work environment like
## source /etc/profile
## module add ...
## module load R/4.0.1
module load R/4.3.0

## Export value of DEBUGME environemnt var to slave
export DEBUGME=<%= Sys.getenv("DEBUGME") %>

<%= sprintf("export OMP_NUM_THREADS=%i", resources$omp.threads) -%>
<%= sprintf("export OPENBLAS_NUM_THREADS=%i", resources$blas.threads) -%>
<%= sprintf("export MKL_NUM_THREADS=%i", resources$blas.threads) -%>

## Run R:
## we merge R output with stdout from SLURM, which gets then logged via --output option
Rscript -e 'batchtools::doJobCollection("<%= uri %>")'

```

## batchtools.conf.R
This is a configuration file for R that allows you to pass specific information to R. This file is only required for this workflow when using the SLURM implementation. It tells R the `cluster.function` to be used is the SLURM function, and that the template is provided as `slurm.tmpl`
```
cluster.functions = makeClusterFunctionsSlurm(template="slurm.tmpl")
```
