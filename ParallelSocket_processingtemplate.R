##REQUIRED##
# Use this script as a template for running batch processing jobs on your local machine. 
# This utilizes one or more cores in a multicored machine.
# The required inputs are standardized for ALL functions defined in the Raster Extractions R source file 

##---- REQUIRED INPUTS ----##
PROJECT_NAME<-"GCMC2_ParallelTest" # string with a project name
#rasterdir<- "/pc/nhair0a/Built_Environment/BE_Data/Geographic_Data/PRISM_daily/PRISM_data/an" # string with a file path to raster covariates to extract- function will try to pull variable names from sub directories i.e /PRISM/ppt or /PRISM/tmean or /NDVI/30m
rasterdir<-"S:/GCMC/Data/Climate/PRISM/"
#extractionlayer = "/d/tmp/nhairs/nhair0a/BellaviaLinkage/sites_10M/sites_10M.shp" # string with path to spatial layer to use for extraction. Can be a CSV or SHP or GDB 
extractionlayer = "C:/Users/wik191/OneDrive - Harvard University/_Projects/Andrea_Bellavia/sites_10M.shp"
layername = "sites_10M" # Layer name used when extraction layer is an SHP or GDB
IDfield<-"ORIG_FID" # Field in extraction layer specifying IDs for features, can be unique or not, used to chunk up batch jobs
Xfield<- "X"
Yfield<- "Y"
startdatefield = "start_date" # Field in extraction layer specifying first date of observations
enddatefield = "end_date" # Field in extraction layer specifying last date of observations
predays = 0 # Integer specifying how many days preceding 'startingdatefield' to extract data. i.e. 365 will mean data extraction will begin 1 year before startdatefield
weights = NA # string specifying file path to raster weights, should only be used when extraction layer is a polygon layer



#### R batchtools
library(batchtools)
require(terra)
require(tools)


##REQUIRED##
##REQUIRED##
#Initialize conf files and template
#Initialize batchtools

#Create a temporary registry item
if(file.exists(paste(PROJECT_NAME,"Registry",sep="_"))){
  reg = loadRegistry(paste(PROJECT_NAME,"Registry",sep="_"),writeable = TRUE)
  reg$cluster.functions=makeClusterFunctionsSocket()
}else{
  reg = makeRegistry(file.dir = paste(PROJECT_NAME,"Registry",sep="_"), seed = 42)
  reg$cluster.functions=makeClusterFunctionsSocket()
}


##########Input PROCESSING HERE####################################################
## Call Desired functions from Functions_RasterExtraction source file
## Call the desired function in batchMap
source("S:/GCMC/_Code/R/GCMC_Rscripts/Functions_RasterExtraction.R")


##---- Set up the batch processing jobs
# grid should contain columns for all desired variable combinations
par_grid <- expand.grid(mu = -5:5, sigma = seq(3, 33, 10), nrep = 1:100)

# Clear the R registry
clearRegistry(reg)

#Create jobs
jobs <- batchMap(fun = examplefunction,args = par_grid)
jobs$chunk <- chunk(jobs$job.id, chunk.size = 1000)


getJobTable()
getStatus()

# Submit Jobs
batchtools::submitJobs()
waitForJobs()