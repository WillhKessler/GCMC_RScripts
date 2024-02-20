########READ ME##############################################
## This script is intended to perform a parallel raster extraction of PRISM climate data to a csv of observation data using the SLURM job manager on the Channing compute cluster. 
## The required R version is 4.1.0
##
## Inputs required are:
## PRISM climate data, as daily BIL raster files, found here: /pc/nhair0a/Built_Environment/BE_Data/Geographic_Data/PRISM_daily/PRISM_data/an
## climate data assumes a naming convention of XXXXX_variable_YYYYMMDD.bil 
## CSV of cohorts observations. 
## The csv format assumes the following fields:  "y","x", "start_date", "stop_date"
## dates should be formatted as "YYYY-MM-DD"
##
## Update the contents of this code to point to the proper location for the directory for PRISM and the cohort CSV, and your output file name/location
## line 53
## line 54
## line 143, 144

## To run this code on the Channing cluster: 
## 1.	First login into the Unix system
## 2. 'be' nhair0a
## 3. set the proper temp directory for R. from the terminal use setenv TMPDIR "/d/tmp/nhairs/nhair0a/PopweightPRISM/tmp"
## Check that it is set with echo ${TMPDIR}
## 3. Submit a the job using SLURM via the command: sbR -v 4.1.0 ExtractPRISM_VITAL.R
########READ ME##############################################




##---- REQUIRED INPUTS ----##
PROJECT_NAME<-"Bellavia_polygon_LINKAGE" # string with a project name
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

#Create a temporary registry item
if(file.exists(paste(PROJECT_NAME,"Registry",sep="_"))){
  reg = loadRegistry(paste(PROJECT_NAME,"Registry",sep="_"),writeable = TRUE)
}else{
  reg = makeRegistry(file.dir = paste(PROJECT_NAME,"Registry",sep="_"), seed = 42)
  reg$cluster.functions=makeClusterFunctionsSocket()
}



##############################################################


##---- The function to perform the extraction
source("S:/GCMC/_Code/R/GCMC_Rscripts/Functions_RasterExtraction.R")
##############################################################
##---- Set up the batch processing jobs
##REQUIRED##
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


clearRegistry(reg)

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
done <- batchtools::submitJobs(jobs)
getStatus()

waitForJobs() # Wait until jobs are completed





