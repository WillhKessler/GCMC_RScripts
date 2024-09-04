##---- REQUIRED INPUTS ----##
PROJECT_NAME<-"innerParallelTest" # string with a project name
rasterdir<-  "/pc/nhair0a/Built_Environment/BE_Data/Geographic_Data/PRISM_daily/PRISM_data/an" # string with a file path to raster covariates to extract- function will try to pull variable names from sub directories i.e /PRISM/ppt or /PRISM/tmean or /NDVI/30m
extractionlayer = "/udd/nhwhk/PROJECTS/testmulticore/toyCohort_nurses51.csv" # string with path to spatial layer to use for extraction. Can be a CSV or SHP or GDB 
layername = "sites_10M" # Layer name used when extraction layer is an SHP or GDB
IDfield<-"UUID" # Field in extraction layer specifying IDs for features, can be unique or not, used to chunk up batch jobs
Xfield<- "longitude"
Yfield<- "latitude"
startdatefield = "start_date" # Field in extraction layer specifying first date of observations
enddatefield = "end_date" # Field in extraction layer specifying last date of observations
predays = 5 # Integer specifying how many days preceding 'startingdatefield' to extract data. i.e. 365 will mean data extraction will begin 1 year before startdatefield
weights = NA # string specifying file path to raster weights, should only be used when extraction layer is a polygon layer
partition = "linux12h"

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

##########Input PROCESSING HERE####################################################
## Call Desired functions from Functions_RasterExtraction source file
## The desired functions are mapped in creating the jobs via batchMap
#source("https://raw.githubusercontent.com/WillhKessler/GCMC_RScripts/main/Functions_RasterExtraction.R")
source("https://raw.githubusercontent.com/WillhKessler/GCMC_RScripts/multiprocessor_parallel/Functions_RasterExtraction.R")
##############################################################
##---- Set up the batch processing jobs
##---- Use the 'batchgrid' function to create a grid of variable combinations to process over. function considers input rasters, input features, and any weighting layers
jobgrid = function(rasterdir,extractionlayer,layername,IDfield,Xfield,Yfield,startdatefield,enddatefield,predays,weightslayers,partition){
  require("tools")
  
  ##---- Set up the batch processing jobs
  pvars = list.dirs(path = rasterdir,full.names = FALSE,recursive = FALSE)
  
  if(file_ext(extractionlayer)=="csv"){
    feature<-read.csv(extractionlayer,stringsAsFactors = FALSE)
    feature$OID<-1:nrow(feature)
    write.csv(x = feature,file = paste0(file_path_sans_ext(extractionlayer),"_tmp",".csv"),row.names = FALSE)
    feature<-feature$OID
    layername = NA
    weightslayers = NA
    extractionlayer<-paste0(file_path_sans_ext(extractionlayer),"_tmp",".csv")
  }else if(file_ext(extractionlayer) %in% c("shp","gdb")){
    require('terra')
    vectorfile<- vect(x=extractionlayer,layer=layername)
    vectorfile$OID<-1:nrow(vectorfile)
    writeVector(x = vectorfile,filename = paste0(file_path_sans_ext(extractionlayer),"_tmp.",file_ext(extractionlayer)),layer=layername,overwrite=TRUE)
    feature<- unlist(unique(values(vectorfile[,"OID"])))
    Xfield = NA
    Yfield = NA
    extractionlayer<-paste0(file_path_sans_ext(extractionlayer),"_tmp.",file_ext(extractionlayer))
  }
  
  if(partition=="linux01"){
    pbatch=c(1:10)
    cpu = 4
  }else if (partition == "linux12h"){
    pbatch = c(1:50)
    cpu = 4
  }
  
  output<- expand.grid(vars = pvars,
                       pieces = split(
                         x = split(x = feature, f = ceiling(seq_along(feature)/100)),
                         f = ceiling(seq_along(split(feature, ceiling(seq_along(feature)/100)))/sum(length(pbatch),length(cpu)))
                         ),
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
jobs<- batchMap(fun = p.extract.rast,
                jobgrid(rasterdir = rasterdir,
                          extractionlayer = extractionlayer,
                          layername = layername,
                          IDfield = IDfield,
                          Xfield = Xfield,
                          Yfield = Yfield,
                          startdatefield = startdatefield,
                          enddatefield = enddatefield,
                          predays = predays,
                          weightslayers = weights,
                          partition=partition),
                reg = reg)
#


jobs$chunk<-chunk(jobs$job.id,n.chunks = length(50))
setJobNames(jobs,paste(abbreviate(PROJECT_NAME),jobs$job.id,sep=""),reg=reg)

getJobTable()
getStatus()

#---- Submit jobs to scheduler
done <- batchtools::submitJobs(jobs,
                               reg=reg,
                               resources=list(partition=partition,
                                              walltime=36000,
                                              ntasks=1,
                                              ncpus=4,
                                              memory=10000,
                                              pm.backend = "multicore"))
#getStatus()

#waitForJobs() # Wait until jobs are completed



