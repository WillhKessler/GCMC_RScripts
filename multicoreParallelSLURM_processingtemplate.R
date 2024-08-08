##---- REQUIRED INPUTS ----##
PROJECT_NAME<-"innerParallelTest" # string with a project name
rasterdir<- "S:/GCMC/Data/Climate/PRISM/daily" # string with a file path to raster covariates to extract- function will try to pull variable names from sub directories i.e /PRISM/ppt or /PRISM/tmean or /NDVI/30m
extractionlayer = "S:\\GCMC\\_Code\\TESTING_datasets\\csv\\toyCohort_nurses51.csv" # string with path to spatial layer to use for extraction. Can be a CSV or SHP or GDB 
layername = "sites_10M" # Layer name used when extraction layer is an SHP or GDB
IDfield<-"UUID" # Field in extraction layer specifying IDs for features, can be unique or not, used to chunk up batch jobs
Xfield<- "longitude"
Yfield<- "latitude"
startdatefield = "start_date" # Field in extraction layer specifying first date of observations
enddatefield = "end_date" # Field in extraction layer specifying last date of observations
predays = 0 # Integer specifying how many days preceding 'startingdatefield' to extract data. i.e. 365 will mean data extraction will begin 1 year before startdatefield
weights = NA # string specifying file path to raster weights, should only be used when extraction layer is a polygon layer
partition = "linux01"

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
source("https://raw.githubusercontent.com/WillhKessler/GCMC_RScripts/main/Functions_RasterExtraction.R")

##############################################################
##---- Set up the batch processing jobs
##---- Use the 'batchgrid' function to create a grid of variable combinations to process over. function considers input rasters, input features, and any weighting layers
jobgrid = function(rasterdir,extractionlayer,layername,IDfield,Xfield,Yfield,startdatefield,enddatefield,predays,weightslayers,pbatch){
  require("tools")
  
  ##---- Set up the batch processing jobs
  pvars = list.dirs(path = rasterdir,full.names = FALSE,recursive = FALSE)
  
  if(file_ext(extractionlayer)=="csv"){
    vectorfile<-read.csv(extractionlayer)
    feature<-vectorfile[,IDfield]
    #feature<-unique(vectorfile[,IDfield])
    layername = NA
    weightslayers = NA
    cpu = c(1:4)
    if(partition=="linux01"){
      pbatch = c(1:10)
    }
    
  }else if(file_ext(extractionlayer) %in% c("gdb")){
    require('terra')
    vectorfile<- vect(x=extractionlayer,layer=layername)
    feature<- unlist(unique(values(vectorfile[,IDfield])))
    Xfield = NA
    Yfield = NA
  }else if(file_ext(extractionlayer) %in% c("shp")){
    require('terra')
    vectorfile<- vect(x=extractionlayer)
    feature<- unlist(unique(values(vectorfile[,IDfield])))
    Xfield = NA
    Yfield = NA
  }
  
  output<- expand.grid(vars = pvars,
                       pieces = split(
                         x = split(x = feature, f = ceiling(seq_along(feature)/100)),
                         f = ceiling(seq_along(split(feature, ceiling(seq_along(feature)/100)))/sum(length(pbatch),length(cpu)))
                         ),
                       #pieces = split(feature, ceiling(seq_along(feature)/sum(length(pbatch),length(cpu)))),
                       #piece = feature,
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

# batchgrid = function(rasterdir,extractionlayer,layername,IDfield,Xfield,Yfield,startdatefield,enddatefield,predays,weightslayers){
#   require("tools")
#   
#   ##---- Set up the batch processing jobs
#   pvars = list.dirs(path = rasterdir,full.names = FALSE,recursive = FALSE)
#   
#   if(file_ext(extractionlayer)=="csv"){
#     feature<-unique(read.csv(extractionlayer)[,IDfield])
#     layername = NA
#     weightslayers = NA
#   }else if(file_ext(extractionlayer) %in% c("gdb")){
#     require('terra')
#     vectorfile<- vect(x=extractionlayer,layer=layername)
#     feature<- unlist(unique(values(vectorfile[,IDfield])))
#     Xfield = NA
#     Yfield = NA
#   }
#   else if(file_ext(extractionlayer) %in% c("shp")){
#     require('terra')
#     vectorfile<- vect(x=extractionlayer)
#     feature<- unlist(unique(values(vectorfile[,IDfield])))
#     Xfield = NA
#     Yfield = NA
#   }
#   
#   output<- expand.grid(vars = pvars,
#                      piece = feature,
#                      rasterdir = rasterdir,
#                      extractionlayer = extractionlayer,
#                      layername = layername,
#                      IDfield = IDfield,
#                      Xfield = Xfield,
#                      Yfield = Yfield,
#                      startdatefield = startdatefield,
#                      enddatefield = enddatefield,
#                      predays = predays,
#                      weightslayers = weightslayers,
#                      stringsAsFactors = FALSE)
#   return(output)
# }

##----  Make sure registry is empty
clearRegistry(reg)

##----  create jobs from variable grid
# jobs<- batchMap(fun = extract.rast,
#                 batchgrid(rasterdir = rasterdir,
#                           extractionlayer = extractionlayer,
#                           layername = layername,
#                           IDfield = IDfield, 
#                           Xfield = Xfield,
#                           Yfield = Yfield,
#                           startdatefield = startdatefield,
#                           enddatefield = enddatefield,
#                           predays = predays,
#                           weightslayers = weights),
#                 reg = reg)

jobs<-batchMap(fun=innerParallel,cpu =4)

jobs$chunk<-chunk(jobs$job.id,chunk.size = 90)
setJobNames(jobs,paste(abbreviate(PROJECT_NAME),jobs$job.id,sep=""),reg=reg)

getJobTable()
getStatus()

##---- Submit jobs to scheduler
# done <- batchtools::submitJobs(jobs, 
#                                reg=reg, 
#                                resources=list(partition="linux01", 
#                                               walltime=3600000, 
#                                               ntasks=1, 
#                                               ncpus=4, 
#                                               memory=80000,
#                                               pm.backend = "multicore"))
getStatus()

waitForJobs() # Wait until jobs are completed



