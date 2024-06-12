####---- READ ME ----###
# Use this script as a template for setting up and running batch processing jobs on your local machine. 
# This utilizes one or more cores in a multicored machine.
# The required inputs are standardized for ALL functions defined in the Raster Extractions R source file 

##---- REQUIRED INPUTS ----##
PROJECT_NAME<-"GCMC2_ParallelTest" # string with a project name
rasterdir<-"S:/GCMC/Data/Climate/PRISM/"
extractionlayer = "C:/Users/wik191/OneDrive - Harvard University/_Projects/Andrea_Bellavia/sites_10M.shp"
layername = "sites_10M" # Layer name used when extraction layer is an SHP or GDB
IDfield<-"ORIG_FID" # Field in extraction layer specifying IDs for features, can be unique or not, used to chunk up batch jobs
Xfield<- "X"
Yfield<- "Y"
startdatefield = "start_date" # Field in extraction layer specifying first date of observations
enddatefield = "end_date" # Field in extraction layer specifying last date of observations
predays = 0 # Integer specifying how many days preceding 'startingdatefield' to extract data. i.e. 365 will mean data extraction will begin 1 year before startdatefield
weights = NA # string specifying file path to raster weights, should only be used when extraction layer is a polygon layer



##---- Required Packages
library(batchtools)
require(terra)
require(tools)


##REQUIRED##
##---- Initialize conf files and template
##---- Initialize batchtools

##---- Create a temporary registry item
if(file.exists(paste(PROJECT_NAME,"Registry",sep="_"))){
  reg = loadRegistry(paste(PROJECT_NAME,"Registry",sep="_"),writeable = TRUE)
  reg$cluster.functions=makeClusterFunctionsSocket()
}else{
  reg = makeRegistry(file.dir = paste(PROJECT_NAME,"Registry",sep="_"), seed = 42)
  reg$cluster.functions=makeClusterFunctionsSocket()
}


##########Input PROCESSING HERE####################################################
##---- Call Desired functions from Functions_RasterExtraction source file
##---- The desired functions are called in batchMap
source("https://raw.githubusercontent.com/WillhKessler/GCMC_RScripts/main/Functions_RasterExtraction.R")


##---- Set up the batch processing jobs
##---- grid should contain columns for all desired variable combinations
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


##---- Clear the R registry
clearRegistry(reg)

##---- Create jobs
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

jobs$chunk <- chunk(jobs$job.id, chunk.size = 1000)


getJobTable()
getStatus()

##---- Submit Jobs
batchtools::submitJobs(resources = list(memory=80000),reg = reg)
waitForJobs()

# If any of the jobs failed, they will be displayed here as 'Errors"
getStatus()


