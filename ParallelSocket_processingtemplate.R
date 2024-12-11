####---- READ ME ----###
# Use this script as a template for setting up and running batch processing jobs on your local machine. 
# This utilizes one or more cores in a multicored machine.
# The required inputs are standardized for ALL functions defined in the Raster Extractions R source file 

##---- REQUIRED INPUTS ----##
PROJECT_NAME<-"GCMC2_ParallelTest" # string with a project name
rasterdir<-"S:/GCMC/Data/Climate/PRISM/daily/" # string with a file path to raster covariates to extract- function will try to pull variable names from sub directories i.e /PRISM/ppt or /PRISM/tmean or /NDVI/30m
extractionlayer = "S:/GCMC/_Code/TESTING_datasets/csv/toyCohort_nurses51.csv" # string with path to spatial layer to use for extraction. Can be a CSV or SHP or GDB 
layername = "sites_10M" # Layer name used when extraction layer is an SHP or GDB
IDfield<-"OID" # Field in extraction layer specifying IDs for features, can be unique or not, used to chunk up batch jobs
Xfield<- "longitude" 
Yfield<- "latitude"
startdatefield = "start_date" # Field in extraction layer specifying first date of observations
enddatefield = "end_date" # Field in extraction layer specifying last date of observations
predays = 0 # Integer specifying how many days preceding 'startingdatefield' to extract data. i.e. 365 will mean data extraction will begin 1 year before startdatefield
weights = NA # string specifying file path to raster weights, should only be used when extraction layer is a polygon layer
period = "daily"


##---- Required Packages
##---- Required Packages
listOfPackages <- c("batchtools","terra","tools","reshape2","ids")
for (i in listOfPackages){
     if(! i %in% installed.packages()){
         install.packages(i, dependencies = TRUE)
     }
     require(i,character.only=TRUE)
}



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
source("https://raw.githubusercontent.com/WillhKessler/GCMC_RScripts/refs/heads/main/Functions_RasterExtraction.R")

##---- Set up the batch processing jobs
##---- grid should contain columns for all desired variable combinations
batchgrid = function(rasterdir,extractionlayer,layername,IDfield,Xfield,Yfield,startdatefield,enddatefield,predays,weightslayers,period){
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
    IDfield="OID"
  }else if(file_ext(extractionlayer) %in% c("shp","gdb")){
    require('terra')
    vectorfile<- vect(x=extractionlayer,layer=layername)
    vectorfile$OID<-1:nrow(vectorfile)
    writeVector(x = vectorfile,filename = paste0(file_path_sans_ext(extractionlayer),"_tmp.",file_ext(extractionlayer)),layer=layername,overwrite=TRUE)
    feature<- unlist(unique(values(vectorfile[,"OID"])))
    Xfield = NA
    Yfield = NA
    extractionlayer<-paste0(file_path_sans_ext(extractionlayer),"_tmp.",file_ext(extractionlayer))
    IDfield="OID"
    if (file_ext(extractionlayer)=="shp"){
      layername<-paste0(extractionlayer,"_tmp")
    }
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
                       period = period,
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
                          period=period,
                          weightslayers = weights),
                reg = reg)

jobs$chunk <- chunk(jobs$job.id, chunk.size = 10)


getJobTable()
getStatus()

##---- Submit Jobs
batchtools::submitJobs(jobs,resources = list(memory=5000),reg = reg)
waitForJobs()

# If any of the jobs failed, they will be displayed here as 'Errors"
getStatus()

# Look at the Error Messages to see what the errors are:
getErrorMessages()



