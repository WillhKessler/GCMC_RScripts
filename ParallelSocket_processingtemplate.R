####---- READ ME ----###
# Use this script as a template for setting up and running batch processing jobs on your local machine. 
# This utilizes one or more cores in a multicored machine.
# The required inputs are standardized for ALL functions defined in the Raster Extractions R source file 

##---- REQUIRED INPUTS ----##
PROJECT_NAME<-"Bellavia_polygon_LINKAGE" # string with a project name
rasterdir<-"~/Data/Climate/PRISM/" # string with a file path to raster covariates to extract- function will try to pull variable names from sub directories i.e /PRISM/ppt or /PRISM/tmean or /NDVI/30m
extractionlayer = "~/_Projects/Andrea_Bellavia/sites_10M.shp" # string with path to spatial layer to use for extraction. Can be a CSV or SHP or GDB 
layername = "sites_10M" # Layer name used when extraction layer is an SHP or GDB
IDfield<-"ORIG_FID" # Field in extraction layer specifying IDs for features, can be unique or not, used to chunk up batch jobs
Xfield<- "X"
Yfield<- "Y"
startdatefield = "start_date" # Field in extraction layer specifying first date of observations
enddatefield = "end_date" # Field in extraction layer specifying last date of observations
predays = 0 # Integer specifying how many days preceding 'startingdatefield' to extract data. i.e. 365 will mean data extraction will begin 1 year before startdatefield
weights = NA # string specifying file path to raster weights, should only be used when extraction layer is a polygon layer




##---- Required Packages
listOfPackages <- c("batchtools","terra","tools")
for (i in listOfPackages){
     if(! i %in% installed.packages()){
         install.packages(i, dependencies = TRUE)
     }
     require(i)
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
## Call Desired functions from Functions_RasterExtraction source file
## The desired functions are mapped in creating the jobs via batchMap
source("https://raw.githubusercontent.com/WillhKessler/GCMC_Rscripts/innerParallel/Functions_RasterExtraction.R")

##############################################################
##---- Set up the batch processing jobs
##---- Use the 'batchgrid' function to create a grid of variable combinations to process over. function considers input rasters, input features, and any weighting layers

batchgrid = function(rasterdir,extractionlayer,layername,IDfield,Xfield,Yfield,startdatefield,enddatefield,predays,weightslayers){
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
  
  output<- expand.grid(vars = pvars,
                       pieces = feature,
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
jobs$chunk <- chunk(jobs$job.id, chunk.size = 1000)


getJobTable()
getStatus()

##---- Submit Jobs
batchtools::submitJobs(resources = c(memory=80000),reg = reg)
waitForJobs()
