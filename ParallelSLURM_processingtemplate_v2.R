require('terra')

##-------------------------------------------------------------------------------------
##---- REQUIRED INPUTS ----##
PROJECT_NAME = "ExampleLinkage" # string with a project name
rasterdir = "/pc/nhair0a/_mock_nhair0a/Raw_Exposure_Data/Natural_Environment/Greenness/NDVI/NDVI_30m" # string with a file path to raster covariates to extract- function will try to pull variable names from sub directories i.e /PRISM/ppt or /PRISM/tmean or /NDVI/30m
extractionlayer = "/d/tmp/nhairs/nhair0a/linkagebonanza/ExtractionBonanza_ExampleData.csv"
layername = "ExtractionBonanza_ExampleData" # Layer name used when extraction layer is an SHP or GDB, ignored when extraction layer is a CSV
IDfield = "subject_ID" # Field in extraction layer specifying IDs for features, can be unique or not, used to chunk up batch jobs
Xfield = "longitude" # A Field containing the X coordinate (Longitude), in decimal degrees, only for CSV
Yfield = "latitude" # A Field containing the Y coordinate (Longitude), in decimal degrees, only for CSV
startdatefield = "startdate" # Field in extraction layer specifying first date of observations
enddatefield = "enddate" # Field in extraction layer specifying last date of observations
predays = 0 # Integer specifying how many days preceding 'startingdatefield' to extract data. i.e. 365 will mean data extraction will begin 1 year before startdatefield
weights = NA # string specifying file path to raster weights, should only be used when extraction layer is a polygon layer
period = "daily" #specify a period to summarize values: daily, monthly, yearly, defaults to daily
email = "" #Enter your email. SLURM will send you an email when your jobs are complete.



##---- Required Packages
listOfPackages <- c("batchtools","terra","tools","reshape2","ids")
for (i in listOfPackages){
  if(! i %in% installed.packages()){
    install.packages(i, dependencies = TRUE)
  }
  require(i,character.only=TRUE)
}



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
source("https://raw.githubusercontent.com/WillhKessler/GCMC_RScripts/refs/heads/main/Functions_RasterExtraction.R")

##############################################################
##---- Set up the batch processing jobs
##---- Use the 'batchgrid' function to create a grid of variable combinations to process over. function considers input rasters, input features, and any weighting layers

batchgrid = function(rasterdir,period,extractionlayer,IDfield,Xfield,Yfield,startdatefield,enddatefield,predays,weightslayers){
  require("tools")
  
  ##---- Set up the batch processing jobs
  pvars = list.dirs(path = rasterdir,full.names = FALSE,recursive = FALSE)
  ##---- 
 
  if(file_ext(extractionlayer)=="csv"){
    feature<-read.csv(extractionlayer,stringsAsFactors = FALSE)
    feature$OID<-1:nrow(feature)
    write.csv(x = feature,file = paste0(file_path_sans_ext(extractionlayer),"_tmp",".csv"),row.names = FALSE)
    extractionlayer<-paste0(file_path_sans_ext(extractionlayer),"_tmp",".csv")
    IDfield="OID"
    feature<-feature$OID
    layername = NA
    weightslayers = NA
    dat<-read.csv(extractionlayer,stringsAsFactors = F)
    dat<-vect(dat,crs="EPSG:4326",geom=c("latitude","longitude"),keepgeom=T)
    datchunk<-split(dat,f=list(dat[[startdatefield]],dat[[enddatefield]]))
    datchunk<-sapply(X = datchunk,FUN = wrap)
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
      layername<-paste0(file_path_sans_ext(basename(extractionlayer)))
    }
    dat<-vect(extactionlayerlayer=layername)
    datchunk<-split(dat,f=list(dat[[startdatefield]],dat[[enddatefield]]))
    datchunk<-sapply(X = splitdat,FUN = wrap)
  }
  ##---- 
  output<- expand.grid(vars = pvars,
                       period = period,
                       datchunk = datchunk,
                       rasterdir = rasterdir,
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
jobs<- batchMap(fun = extract.rastv2,
                batchgrid(rasterdir = rasterdir,
                          extractionlayer = extractionlayer,
                          IDfield = IDfield, 
                          Xfield = Xfield,
                          Yfield = Yfield,
                          startdatefield = startdatefield,
                          enddatefield = enddatefield,
                          predays = predays,
                          weightslayers = weights,
                          period=period),
                reg = reg)
jobs$chunk<-chunk(jobs$job.id,n.chunks=50)
setJobNames(jobs,paste(abbreviate(PROJECT_NAME),jobs$job.id,sep=""),reg=reg)

getJobTable()
getStatus()

##---- Submit jobs to scheduler
done <- batchtools::submitJobs(jobs, 
                               reg=reg, 
                               resources=list(partition="linux01", walltime=3600000, ntasks=1, ncpus=1, memory=5000,email=email))
#Sys.sleep(1000)
#estimateRuntimes(jobs,reg=reg)
getStatus()

waitForJobs() # Wait until jobs are completed
