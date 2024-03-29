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
#Initialize conf files and template
#Initialize batchtools configuration files and template
if(!file.exists("slurm.tmpl")){
  download.file("https://bit.ly/3Oh9dRO","slurm.tmpl")
}else{
  print("template exists")
}

if(!file.exists("batchtools.conf.R")){
  download.file("https://bit.ly/3KPBwou", "batchtools.conf.R")
}else{
  print("conf file exists")
}

#Create a temporary registry item
if(file.exists(paste(PROJECT_NAME,"Registry",sep="_"))){
  reg = loadRegistry(paste(PROJECT_NAME,"Registry",sep="_"),writeable = TRUE,conf.file="batchtools.conf.R")
}else{
  reg = makeRegistry(file.dir = paste(PROJECT_NAME,"Registry",sep="_"), seed = 42,conf.file="batchtools.conf.R")
}

##############################################################


##---- The function to perform the extraction
extract.rast= function(vars,piece,rasterdir,extractionlayer,layername,IDfield,Xfield,Yfield,startdatefield,enddatefield,predays=0,weightslayers = NA){

  ##---- Load required packages, needs to be inside function for batch jobs
  require(terra)
  #require(sp)
  require(tools)
  require(ids)
  print(system("hostname",intern=TRUE))
  print(paste('Current working directory:',getwd()))
  print(paste('Current temp directory:',tempdir()))
  
  ##---- Climate Rasters
  rastfiles<-rasterdir
  climvars<-list.files(file.path(rastfiles,vars,"daily"),pattern = paste(".*",vars,".*[1-2][0-9][0-9][0-9][0-1][0-9][0-3][0-9]\\.bil$",sep=""),recursive=TRUE,full.names=TRUE)
  # Determine unique raster dates
  rdates<-unique(sapply(X = strsplit(file_path_sans_ext(basename(climvars)),"_"),FUN = function(x){x[length(x)]}))
  rdates<-rdates[order(rdates)]
  
  
  ##---- Extraction Features Layer
  if(file_ext(extractionlayer)=='csv'){
    extlayer<-read.csv(extractionlayer,stringsAsFactors = FALSE)
    extlayer<-extlayer[extlayer[IDField]==piece,]
    polygons<- vect(x = extlayer,geom = c(Xfield,Yfield), atts = extlayer)
  }else if (file_ext(extractionlayer) %in% c("shp","gdb")){
    polygons<-vect(x=extractionlayer,layer = layername,query = paste("SELECT * FROM ",layername," WHERE ",IDfield," = ",piece))  
    }
  polygons$extract_start<- as.character(as.Date(unlist(as.data.frame(polygons[,startdatefield])))-predays)
  polygons$stop_date<-as.character(as.Date(unlist(as.data.frame(polygons[,enddatefield]))))
 
  
  ##---- Create extraction date ranges for points
  polygonstartSeasonIndex<- sapply(polygons$extract_start, function(i) which((as.Date(rdates,tryFormats = "%Y%m%d")-as.Date(i)) <= 0)[which.min(abs(as.Date(rdates,tryFormats = "%Y%m%d")-as.Date(i))[(as.Date(rdates,tryFormats = "%Y%m%d")-as.Date(i)) <= 0])])
  polygonsendSeasonIndex<- sapply(polygons$stop_date, function(i) which((as.Date(rdates,tryFormats = "%Y%m%d")-as.Date(i)) <= 0)[which.min(abs(as.Date(rdates,tryFormats = "%Y%m%d")-as.Date(i))[(as.Date(rdates,tryFormats = "%Y%m%d")-as.Date(i)) <= 0])])
  polygons$first_extract<-as.Date(rdates[polygonstartSeasonIndex],tryFormats="%Y%m%d")
  polygons$last_extract<-as.Date(rdates[polygonsendSeasonIndex],tryFormats="%Y%m%d")
  
  
  ##---- Determine which raster dates fall within the data range
  rasterDateRange<-rdates[as.Date(rdates,tryFormats = "%Y%m%d")>=min(polygons$first_extract) & as.Date(rdates,tryFormats = "%Y%m%d")<=max(polygons$last_extract)]
  # Load Climate Rasters
  print("loading the climvars to rast()")
  climvars2<-sapply(rasterDateRange, function(x){climvars[grep(x,climvars)]})
  rasters<- rast(climvars2)
  #################################################################
  #################################################################
  ##---- Weights Rasters for spatial weights
  calc.spatialweights<- function(weightslayers,rasters,polygons){
    rweights<-list.files(weightslayers,full.names = TRUE)
    print(rweights)
    
    ## Rasterize the Weights data
    print('the weights rasters')
    weightrast<- rast(rweights)
    print(weightrast)
    
    ## Reproject everything to the same resolution and CRS
    print('reprojecting clim vars')
    crs(polygons)<-crs(rasters[1])
    crs(weightrast)<-crs(rasters[1])
    
    print('cropping weightrasters')
    weightrast<-crop(weightrast,polygons,snap="out")
  
    ## Create a composite population raster at the same crs and extent of the climate variables
    weightrast2<-sum(weightrast)
    print(weightrast2)
  
    # Crop and resample climate rasters to weights
    print("croppings rasters with weightrast2") 
    rasters2<-crop(rasters, weightrast2,snap='out')
    print("resampling rasters2")
    print("the tempdir(): ")
    print(tempdir())
    print("the current working directory")
    print(getwd())
    
    print("starting resample")
    rasters2<-resample(rasters2,weightrast2,method='bilinear')
    output<-data.frame()
    print('cropping the weightrast2 to polygon')  
    weightzone = crop(x= weightrast2,y= polygons, touches=FALSE,mask=TRUE)
    
    #Scale the population weights to sum to 1
    print('scaling the population weights')
    weights = weightzone*(1/sum(values(weightzone,na.rm=TRUE)))
    weights<-extend(weights,rasters2,fill=NA)
    weightedavg<-zonal(x=rasters2,z=polygons,w=weights, fun = mean,na.rm=TRUE)
    
    print("the weights average: ")
    print(weightedavg)
    output<-rbind(c(values(polygons[,IDfield]),weightedavg))
    return (output)
  }
  
  #################################################################
  #################################################################
  
  ##---- Perform Extractions
  if(is.polygons(polygons)){
    if(is.na(weightslayers)){
      rasters2<- crop(x = rasters, y = polygons,snap = 'out')
      tempoutput<-zonal(x=rasters2,z=polygons,fun=mean,na.rm=TRUE)
      output<-rbind(c(values(polygons[,IDfield]),tempoutput))
    }else{output<-calc.spatialweights(weightslayers= weightslayers,rasters= rasters,polygons= polygons)}
  }else if(is.points(polygons)){
    output<-extract(x = rasters,y = polygons,ID=FALSE)
    names(output)<-names(rasters)
    output<-cbind(polygons,output)
    
   "Do the extract for points" 
  }  
  
  return(list(exposure=vars,piece=piece,result=output,node = system("hostname",intern=TRUE), Rversion = paste(R.Version()[6:7],collapse=".") ))
}


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
done <- batchtools::submitJobs(jobs, 
                               reg=reg, 
                               resources=list(partition="linux01", walltime=3600000, ntasks=1, ncpus=1, memory=80000))
getStatus()

waitForJobs() # Wait until jobs are completed





