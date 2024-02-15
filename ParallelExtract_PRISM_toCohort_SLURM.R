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
## 3. Submit a the job using SLURM via the command: sbR -v 4.1.0 ExtractPRISM_VITAL.R

## Run this code in an interactive session from your current unix directory, where the created registry directory is located. 


#### R batchtools
library(batchtools)
require(sp)
require(terra)
require(tools)
require(parallel)

##REQUIRED##
##REQUIRED##
#Initialize conf files and template
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
if(file.exists("VitalExtractRegistry")){
  reg = loadRegistry("VitalExtractRegistry",writeable = TRUE,conf.file="batchtools.conf.R")
}else{
  reg = makeRegistry(file.dir = "VitalExtractRegistry", seed = 42,conf.file="batchtools.conf.R")
}
rasterdir<- "/pc/nhair0a/Built_Environment/BE_Data/Geographic_Data/PRISM_daily/PRISM_data/an"
##############################################################


##---- The function to perform the extraction
extract.rast= function(vars,piece){
  ##---- Load required packages, needs to be inside function for batch jobs
  require(terra)
  require(sp)
  require(tools)
  require(ids)
  
  ##---- Required Inputs: 
  rasterdir<- "/pc/nhair0a/Built_Environment/BE_Data/Geographic_Data/PRISM_daily/PRISM_data/an"
  cohort<-read.csv("/pc/nhair0a/VITAL/Feb2023/VITAL_geo_startstop_20230418.csv")
  cohort$UUID<-uuid(nrow(cohort))
  cohort$start_date<-as.Date(cohort$start_date,format = "%m/%d/%Y")
  cohort$stop_date<-as.Date(cohort$stop_date,format = "%m/%d/%Y")
  
  ##---- Initialize raster list and cohort Point to raster directory
  allRasterPaths<- list.files(path = rasterdir,
                              pattern = paste(".*",vars,".*[1-2][0-9][0-9][0-9][0-1][0-9][0-3][0-9].bil$",sep=""),
                              all.files = TRUE,
                              full.names = TRUE,recursive = TRUE,include.dirs = FALSE)
  
  ##---- Get the min and max start and end date of the entire cohort
  minstart<-min(cohort$start_date)
  maxend<-max(cohort$stop_date)
  
  ##---- Split the cohort into the appropriate piece. Hard coded into 10 chunks. 
  cohort<-split(cohort, rep(1:ceiling(nrow(cohort)/floor(nrow(cohort)/10)), each=floor(nrow(cohort)/10), length.out=nrow(cohort)))
  cohort<- cohort[[piece]]
  
  ##---- Vectorize Cohort Data, EPSG:4326 = WGS84
  cohort<-wrap(vect(x = cohort, geom = c("x","y"), crs = "epsg:4326",keepgeom=TRUE))
  cohort<-vect(cohort)
  cohort<-project(cohort,"epsg:4269") #PRISM is NAD83, EPSG:4269
  
  
  ##---- Create your output dataset
  outputcohort<-cohort
  
  ##---- Perform the extraction process
  d<-minstart
  while (d<=maxend){
    subcohort<-outputcohort[(d>=outputcohort$start_date & d<=outputcohort$stop_date),]
    
    if (nrow(subcohort)==0){
      outputcohort<- cbind(outputcohort,data.frame(matrix(NA,nrow(outputcohort),ncol=1)))
      names(outputcohort)[length(names(outputcohort))]<-gsub("-","",d)
    }else{
      print(paste0("n in cohort: ",nrow(subcohort)))
      ##---- Perform the extraction
      if (any(grepl(gsub("-","",d),allRasterPaths))){
        ##Read the rasters
        mrast<-rast(lapply(allRasterPaths[grep(gsub("-","",d),allRasterPaths)],function(path){rast(path)}))
        ##Extract the raster values
        valuematrix<-extract(mrast,subcohort,ID=FALSE)
        ## Add an empty column to the outputcohort
        outputcohort<- cbind(outputcohort,data.frame(matrix(NA,nrow(outputcohort),ncol=1)))
        ## Bind the raster values to the subcohort
        # subcohort<-cbind(subcohort,valuematrix)
        
        ##---- Recombine the cohort
        ##Find the subcohort index in the output
        ind<-match(subcohort$UUID,outputcohort$UUID)
        ## bind the raster values to output
        outputcohort[ind,ncol(outputcohort)]<-valuematrix
        # outputcohort[ind,ncol(outputcohort)]<-subcohort[[daysbetween[d]]]
        #print(d)
        names(outputcohort)[length(names(outputcohort))]<-gsub("-","",d)
      }else{
        outputcohort<-cbind(outputcohort,data.frame(matrix(NA,nrow(outputcohort),ncol=1)))
        names(outputcohort)[length(names(outputcohort))]<-gsub("-","",d)
      }
    }
    d=d+1
  }
  
  return(list(exposure=vars,piece=piece,result=wrap(outputcohort),node = system("hostname",intern=TRUE), Rversion = paste(R.Version()[6:7],collapse=".") ))
}


##---- Set up the batch processing jobs
pvars = list.dirs(path = rasterdir,full.names = FALSE,recursive = FALSE)
#pvars="ppt"
pars<- expand.grid(vars=pvars,piece=1:10)
# pars<- expand.grid(vars=c("PM25","NO3","O3"),chunk=1:10)
clearRegistry(reg)

jobs<- batchMap(fun = extract.rast,
                pars,
                reg = reg)
#jobs$chunk<-chunk(jobs$job.id,chunk.size = 10)

getJobTable()
getStatus()
done <- batchtools::submitJobs(jobs, reg=reg, resources=list(partition="linux01", walltime=3600000, ntasks=1, ncpus=1, memory=1024))
waitForJobs() # Wait until jobs are completed

##---- Combine all the outputs into a Dataframe
results<-do.call("rbind", lapply(1:nrow(jobs), loadResult))

for (v in as.character(unique(unlist(results[,1])))){
  
  out<- lapply(results[as.character(unlist(results[,1]))==v,3],terra::vect)
  out<-lapply(out,terra::as.data.frame)
  out<-do.call("rbind",out)
  write.csv(out,paste("VITAL_PRISMDAILY_",v,".csv",sep=""))
  saveRDS(out,file = paste("VITAL_PRISMDAILY_",v,".rds",sep=""))
}



