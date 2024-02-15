########READ ME##############################################
## This script is intended to perform a parallel raster extraction of PRISM climate data to a csv of observation data using the SLURM job manager on the Channing compute cluster. 
## The required R version is 4.1.0
##
## Inputs required are:
## PRISM climate data, as daily BIL raster files, found here: /pc/nhair0a/Built_Environment/BE_Data/Geographic_Data/PRISM_daily/PRISM_data/an
## climate data assumes a naming convention of XXXXX_variable_YYYYMMDD.bil 
## CSV of cohorts observations. 
## The csv format assumes the following fields:  "latitude","longitude", "start_date", "end_date"
## dates should be formatted as "YYYY-MM-DD"
##
## Update the contents of this code to point to the proper location for the directory for PRISM and the cohort CSV, and your output file name/location
## line 53
## line 54
## line 143, 144



## To run this code on the Channing cluster: 
## 1.	First login into the Unix system (head node (udd), or wherever you need to ‘be’, not in the compute cluster)
## 2.	Open R: module load R/4.1.0 
## R
## 3.Run the following code to install some packages and download necessary files: 
# (‘batchtools’)
# dir.create("mytestdir")
# setwd("mytestdir")
# download.file("https://bit.ly/3Oh9dRO","batchtools.slurm.tmpl")
# download.file("https://bit.ly/3KPBwou", "batchtools.conf.R") 

##4.	Update the contents of batchtools.conf.R to point to “batchtools.slurm.tmpl” instead of “slurm.tmpl”

## Run this code in an interactive session from your current unix directory, where the created registry directory is located. 


#### R batchtools
library(batchtools)
require(sp)
require(terra)
require(tools)
require(parallel)
setwd("mytestdir")
##REQUIRED##
#Create a temporary registry item
reg = makeRegistry(file.dir = "PRISMextractSLURM", seed = 42,conf.file="batchtools.conf.R")
##############################################################


##---- The function to perform the extraction
extract.rast= function(vars,piece){
  ##---- Load required packages, needs to be inside function for batch jobs
  require(terra)
  require(sp)
  require(tools)
  
  ##---- Required Inputs: 
  rasterdir<- "/pc/nhair0a/Built_Environment/BE_Data/Geographic_Data/PRISM_daily/PRISM_data/an"
  cohort<-read.csv("/udd/nhwhk/mytestdir/toyCohort_nurses55.csv")
  
  ##---- Initialize raster list and cohort Point to raster directory
  allRasterPaths<- list.files(path = rasterdir,pattern = paste(".*",vars,".*[0-2][0-9][0-3][0-9][0-1][0-9][0-9][0-9].bil$",sep=""),all.files = TRUE,full.names = TRUE,recursive = TRUE,include.dirs = FALSE)
  
  ##---- Get the min and max start and end date of the entire cohort
  minstart<-min(cohort$start_date)
  maxend<-max(cohort$end_date)
  
  ##---- Split the cohort into the appropriate piece. Hard coded into 10 chunks. 
  cohort<-split(cohort, rep(1:ceiling(nrow(cohort)/floor(nrow(cohort)/10)), each=floor(nrow(cohort)/10), length.out=nrow(cohort)))
  cohort<- cohort[[piece]]
  
  ##---- Vectorize Cohort Data, EPSG:4326 = WGS84
  cohort<-wrap(vect(x = cohort, geom = c("longitude","latitude"), crs = "epsg:4326",keepgeom=TRUE))
  cohort<-vect(cohort)
  cohort<-project(cohort,"epsg:102010") # For Air Pollution Data, either ESRI:102010 or epsg:102010 depending on version of terra......
  
  
  ##---- Create your output dataset
  outputcohort<-cohort
  
  ##---- Perform the extraction process
  d<-minstart
  while (d<=maxend){
    subcohort<-outputcohort[(d>=outputcohort$start_date & d<=outputcohort$end_date),]
    
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
    d=format(as.Date(d)+1,"%Y-%m-%d")
  }
  
  return(list(exposure=vars,piece=piece,result=wrap(outputcohort),node = system("hostname",intern=TRUE), Rversion = paste(R.Version()[6:7],collapse=".") ))
}
 

##---- Set up the batch processing jobs
pvars = list.dirs(path = rasterdir,full.names = FALSE,recursive = FALSE)
pars<- expand.grid(vars=pvars,piece=1:10)
# pars<- expand.grid(vars=c("PM25","NO3","O3"),chunk=1:10)
clearRegistry(reg)

jobs<- batchMap(fun = extract.rast,
         pars,
         reg = reg)
jobs$chunk<-chunk(jobs$job.id,chunk.size = 3)

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
  write.csv(out,paste("NursesPRISMDAILY_",v,".csv",sep=""))
  saveRDS(out,file = paste("Nurses_PRISMDAILY_",v,".rds",sep=""))
}



