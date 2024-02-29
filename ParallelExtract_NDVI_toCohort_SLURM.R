########READ ME##############################################
## This script is intended to perform a parallel raster extraction of PRISM climate data to a csv of observation data using the SLURM job manager on the Channing compute cluster. 
## The required R version is 4.1.0
##
## Inputs required are:
## PRISM climate data, as daily BIL raster files, found here: /pc/nhair0a/Built_Environment/BE_Data/Geographic_Data/PRISM_daily/PRISM_data/an
## climate data assumes a naming convention of XXXXX_variable_YYYYMMDD.bil 
## CSV of cohorts observations. 
## The csv format assumes the following fields:  "OID",y","x", "start_date", "stop_date"
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
## REQUIRED INPUTS##
PROJECT_NAME<-"VITAL_NDVI_LINKAGE"
rasterdir<-"/d/tmp/nhairs/nhair0a/NDVI_30m"
cohortfilepath<-"/pc/nhair0a/VITAL/Feb2023/VITAL_geo_startstop_20230418.csv"
#cohortfilepath<-"S:/GCMC/_Code/TESTING_datasets/VITAL_toycohort57.csv"
IDfield<-"subject_ID"
startdatefield = "start_date"
enddatefield = "stop_date"
predays = 365

##REQUIRED##

##---- Set up the batch processing jobs
pvars = list.dirs(path = rasterdir,full.names = FALSE,recursive = FALSE)
feature = unique(read.csv(cohortfilepath)[,IDfield])
pars<- expand.grid(vars=pvars,
                   piece=feature,
                   rasterdir = rasterdir,
                   cohortfilepath=cohortfilepath,
                   IDfield = IDfield,
                   startdatefield = startdatefield,
                   enddatefield = enddatefield,
                   predays = predays
                   )




##---- Load packages and initialize required batchtools files
library(batchtools)
require(sp)
require(terra)
require(tools)
require(parallel)

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
extract.rast= function(vars,piece,rasterdir,cohortfilepath,IDfield,startdatefield,enddatefield,predays){
  ##---- Load required packages, needs to be inside function for batch jobs
  require(terra)
  require(sp)
  require(tools)
  require(ids)
  
  ##---- Required Inputs: 
  rasterdir<- rasterdir
  cohort<-read.csv(cohortfilepath)
  
  cohort$UUID<-uuid(nrow(cohort))
  #cohort$start_date<-as.Date(cohort[,startdatefield],format = "%m/%d/%Y")
  cohort$start_date<-as.Date(cohort[,startdatefield])
  cohort$extract_start<-cohort$start_date - predays
  #cohort$stop_date<-as.Date(cohort[,enddatefield],format = "%m/%d/%Y")
  cohort$stop_date<-as.Date(cohort[,enddatefield])
  
  
  ##---- Initialize raster list
  allRasterPaths<- list.files(path = rasterdir,
                              pattern = paste(vars,".*[1-2][0-9][0-9][0-9]-[0-1][0-9]-[0-3][0-9].tif$",sep=""),
                              all.files = TRUE,
                              full.names = TRUE,recursive = TRUE,include.dirs = FALSE)
  allFilePaths<- list.files(path = rasterdir,pattern = "*.tif$",all.files = TRUE,full.names = TRUE,recursive = TRUE,include.dirs = FALSE)
  allRasterPaths<-allFilePaths
  
  #---- Determine Unique Raster Dates
  rdates<-unique(sapply(X = strsplit(file_path_sans_ext(basename(allFilePaths)),"_"),FUN = function(x){x[2]}))
  rdates<-rdates[order(rdates)]
  
  ##---- Create rdate ranges for points
  cohortstartSeasonIndex<- sapply(cohort$extract_start, function(i) which((as.Date(rdates)-as.Date(i)) <= 0)[which.min(abs(as.Date(rdates)-as.Date(i))[(as.Date(rdates)-as.Date(i)) <= 0])])
  cohortendSeasonIndex<- sapply(cohort$stop_date, function(i) which((as.Date(rdates)-as.Date(i)) <= 0)[which.min(abs(as.Date(rdates)-as.Date(i))[(as.Date(rdates)-as.Date(i)) <= 0])])
  
  cohort$first_extract<-as.Date(rdates[cohortstartSeasonIndex])
  cohort$last_extract<-as.Date(rdates[cohortendSeasonIndex])
  
  ##---- Get the min and max start and end date of the entire cohort
  minstart<-min(cohort$first_extract)
  maxend<-max(cohort$last_extract)
  
  ##---- Subset the cohort into the appropriate piece. 
  cohort<-cohort[cohort[IDfield]==piece,]
  
  ##---- Vectorize Cohort Data, EPSG:4326 = WGS84, PRISM is NAD83=EPSG:4269
  cohort<-wrap(vect(x = cohort, geom = c("x","y"), crs = "epsg:4326",keepgeom=TRUE))
  cohort<-vect(cohort)
  cohort<-project(cohort,"epsg:4269")
  
  
  
  ##---- Determine which raster dates fall within the data range
  rasterDateRange<-rdates[rdates>=minstart & rdates<=maxend]
  
  
  
  ##---- Create your output dataset
  outputcohort<-cohort
  
  
  for (d in 1:length(rasterDateRange)){
    subcohort<-outputcohort[(rasterDateRange[d]>=outputcohort$first_extract & rasterDateRange[d]<=outputcohort$last_extract),]
    
    if (nrow(subcohort)==0){
      outputcohort<- cbind(outputcohort,data.frame(matrix(NA,nrow(outputcohort),ncol=1)))
      names(outputcohort)[length(names(outputcohort))]<-gsub("-","",rasterDateRange[d])
    }else{
      print(paste0("n in cohort: ",nrow(subcohort)))
      ##---- Perform the extraction
      if (any(grepl(rasterDateRange[d],allFilePaths))){
        #read the rasters
        seasonalRasters<-lapply(allFilePaths[grep(rasterDateRange[d],allFilePaths)],function(path){rast(path)})
        
        ## Loop through each raster in the list, extract values to points (will introduce NA for points outside the raster extents) 
        output<-as.data.frame(do.call(cbind,lapply(seasonalRasters,function(path){extract(x=path,y=subcohort,ID=FALSE)})))
        # extractoutput<-subcohort
        # for (r in seasonalRasters){
        #   message("the Date d:", rasterDateRange[d])
        #   message("the seasonal Rasters: ", sources(r[[1]]))
        #   
        #   ## The extraction output
        #   output <-extract(x = r[[1]], y = subcohort,ID=FALSE)
        #   ## add extraction column to the subcohort df
        #   #subcohort<-cbind(subcohort,output)
        #   extractoutput<-cbind(extractoutput,output)
        # }
        ## calculate the mean value for any points where their extraction was in multiple Rasters (maybe instances in edge cases where rasters overlap)
        t<- rowMeans(output,na.rm =TRUE)
        subcohort<-cbind(subcohort,as.data.frame(t))
        ## merge the subcohort w/ it's NDVI values back into the original cohort. cohort members not in subset will have NAs in the additional column
        outputcohort<-terra::merge(x = outputcohort,y = subcohort,all.x=TRUE, no.dups=TRUE)
        names(outputcohort)[length(names(outputcohort))]<-gsub("-","",rasterDateRange[d])
        
        }
      }
    }
  
  return(list(exposure=vars,piece=piece,result=wrap(outputcohort),node = system("hostname",intern=TRUE), Rversion = paste(R.Version()[6:7],collapse=".")))
}

########################################################################################################################################
########################################################################################################################################



clearRegistry(reg)

jobs<- batchMap(fun = extract.rast,
                pars,
                reg = reg)

jobs$chunk<-chunk(jobs$job.id,chunk.size = 10)

getJobTable()
getStatus()
done <- batchtools::submitJobs(jobs, reg=reg, resources=list(partition="linux01", walltime=3600000, ntasks=1, ncpus=1, memory=80000))
waitForJobs() # Wait until jobs are completed

# ##---- Combine all the outputs into a Dataframe
# results<-do.call("rbind", lapply(1:nrow(jobs), loadResult))
# 
# for (v in as.character(unique(unlist(results[,1])))){
#   
#   out<- lapply(results[as.character(unlist(results[,1]))==v,3],terra::vect)
#   out<-lapply(out,terra::as.data.frame)
#   out<-do.call("rbind",out)
#   write.csv(out,paste(PROJECT_NAME,"_",v,".csv",sep=""))
#   saveRDS(out,file = paste(PROJECT_NAME,"_",v,".rds",sep=""))
# }



