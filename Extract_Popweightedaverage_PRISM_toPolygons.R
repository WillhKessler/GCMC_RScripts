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


##############################################################

  
## Climate Rasters
rastfiles<-"S:\\GCMC\\Data\\Climate\\PRISM"
allclimvars<-list.files(file.path(rastfiles),pattern = paste(".\\./daily/./.bil$",sep=""),all.files = TRUE,full.names=TRUE,recursive = TRUE,include.dirs = FALSE)



## Population Rasters for spatial weights
population<-list.files("S:\\GCMC\\Data\\Population\\WorldPop\\USA",pattern = ".tif$",full.names=TRUE)

## Boston Metro Counties
counties<-vect(x="S:\\GCMC\\Data\\AdminBoundaries/CensusGeometry_2020.gdb",layer="Counties")
Bostoncounties<- counties[counties$GEOID %in% c("25009", "25017","25021","25023","25025"),]

## Boston Metro ZCTAs
polygons<-"S:\\GCMC\\Data\\AdminBoundaries/CensusGeometry_2020.gdb"
polygons<-vect(x=polygons,layer="ZCTA")
polygons<-polygons[Bostoncounties]
  
  ##---- The function to perform the extraction
extract.popweightedrast= function(vars,piece){
  ## Subset to the piece
  polygons<- polygons[polygons$GEOID10==piece,]
  
  ## Rasterize the population data
  poprast<- rast(population)
  
  ## Reproject everything to the same resolution and CRS
  #climvars2<-grep(paste(".*",vars,".*_(19|20)\\d\\d(0[1-9]|1[0-2])(0[1-9]|[12][0-9]|3[01])\\.bil$",sep=""),allclimvars)
  climvars<-list.files(file.path(rastfiles,vars,"/daily/"),pattern = paste(".*",vars,".*_(19|20)\\d\\d(0[1-9]|1[0-2])(0[1-9]|[12][0-9]|3[01])\\.bil$",sep=""),all.files = TRUE,full.names=TRUE,recursive = TRUE,include.dirs = FALSE)
  crs(poprast,proj=TRUE)
  crs(polygons,proj=TRUE)
  crs(rast(climvars[1]),proj=TRUE)
  
  crs(polygons)<-crs(rast(climvars[1]))
  crs(poprast)<-crs(rast(climvars[1]))
  poprast<-crop(poprast,ext(polygons),snap='out')
  
  # if (crs(poprast,proj=TRUE) == crs(polygons,proj=TRUE)){
  #   print("TRUE")
  #   poprast<-crop(poprast,ext(polygons),snap='out')
  # }else{
  #   print("FALSE")
  #   #polygons3<-terra::project(x = polygons2,y=poprast)
  #   polygons<-terra::project(x = polygons,y=poprast)
  #   poprast<-crop(poprast,ext(polygons),snap='out')
  #   
  # }
  # 
  # if (crs(poprast,proj=TRUE) == crs(polygons,proj=TRUE) &&
  #     crs(poprast,proj=TRUE) == crs(rast(climvars[1]),proj=TRUE)){
  #   print("all good")
  # }else{
  #   print("not all good")
  #   polygons<-terra::project(polygons,rast(climvars[1]))
  #   poprast<-terra::project(poprast,crs(rast(climvars[1])),method='bilinear',align=TRUE) 
  # }
  
  ## Create a composite population raster at the same crs and extent of the climate variables
  poprast2<-sum(poprast)
  print(poprast2)
  
  # Load Climate Rasters
  rasters<-rast(climvars)
  rasters2<-crop(rasters, poprast2,snap='out')
  rasters2<-resample(rasters2,poprast2,method='bilinear')
  
  output<-data.frame()
  # for(i in 1:length(polygons)){
  #   p<-polygons[i]
    
  popzone = crop(x= poprast2,y= polygons, touches=FALSE,mask=TRUE)
  
  #Scale the population weights to sum to 1
  weights = popzone*(1/sum(values(popzone,na.rm=TRUE)))
  weights<-extend(weights,rasters2,fill=NA)
  weightedavg<-zonal(x=rasters2,z=polygons,w=weights, fun = mean, na.rm=TRUE)
  output<-rbind(c(polygons$ZCTA5CE10,weightedavg))
  
  return(output)
}


##############################################################
##---- Set up the batch processing jobs
pvars = list.dirs(path = rastfiles,full.names = FALSE,recursive = FALSE)
ZCTAs = polygons$GEOID10

##################

##Run Processing
for (row in pvars){
  output<-data.frame()
  for (piece in ZCTAs){
  output<-rbind(extract.popweightedrast(vars = row,piece = piece))
  }
  write.csv(output,paste("S:\\GCMC\\tmp\\ZCTA_over65_PRISMDAILY_",row,".csv",sep=""))
  saveRDS(output,file = paste("S:\\\GCMC\\\tmp\\ZCTA_over65_PRISMDAILY_",row,".rds",sep=""))
}



