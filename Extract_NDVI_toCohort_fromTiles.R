
#############-----------READ ME---------########################################
#This script will extract NDVI values from tiled NDVI geotiff files to csv of GPS 
#datapoints with the following structure
#
#GPS data: df[n,list("id","uid","longitude","latitude","start_datetime","end_datetime")]
#Raster data: filename: "AOIs_YYYY-mm-dd.tif"

# The Code pulls "start_datetime" from all points in the GPS data and compares them to 
# the date substring from all the raster data file names to identify the most recent HISTORIC 
# NDVI collection date to each GPS point. i.e. min((GPSdate - NDVIdate) <=0)
# Then the code iterates through all unique NDVI dates, and subsets the GPS data to the matching points
# and performs a raster extraction. Results are recombined and exported to CSV. 

# The final "write.csv() is commented out to prevent overwriting of existing data

###################################################################

require(sp)
require(raster)
require(tools)
#########################################
## ## Required
greennessDir<-"/pc/n3mhs00/Landsat/2018_2019_2020"
cohortdir<- "/pc/n3mhs00/Cindy/For_WilliamK/RawGPSDataRINX.csv"
outputdirectory<- "/pc/n3mhs00/Cindy/For_WilliamK/GPSData_NDVI_2.csv"
#########################################


##Read in the Cohort Data
cohort<-read.csv(cohortdir,header=TRUE,stringsAsFactors = FALSE)

##---- Read in the raster data paths
allFilePaths<- list.files(path = greennessDir,pattern = "*.tif$",all.files = TRUE,full.names = TRUE,recursive = TRUE,include.dirs = FALSE)
head(allFilePaths)

#---- Determine Unique Dates
dates<-unique(sapply(X = strsplit(file_path_sans_ext(basename(allFilePaths)),"_"),FUN = function(x){x[2]}))
dates<-dates[order(dates)]

##----Vectorize Cohort Data
cohort<- SpatialPointsDataFrame(coords = cohort[,c("longitude","latitude")],data = cohort,proj4string = CRS("+init=epsg:4326"))

cohort<-spTransform(x = cohort,CRSobj = CRS("+init=epsg:4269"))

##---- Determine which Season the data occurs
cohortSeasonIndex<- sapply(cohort@data[,"start_date"], function(i) which((as.Date(dates)-as.Date(i)) <= 0)[which.min(abs(as.Date(dates)-as.Date(i))[(as.Date(dates)-as.Date(i)) <= 0])])


##---- Get Original list of column names in cohort data
col_combine<- colnames(cohort@data)


## Perform the raster Extraction
for (d in 1:length(dates)){
  
  message("For date d: ", dates[d])
  message("allFiles matching date: ", allFilePaths[grep(dates[d],allFilePaths)])
  ## Create a list of rasters matching the current date d
  seasonalRasters<-lapply(allFilePaths[grep(dates[d],allFilePaths)],function(path){raster(path)})
  ## Create a subset of the cohort for the current date 
  subcohort <-cohort[cohortSeasonIndex ==d,]
  
  ## Loop through each raster in the list, extract values to points (will introduce NA for points outside the raster extents) 
  for (r in seasonalRasters){
   message("the Date d:", dates[d])
   message("the seasonal Rasters: ", r[[1]]@file@name)
   ## The extraction output
   output <-extract(x = r[[1]], y = subcohort) 
   ## add extraction column to the subcohort df
   subcohort<-cbind(subcohort,output)
  
  }
  ## calculate the mean value for any points where their extraction was in multiple Rasters (maybe instances in edge cases where rasters overlap)
  t<- rowMeans(subcohort@data[ , colnames(subcohort@data) %in% col_combine ==FALSE],na.rm =TRUE)
  
  ## merge the subcohort w/ it's NDVI values back into the original cohort. cohort members not in subset will have NAs in the additional column
  cohort<-merge(x = cohort,y = cbind(subcohort@data[,colnames(subcohort@data) %in% col_combine],t), by=col_combine, no.dups=TRUE)

}
## duplicate the df- not really necessary, just for testing purposes
cohort2<-cohort

## Calculate row means to flatten the extra columns in the DF. 
cohort2@data<- cbind(cohort2@data[,colnames(cohort2@data) %in% col_combine],NDVI =rowMeans(cohort2@data[ , colnames(cohort2@data) %in% col_combine ==FALSE],na.rm =TRUE))

## Write output to CSV. 
#write.csv(x = cohort2@data,file = outputdirectory,row.names = FALSE)






