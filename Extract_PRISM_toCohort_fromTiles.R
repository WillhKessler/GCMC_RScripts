#############-----------READ ME---------########################################
#This script will extracts Daily PRISM climate variables from CONUS coverage bil rasters
# to lat/long contained in a csv with the following structure
#
#GPS data: df[n,list("id","uid","longitude","latitude","start_datetime","end_datetime")]
#Raster data: filename: "AOIs_YYYY-mm-dd.tif"

# The Code pulls "start_datetime" from all points in the GPS data and compares them to 
# the date substring from all the raster data file.
# Dates must match exactly for extraction to occur. Assumes dates are formated as 'YYYYmmdd'

# The final "write.csv() is commented out to prevent overwriting of existing data
## Required Inputs:
#PRISMDir- directory to where the PRISM data resides. File structure can be nested
#outputdirectory- directory where to write the CSV
#cohortdir- directory where the CSV of lat/long resides
###################################################################

require(sp)
require(raster)
require(tools)
#########################################
## ## Required
PRISMDir<-"/pc/nhair0a/Built_Environment/BE_Data/Geographic_Data/PRISM_daily/PRISM_data/an"
outputdirectory<- "/pc/n3mhs00/Cindy/For_WilliamK/tmp"
cohortdir<- "/pc/n3mhs00/Cindy/For_WilliamK/RawGPSDataRinx.csv"
#########################################


##Read in the Cohort Data
cohort<-read.csv(cohortdir,header=TRUE,stringsAsFactors = FALSE)

##----Vectorize Cohort Data
##input GPS data is in WGS84, unprojected. 
cohort<- SpatialPointsDataFrame(coords = cohort[,c("longitude","latitude")],data = cohort,proj4string = CRS("+init=epsg:4326"))
## Reproject to NAD83, unprojected geographic data
cohort<-spTransform(x = cohort,CRSobj = CRS("+init=epsg:4269"))


## Loop through each PRISM variable........There is a better way to do this.........
for (subdir in list.dirs(PRISMDir,recursive =FALSE)){
  ## Get the PRISM variable name: list::7
  varname<- strsplit(subdir,"/")[[1]][length(strsplit(subdir,"/")[[1]])]
  
  ## Get all the file paths for the current PRISM variable. Limit to BIL files
  allFilePaths<- list.files(path = subdir,pattern = "*.bil$",all.files = TRUE,full.names = TRUE,recursive = TRUE,include.dirs = FALSE)
  head(allFilePaths)
  
  ## List dates from PRISM Rasters  
  dates<-unique(sapply(X = strsplit(file_path_sans_ext(basename(allFilePaths)),"_"),FUN = function(x){x[5]}))
  dates<-dates[order(dates)]
  
  ## Subset date list to just those dates found in your cohort
  shortdatelist<- dates[which(as.Date(dates,"%Y%m%d") %in% unique(as.Date(cohort@data[,"start_date"])))]
  
  ## Determine the columns in the original cohort data. This is important for ensuring data consistency 
  col_combine<- colnames(cohort@data)
  
  ## Loop through each date in the list of raster dates short list 
  for (d in 1:length(shortdatelist)){
    # messages for clarity
    message("date: ",shortdatelist[d], " is in the cohort")
    message("For date d: ", shortdatelist[d])
    message("allFiles matching date: ", allFilePaths[grep(shortdatelist[d],allFilePaths)])
    
    # Generate a list of all rasters matching the date in question. For PRISM daily. This is 1 raster.
    seasonalRasters<-lapply(allFilePaths[grep(shortdatelist[d],allFilePaths)],function(path){raster(path)})
    
    ## Subset the cohort based on those points who have a date matching the current date in the loop
    logic<- as.Date(cohort@data[,"start_date"])==as.Date(shortdatelist[d],"%Y%m%d")
    subcohort <-cohort[logic,]
    message("number of observations in subcohort matching day d: ",length(subcohort))
    
    ## Perform the extraction using the subsetted cohort and each raster in the list
    for (r in seasonalRasters){
      # plot(r[[1]],add=TRUE)
      message("the Date d:", shortdatelist[d])
      message("the PRISM Rasters: ", r[[1]]@file@name)
      ## Extract
      output<- extract(x = r[[1]], y = subcohort,) 
      
      ## bind the output back to the subcohort dataframe. Output var name isn't sticking......
      subcohort<-cbind(subcohort,PRISMvar=output)
    }
    ## Write each daily cohort subset to file. These will be reconstituted later
    message("writing file: ",paste(outputdirectory,"/","GPSdata_PRISM_",varname,"_",d,".csv",sep=""))
    write.csv(subcohort@data, file = paste(outputdirectory,"/","GPSdata_PRISM_",varname,"_",d,".csv",sep=""),row.names = FALSE)
  }}

