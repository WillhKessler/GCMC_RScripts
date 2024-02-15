require(sp)
require(raster)
require(tools)
#########################################
## ## Required
PRISMDir<-"/pc/nhair0a/Built_Environment/BE_Data/Geographic_Data/PRISM_daily/PRISM_data/an" 
outputdirectory<- "/pc/n3mhs00/Cindy/For_WilliamK"
cohortdir<- "/pc/n3mhs00/Cindy/For_WilliamK/RawGPSDataRinx.csv"
#########################################
# ## Create Toy Data
# require(spdep) require(spData) set.seed(123) data(us_states) geo <- 
# as_Spatial(us_states[us_states$NAME %in% 
# c("Alaska","Hawaii")==FALSE,][1]) points<- spsample(geo, n=50, 
# "random")
# 
# # Define the start and end dates
# start_date <- as.Date("2018-01-01") end_date <- as.Date("2018-04-30")
# 
# # Generate random dates
# num_dates <- 50 # Number of dates to generate rand_dates <- 
# sample(seq(start_date,end_date,by="day"), num_dates, replace = TRUE) 
# cohort<-data.frame(latitude = points@coords[,"y"], longitude = 
# points@coords[,"x"], start_date = rand_dates, end_date = rand_dates)


##Read in the Cohort Data
cohort<-read.csv(cohortdir,header=TRUE,stringsAsFactors = FALSE)
##----Vectorize Cohort Data
cohort<- SpatialPointsDataFrame(coords = cohort[,c("longitude","latitude")],data = cohort,proj4string = CRS("+init=epsg:4326")) 
cohort<-spTransform(x = cohort,CRSobj = CRS("+init=epsg:4269"))

##---- Read in the raster data paths
for (subdir in list.dirs(PRISMDir,recursive =FALSE)[1]){
  varname<- strsplit(subdir,"/")[[1]][length(strsplit(subdir,"/")[[1]])]
  
  allFilePaths<- list.files(path = subdir,pattern = "*.bil$",all.files = TRUE,full.names = TRUE,recursive = TRUE,include.dirs = FALSE)
  head(allFilePaths)
  
  #---- Determine Dates Year and Season
  dates<-unique(sapply(X = strsplit(file_path_sans_ext(basename(allFilePaths)),"_"),FUN = function(x){x[5]}))
  dates<-dates[order(dates)]
  
  ##---- Determine which Season the data occurs
  cohortSeasonIndex<- sapply(cohort@data[,"start_date"], function(i) which((as.Date(dates,"%Y%m%d")-as.Date(i)) <=0)[which.min(abs(as.Date(dates,"%Y%m%d")-as.Date(i))[(as.Date(dates,"%Y%m%d")-as.Date(i)) <= 0])])
  
  
  ##---- Get OG list of column names in cohort data
  col_combine<- colnames(cohort@data)
  
  
  ## Perform the raster Extraction
  for (d in 1:length(dates)){
    if (dates[d] %in% dates[unique(cohortSeasonIndex)]){
      message("date: ",dates[d], "is in the cohort")
    
      message("For date d: ", dates[d])
      message("allFiles matching date: ", allFilePaths[grep(dates[d],allFilePaths)])
      seasonalRasters<-lapply(allFilePaths[grep(dates[d],allFilePaths)],function(path){raster(path)})
      subcohort <-cohort[cohortSeasonIndex ==d,]
      message("number of observations in subcohort matching day d: ",length(subcohort))
    
      for (r in seasonalRasters){
      # plot(r[[1]],add=TRUE)
       message("the Date d:", dates[d])
       #message("the subcohort: ", subcohort@data)
       message("the PRISM Rasters: ", r[[1]]@file@name)
       message(extract(x = r[[1]], y = subcohort))
       output <-extract(x = r[[1]], y = subcohort,)
       subcohort<-cbind(subcohort,output)
      
      }
    
      cohort<-merge(x = cohort,y = subcohort@data, by=col_combine, no.dups=TRUE)
    }else{}
    
  }
  cohort2<-cohort
  
  
  cohort2@data<- cbind(cohort2@data[,colnames(cohort2@data) %in% col_combine],NDVI =rowMeans(cohort2@data[ , colnames(cohort2@data) %in% col_combine ==FALSE],na.rm =TRUE))
  names(cohort2@data)[names(cohort2@data)=="NDVI"]<-varname
  cohort<- cohort2
}
write.csv(x = cohort@data,file = 
paste(outputdirectory,"/","GPSdata_PRISM_test.csv",sep=""),row.names = FALSE)
