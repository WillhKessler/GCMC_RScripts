
require(tools)
require(dplyr)

#CHANGE INPUTS HERE
datefrequency<-"seasonal"
filedirectory="S:\\GCMC\\Data\\Greenness\\EVI\\30m"
filedirectory="S:\\GCMC\\Data\\Greenness\\NDVI/fs90m/"




##############################################################################################
#List all Files
filedir<- list.files(filedirectory,pattern="*.tif$",full.names = T)

#Get file names of rasters from path
allFiles<-file_path_sans_ext(basename(filedir))
head(allFiles)

bnames<-unique(
  sapply(
    X = strsplit(allFiles,"-"),
    FUN = function(x){paste(x[1:3],collapse="-")}))

#FileName Grid
raster_atts<-as.data.frame(do.call(rbind, strsplit(allFiles,"_")))
colnames(raster_atts)<-c("metric","resolution","date")

# Tabulate metrics
actual_metrics<- raster_atts |> count(metric)

# Tabulate resolution
actual_resolutions<- raster_atts |> count(resolution)

# Tabulate Dates
actual_dates<- raster_atts |> count(date)

## Compare tabulated totals with expected
#Expected metric
metric<-unique(sapply(X =strsplit(allFiles,"_"),FUN = function(x){x[1]}))
metric<-metric[order(metric)]

#Expected resolution
spat_resolution<-unique(sapply(X = strsplit(bnames,"_"),FUN = function(x){x[2]}))
spat_resolution<-spat_resolution[order(spat_resolution)]

#Expected dates
if(datefrequency =="seasonal"){
  startdate=as.Date("1984-01-01")
  enddate=as.Date(paste0(format(Sys.Date(), "%Y"), "-01-01"))
  dateseq<-as.character(seq.Date(startdate,enddate,by = "quarter"))
}else if(datefrequency=="daily"){
  startdate=as.Date("1984-01-01")
  enddate=as.Date(paste0(format(Sys.Date(), "%Y"), "-01-01"))
  dateseq<-as.character(seq.Date(startdate,enddate,by = "day"))
}else if(datefrequency=="yearly"){
  startdate=as.Date("1984-01-01")
  enddate=as.Date(paste0(format(Sys.Date(), "%Y"), "-01-01"))
  dateseq<-as.character(seq.Date(startdate,enddate,by = "year"))
}

### Tabulate Expected Counts
expected<-expand.grid(metric,spat_resolution,dateseq,stringsAsFactors = F)
colnames(expected)<-c("metric","resolution","date")
# Tabulate metrics
expected_metrics<- data.frame(expected |> count(metric))

# Tabulate resolution
expected_resolutions<- data.frame(expected |> count(resolution))

# Tabulate Dates
expected_dates<- data.frame(expected |> count(date))


## Compare Metrics
if(isTRUE(all.equal(expected_metrics,actual_metrics))){
  message("All Expected Files are Present")
  print("All Expected Files are Present")
}else{
  all.equal(expected_metrics,actual_metrics)
  all.equal(expected_resolutions,actual_resolutions)
  all.equal(expected_dates,actual_dates)
  message("The following files are expected but not found")
  setdiff(expected, raster_atts)
  
  message("The following files were found, but not expected. Check File names for consistency")
  setdiff(raster_atts,expected)
  }


               