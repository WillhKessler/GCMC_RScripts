## Download Biodiversity Metrics

## Human Footprint_index
hfi_baseurl<-"https://storage.googleapis.com/hii-export"
start_date<-"2001-01-01"
end_date<-"2020-01-01"
outputpath<- "S:/GCMC/Data/Ecology/Biodiversity/HII"
hfi_dates<-seq(as.Date(start_date,format="%Y-%m-%d"),as.Date(end_date,format="%Y-%m-%d"),by="year")
filenames<-paste0("hii_",hfi_dates,".tif")
  
hfi_url<-file.path(hfi_baseurl,hfi_dates,filenames)
options(timeout = 1000)
Map(function(u, d) download.file(u, d, mode="wb"), hfi_url, file.path(outputpath,filenames))
  

# ## Human Footprint_index
# hfi_baseurl<-"https://storage.googleapis.com/hii-export"
# start_date<-"2001-01-01"
# end_date<-"2020-01-01"
# outputpath<- "S:/GCMC/Data/Ecology/Biodiversity/HII"
# hfi_dates<-seq(as.Date(start_date,format="%Y-%m-%d"),as.Date(end_date,format="%Y-%m-%d"),by="year")
# filenames<-paste0("hii_",hfi_dates,".tif")
# 
# hfi_path<-file.path(hfi_baseurl,hfi_dates,filenames)
# 
# applydownload.file(url = ,destfile = file.path(outputpath,filename))