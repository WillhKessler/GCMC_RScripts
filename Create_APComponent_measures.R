library('terra')
library('tools')


projection_info<-"EPSG:4326"

files<-list.files("S:\\GCMC\\Data\\AirPollution\\PM25_Components\\",pattern="*[0-9]_non_urban.rds",recursive=TRUE,full.names=TRUE)

for(i in files){
  dat<-readRDS(i)
  
  datvect<-vect(dat,geom=c("lon","lat"),crs = projection_info)
  datvect<-terra::project(datvect,"ESRI:102010")
  #datrast<-rast(extend(ext(datvect),500),res=c(1000,1000),crs="ESRI:102010")
  datrast<-rast(extend(ext(datvect),25),res=c(50,50),crs="ESRI:102010")
  datrast<-rasterize(x=datvect,
                     y=datrast,
                     field=names(datvect),
                     fun=mean,
                     filename= paste0(dirname(dirname(file_path_sans_ext(i))),"\\",basename(file_path_sans_ext(i)),".tif"),overwrite=TRUE)
}  

