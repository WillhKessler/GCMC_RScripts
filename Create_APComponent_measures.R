library('terra')
library('tools')


projection_info<-"EPSG:4326"

files<-list.files("S:\\GCMC\\Data\\AirPollution\\PM25_Components\\",pattern="*[0-9]_non_urban.rds",recursive=TRUE,full.names=TRUE)

i=files[1]

dat<-readRDS(i)

datvect<-vect(dat[1:10,],geom=c("lon","lat"),crs = projection_info)
plot(datvect)
datvect<-terra::project(datvect,"ESRI:102004")
datrast<-rast(extend(ext(datvect),500),res=c(1000,1000),crs="ESRI:102004")
datrast<- rast(ext(datvect),res=c(1000,1000),crs="EPSG:6350")
  values(datrast)<-1:5
  plot(datrast)
  plot(datvect,add=TRUE)
  datrast<-rasterize(x=datvect,
                     y=datrast,
                     field=names(datvect),
                     fun=mean,
                     filename= paste0(dirname(dirname(file_path_sans_ext(i))),"\\",basename(file_path_sans_ext(i)),".tif"),
                     overwrite=TRUE)
  writeVector(datvect,filename = "S:/GCMC/tmp/ap_comptest2.shp",overwrite=TRUE)
  

datvect2<-datvect[1:100]
datrast2<-rast(ext(datvect),crs="EPSG:4326")
