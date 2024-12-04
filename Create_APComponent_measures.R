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



library(terra)
require(tools)

files<-list.files("S:/GCMC/Data/AirPollution/PM25_Components/",recursive=TRUE,pattern="*.tif$",full.names = TRUE)

patterns = unlist(unique(lapply(X = strsplit(file_path_sans_ext(basename(files)),split="_"),
                                FUN = function(x){paste0(unlist(x)[1:4],collapse = "_")})))



for(i in patterns){
  urbanfiles<-list.files("S:/GCMC/Data/AirPollution/PM25_Components/urban",
                         recursive=TRUE,
                         pattern=paste0(i,".*.tif$"),
                         full.names = TRUE)
  nonurbanfiles<-list.files("S:/GCMC/Data/AirPollution/PM25_Components/non_urban",
                            recursive=TRUE,
                            pattern=paste0(i,".*.tif$"),
                            full.names = TRUE)
  
  
  # Read in files as rasters, one for urban, one for non-urban
  urbanrast<-rast(urbanfiles[1])
  nonurbanrast<-rast(nonurbanfiles[1])
  
  # extend both rasters so their extents match
  #urbanrast<- extend(urbanrast,nonurbanrast,snap = "near")
  #nonurbanrast<-extend(nonurbanrast,urbanrast,snap = "near")
  
  # create an empty raster of the non urban extent and resolution
  nonurbanrastnofill<-nonurbanrast
  values(nonurbanrastnofill)<-NA
  
  # Resample the urban rast to the non-urban resolution
  # resample_urbanrast<-resample(urbanrast,nonurbanrast,method="cubicspline",threads=TRUE,filename="S:/GCMC/tmp/resample.tif")
  resample_urbanrast<-resample(urbanrast,nonurbanrast,method="cubicspline",threads=TRUE)
  
  # Mosaic the urban and non-urban rasters together
  # PM25_comp<-mosaic(nonurbanrast,resample_urbanrast,fun="first",filename="S:/GCMC/tmp/mosaic.tif")
  PM25_comp<-mosaic(nonurbanrast,resample_urbanrast,fun="first")
  
  # Use a void fill to fill in any remaining NA values using an average of surrounding values 
  PM25_comp<-focal(PM25_comp,w=3,fun=mean,na.policy="only",na.rm=T,filename=file.path(dirname(dirname(dirname(dirname(urbanfiles[1])))),paste0(paste0(unlist(strsplit(i[[1]],"_"))[-1],collapse="_"),".tif")),
                   overwrite=TRUE)  
  
  #outrast<- rast(list(urbanrast,nonurbanrast,PM25_comp))
  #writeRaster(outrast,filename=file.path(dirname(dirname(dirname(dirname(urbanfiles[1])))),paste0(paste0(unlist(strsplit(i[[1]],"_"))[-1],collapse="_"),".tif")),
  #            overwrite=TRUE))
  
  
}

