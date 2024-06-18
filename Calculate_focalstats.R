require(terra)
require(tools)


files<-list.files(path="S:\\GCMC\\Data\\Greenness\\CanopyCover\\nlcd_tcc_CONUS_30m",pattern = ".tif$",full.names = TRUE)


for(i in files){
  fw<- focalMat(x = rast(i),d = 1230,type = "circle",fillNA=TRUE)
  focalrast<-focal(x= rast(i), 
                   fun =mean,
                   na.rm=TRUE,
                   filename = paste("S:\\GCMC\\Data\\Greenness\\CanopyCover\\nlcd_tcc_CONUS_1230mfs\\",basename(file_path_sans_ext(i)),"_1230m",".tif",sep="")
                   )
}


## To extract your focal raster to points do something like:
# addresses<-terra::vect(addresses) # addresses can be an sf object I think
#out<- terra::extract(focalrast,pointdata, bind=TRUE)