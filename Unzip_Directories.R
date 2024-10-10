
require(tools)
require(utils)
#########################################
##---Required Inputs---------############
zipdir<-"S:\\GCMC\\Data\\AirPollution\\ec"

#########################################
##---- Clipping Boundary

##---- Read in the raster data paths
allzip<- list.files(path = zipdir,pattern = "*.zip$",all.files = TRUE,full.names = TRUE,recursive = TRUE,include.dirs = FALSE)
allzipdir<-file_path_sans_ext(allzip)

allunzip<-list.dirs(path=zipdir,full.names=TRUE)
remaining<- allzip[!(allzipdir %in% allunzip)]
head(allzip)



for(i in remaining){
  dirout <- file_path_sans_ext(basename(i))
  unzip(zipfile = i, exdir = paste(zipdir,dirout,sep = "/"),overwrite = TRUE)
  print(paste(zipdir,dirout,sep = "/"))
  }
