
require(tools)
require(utils)
#########################################
##---Required Inputs---------############
#zipdir<-"S:\\GCMC\\Data\\AirPollution\\ec"
zipdir<-"S:/GCMC/tmp/PRISM_processing"
destdir<-"S:/GCMC/Data/Climate/PRISM"
#########################################
##---- Clipping Boundary

##---- Read in the raster data paths
allzip<- list.files(path = zipdir,pattern = "*.zip$",all.files = TRUE,full.names = TRUE,recursive = TRUE,include.dirs = FALSE)
allzipdir<-file_path_sans_ext(allzip)

#allunzip<-list.dirs(path=zipdir,full.names=TRUE)
allunzip<-dirname(allzipdir)

remaining<- allzip[!(allzipdir %in% allunzip)]
head(allzip)



for(i in remaining){
  #dirout <- file_path_sans_ext(basename(i))
  unzip(zipfile = i, exdir = dirname(i),overwrite = TRUE,)
  print(paste("unzipping:",i))
  file.remove(i)
}
print("Done!")


########################Move Files to Destination
newfiles<-list.files(zipdir,full.names = T,recursive = T)

newloc<-gsub("S:/GCMC/tmp/PRISM_processing",destdir,newfiles)


sapply(newPRISM,FUN=function(x,destdir){
  newloc<-newloc<-gsub("S:/GCMC/tmp/PRISM_processing",destdir,x)
  if(!dir.exists(dirname(newloc))){
    dir.create(dirname(newloc),recursive=T)
  }
  file.rename(from=x,newloc)
})
file.copy(newPRISM,newloc,recursive=T)



