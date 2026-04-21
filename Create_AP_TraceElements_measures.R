library('terra')
library('tools')


projection_info<-"EPSG:4326"

files<-list.files("S:\\GCMC\\tmp\\pmcomp_temp",pattern=".*.rds$",recursive=TRUE,full.names=TRUE)
done<-list.files("S:\\GCMC\\tmp\\pmcomp_temp",pattern=".*.tif$",recursive=TRUE,full.names=TRUE)
files<-files[!basename(file_path_sans_ext(files)) %in% basename(file_path_sans_ext(done))]
grids<- list.files("S:\\GCMC\\tmp\\pmcomp_temp\\grids",pattern="*.gpkg$",recursive=TRUE,full.names=TRUE)

for(g in grids){
  gridname<- file_path_sans_ext(basename(g))
  region_files<-files[grep(gridname,files)]
  for(compfile in region_files){
    dat<-readRDS(compfile)
    #dat<-as.data.frame(dat)
    
    datvect<-vect(g)
    datvect$value<-dat
    
    datvect<-terra::project(datvect,"ESRI:102010")
    if(grepl("non-urban",gridname)){
      datrast<-rast(extend(ext(datvect),500),res=c(1000,1000),crs="ESRI:102010")
      }else{  
    datrast<-rast(extend(ext(datvect),25),res=c(50,50),crs="ESRI:102010")
    }
    
    datrast<-rasterize(x=datvect,
                       y=datrast,
                       field="value",
                       fun=mean,
                       filename= paste0(dirname(dirname(file_path_sans_ext(compfile))),"\\",basename(file_path_sans_ext(compfile)),".tif"),overwrite=TRUE)
  }  
}  
  
######Confirm all rasters are valid:

files<-list.files("S:\\GCMC\\tmp\\pmcomp_temp",pattern=".*.rds$",recursive=TRUE,full.names=TRUE)
files[basename(files) %in% files[duplicated(basename(files))]]



######Create single layer coverages

library(terra)
require(tools)

files<-list.files("S:\\GCMC\\tmp\\pmcomp_temp",recursive=TRUE,pattern="*.tif$",full.names = TRUE)
#files2<-files[grep("non-urban",files)]

#newfiles2<-file.path(dirname(files2),basename(gsub("non-urban","non_urban",files2)))
#file.rename(files2,newfiles2)
files<-list.files("S:\\GCMC\\tmp\\pmcomp_temp",recursive=TRUE,pattern="*.tif$",full.names = TRUE)

patterns = unlist(unique(lapply(X = strsplit(file_path_sans_ext(basename(files)),split="-"),
                                FUN = function(x){paste0(unlist(x)[3:4],collapse = "-")})))



for(i in patterns){
  urbanfiles<-list.files("S:\\GCMC\\tmp\\pmcomp_temp/urban-rds",#"S:/GCMC/Data/AirPollution/PM25_Components/urban",
                         recursive=TRUE,
                         pattern=paste0("-",i,".tif$"),
                         full.names = TRUE)
  nonurbanfiles<-list.files("S:\\GCMC\\tmp\\pmcomp_temp/non-urban-rds",# "S:/GCMC/Data/AirPollution/PM25_Components/non_urban",
                            recursive=TRUE,
                            pattern=paste0(i,".*.tif$"),
                            full.names = TRUE)
  
  # Mosaic the chunks together
  rsrc<-sprc(urbanfiles)
  urbanrast<-mosaic(rsrc,fun='mean',filename=file.path("S:\\GCMC\\tmp\\pmcomp_temp",paste0("urban_",i,".tif")))
  
  # Mosaic the non-urban chunks together
  rsrc<-sprc(nonurbanfiles)
  nonurbanrast<-mosaic(rsrc,fun='mean',filename=file.path("S:\\GCMC\\tmp\\pmcomp_temp",paste0("nonurban_",i,".tif")))
  
  # Read in files as rasters, one for urban, one for non-urban
  urbanfiles<-list.files("S:\\GCMC\\tmp\\pmcomp_temp/",
                         pattern=paste0("^urban_",i,".tif$"),
                         full.names = TRUE)
  nonurbanfiles<-list.files("S:\\GCMC\\tmp\\pmcomp_temp/",# "S:/GCMC/Data/AirPollution/PM25_Components/non-urban",
                            pattern=paste0("nonurban_",i,".tif$"),
                            full.names = TRUE)
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
  traceElement<-mosaic(nonurbanrast,resample_urbanrast,fun="first")
  
  #Create Out dir if not exists
  if(!dir.exists(file.path("S:/GCMC/Data/AirPollution/TraceElements","TraceElements_1kmfill",strsplit(i,split = "-")[[1]][1],strsplit(i,split = "-")[[1]][2]))){
  dir.create(file.path("S:/GCMC/Data/AirPollution/TraceElements","TraceElements_1kmfill",strsplit(i,split = "-")[[1]][1],strsplit(i,split = "-")[[1]][2]),recursive = T)
             }else{}
  # Use a void fill to fill in any remaining NA values using an average of surrounding values 
  traceElement<-focal(traceElement,
                      w=3,
                      fun=mean,
                      na.policy="only",
                      na.rm=T,
                      filename=file.path("S:/GCMC/Data/AirPollution/TraceElements","TraceElements_1kmfill",strsplit(i,split = "-")[[1]][1],strsplit(i,split = "-")[[1]][2],paste0("TraceElements_",i,".tif")),
                   overwrite=TRUE)  
  
  #outrast<- rast(list(urbanrast,nonurbanrast,PM25_comp))
  #writeRaster(outrast,filename=file.path(dirname(dirname(dirname(dirname(urbanfiles[1])))),paste0(paste0(unlist(strsplit(i[[1]],"_"))[-1],collapse="_"),".tif")),
  #            overwrite=TRUE))
  
 
}



## Rename 1kmfill rasters to standard naming convention
oldfiles<-list.files('S:\\GCMC\\Data\\AirPollution\\TraceElements\\TraceElements_1kmfill',full.names=T,recursive=T)
olddir<-dirname(oldfiles)
oldnames<-basename(oldfiles)
newnames<-gsub("TraceElements","TraceElements_",oldnames)
newnames<-gsub("-","_",newnames)
newnames<-gsub(".tif","-01-01.tif",newnames)
newnames<-file.path(olddir,newnames)

###### Move urban and nonurban rasters to proper directory
require('tools')
oldfiles<-list.files('S:\\GCMC\\tmp\\pmcomp_temp',full.names=T,pattern="*.tif$")
olddir<-dirname(oldfiles)
oldnames<-basename(oldfiles)
newnames<-gsub("-","_",oldnames)
newpath<-sapply(strsplit(file_path_sans_ext(newnames),"_"),FUN=function(x){file.path("S:\\GCMC\\Data\\AirPollution\\TraceElements",x[1],x[2],x[3])})
newnames<-gsub(".tif","-01-01.tif",newnames)

newfiles<-file.path(newpath,newnames)

for(f in 1:length(newfiles)){
  if(!dir.exists(dirname(newfiles[f]))){
    dir.create(dirname(newfiles[f]),recursive = T)
    file.rename(oldfiles[f],newfiles[f])
  }else{file.rename(oldfiles[f],newfiles[f])}
}

