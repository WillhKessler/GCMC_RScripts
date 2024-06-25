require(terra)
require(tools)
greennessDir<-"S:/GCMC/Data/Greenness/NDVI/30m"
outputdir<-"S:/GCMC/Data/Greenness/NDVI/30m_CONUS"


## Recursively list all paths to TIFF rasters in the directory
## Input Directory
allFilePaths<- list.files(path = greennessDir,pattern = "*.tif$",all.files = TRUE,full.names = TRUE,recursive = TRUE,include.dirs = FALSE)
head(allFilePaths)

## Get file names of rasters from path
#allFiles<-file_path_sans_ext(basename(allFilePaths))
#head(allFiles)

## Mosaic all rasters for each season and write to disk
mosaicRasters<-function(directory,outputdir){
  
  inputdates<-unique(sapply(X = strsplit(file_path_sans_ext(basename(directory)),"_"),FUN = function(x){x[2]}))
  inputdates<-inputdates[grep("2023",inputdates)]
  
  for (d in inputdates){
    seasonalRasters<-lapply(allFilePaths[grep(d,directory)],function(path){rast(path)})
    
    y<-mosaic(sprc(seasonalRasters),fun="mean",filename = paste(outputdir,"/NDVI_CONUS","_",d,".tif",sep=""),overwrite=TRUE)
    
    #writeRaster(x = y, filename = paste(outputdir,"/NDVI_CONUS","_",d,".tif",sep=""),overwrite=TRUE)
    
  }
  
  
}


mosaicRasters(directory=allFilePaths,outputdir=outputdir)


