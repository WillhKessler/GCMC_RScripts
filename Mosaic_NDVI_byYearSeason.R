require(raster)
require(tools)
#require(terra)
greennessDir<-"S:/GCMC/Data/Greenness/NDVI"
outputdir<-"S:/GCMC/temp"


## Input Directory
#greennessDir<- "S:/GCMC/Data/Greenness/NDVI"


## Generate all Seasonal periods
# allDays<-c("01")
# allMonths<-c("01","04","07","10")
# allYears<-c("1984","1985","1986","1987","1988","1989","1990","1991","1992",
#             "1993","1994","1995","1996","1997","1998","1999","2000","2001",
#             "2002","2003","2004","2005","2006","2007","2008","2009","2010",
#             "2011","2012","2013","2014","2015","2016","2017","2018")
# allDates<-paste(allYears,allMonths,allDays,sep="-")
# 
# allRegions <-c("MontanaPart1","NC1","NC2","Texas1","Texas2","TX3","CaliPart1","CaliPart2", "MontanaPart2", "WashingtonOregon", "ArkansasLouisiana", 
#                "Oklahoma", "NewMexico", "Arizona", "Colorado", "Utah", "Nevada", "Wyoming", "Idaho", "Florida", "SouthCarolinaGeorgia", 
#                "SouthAtlantic1", "Kansas", "MissouriIowa", "Nebraska", "Minnesota", "NorthSouthDakota", "MississippiAlabama", "KentuckyTennessee", "IndianaOhio", 
#                "Illinois", "Michigan", "Wisconsin", "MiddleAtlantic", "NewEngland")


## Recursively list all paths to TIFF rasters in the directory
## Input Directory
#greennessDir<- "/pc/n3mhs00/Landsat/2018_2019_2020"
greennessDir<-"S:/GCMC/Data/Greenness/NDVI"
allFilePaths<- list.files(path = greennessDir,pattern = "*.tif$",all.files = TRUE,full.names = TRUE,recursive = TRUE,include.dirs = FALSE)
head(allFilePaths)

## Get file names of rasters from path
allFiles<-file_path_sans_ext(basename(allFilePaths))
head(allFiles)

## Mosaic all rasters for each season and write to disk
mosaicRasters<-function(directory){
  
  inputdates<-unique(sapply(X = strsplit(file_path_sans_ext(basename(directory)),"_"),FUN = function(x){x[2]}))
  
  for (d in inputdates[1:2]){
    seasonalRasters<-lapply(allFilePaths[grep(d,directory)],function(path){raster(path)})
    
    seasonalRasters$fun<-mean
    seasonalRasters$na.rm<-TRUE
    y<- do.call(mosaic,seasonalRasters)
    n#rstack<-stack(seasonalRasters)
    #rsrc<-sprc(seasonalRasters)
    # writeRaster(mosaic(rsrc), "/pc/n3mhs00/Landsat/2018_2019_2020/NDVI30m_CONUS.tif")
    #writeRaster(y, "/pc/n3mhs00/Landsat/2018_2019_2020/NDVI30m_CONUS.tif")
    writeRaster(y, paste(outputdir,"/NDVI30m_CONUS","_",d,".tif",sep=""))
    
  }
  
  
}


mosaicRasters(directory=allFilePaths)


