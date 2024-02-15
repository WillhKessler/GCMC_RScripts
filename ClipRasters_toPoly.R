
#############-----------READ ME---------########################################
#This script will Clip  one or more rasters to an AOI defined by a shapefile or featureclass 
##### Required Inputs:
# rasterdir- A directory contining one or more rasters to be clipped. Script is currently limited to *.tif
# clippinggeometry- A path to a shapefile or Geodatabase. If given as GDB, specify the layer name
# clippinglayer- name of the layer in the clipping geometry
#outputdirectory<- specify the output directory
###################################################################

require(rgdal)
require(terra)
require(tools)
#########################################
##---Required Inputs---------############
rasterdir<-"S:\\GCMC\\Data\\Greenness\\NDVI\\focalstats_270m\\NewEngland"
clippinggeometry<- "S:\\GCMC\\Data\\Massachusetts\\statewide_viewer_fgdb\\MassGIS_Vector_GISDATA.gdb"
clippinglayer<- "OUTLINE_POLY"
outputdirectory<- "S:\\GCMC\\tmp\\NDVI_270m"
outputname1<- "MA_LS_ndvi30m_19840101-20221001"
#########################################
##---- Clipping Boundary
clippingpoly<-vect(readOGR(dsn=clippinggeometry,layer=clippinglayer))

##---- Get unique Years
#years<-  unique(sapply(X = strsplit(list.dirs(path = rasterdir,full.names = FALSE,recursive = FALSE),"-"),FUN = function(x){substr(x[10],1,nchar(x[10])-2)})) # Pattern for NO2
#years<-  unique(sapply(X = strsplit(list.dirs(path = rasterdir,full.names = FALSE,recursive = FALSE),"-"),FUN = function(x){substr(x[10],1,nchar(x[10])-2)})) #NDVI 270m

##---- Read in the raster data paths
#for(y in years){
#print("processing year: ")
#print(y)

#allRasters<- list.files(path = rasterdir,pattern = paste(".+",y,"[0-1][0-9][0-3][0-9].tif$",sep=""),all.files = TRUE,full.names = TRUE,recursive = TRUE,include.dirs = FALSE) # Pattern for NO2
allRasters<- list.files(path = rasterdir,pattern = ".tif$",all.files = TRUE,full.names = TRUE,recursive = TRUE,include.dirs = FALSE) # Pattern for NDVI270m

print(paste("the number of rasters in this year: ",as.character(length(allRasters)),sep=""))

#---- Determine Raster Dates
#dates<-unique(sapply(X = strsplit(file_path_sans_ext(basename(allRasters)),"_"),FUN = function(x){substr(x[3],5,nchar(x[3]))})) # Pattern for N02
dates<-unique(sapply(X = strsplit(file_path_sans_ext(basename(allRasters)),"_"),FUN = function(x){x[3]})) # NDVI 270m
dates<-dates[order(dates)]
head(dates)
allRasters<-allRasters[order(dates)]

##---- Create Raster Stack
rstack<- rast(allRasters)
names(rstack) <- dates
crs(rstack,proj = TRUE)
crs(clippingpoly,proj = TRUE)

##---- Reproject Rasters to appropriate CRS
rstack<- project(x = rstack,crs(clippingpoly))
crs(rstack) == crs(clippingpoly)
##---- Clip Rasters to Boundary
clipstack<-crop(x = rstack, y = clippingpoly, )

writeRaster(
  x = clipstack,
  #filename = file.path(outputdirectory,paste(outputname1,y,".tif",sep="")), 
  filename = file.path(outputdirectory,paste(outputname1,".tif",sep="")),
  names = names(clipstack))
#}
