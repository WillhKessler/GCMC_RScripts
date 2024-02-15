
# Creating a new branch on github
#made an edit on branch createGitHubBranch-onlocal from local
#made an edit on github
require(terra)
require(rgdal)

rasterdir<- "S:\\GCMC\\tmp\\PRISM_Daily"
files<-list.files(path = rasterdir ,pattern = "*.tif$",all.files = TRUE,full.names = TRUE,recursive = TRUE,include.dirs = FALSE)
head(files)
testrast<-rast(files[1])
plot(testrast)
crs(testrast,proj=TRUE)
terra::nlyr(testrast)

clippinggeometry<- "S:\\GCMC\\Data\\Massachusetts\\statewide_viewer_fgdb\\MassGIS_Vector_GISDATA.gdb"
clippinglayer<- "OUTLINE_POLY"
clippingpoly<-vect(readOGR(dsn=clippinggeometry,layer=clippinglayer) )

crs(clippingpoly,proj=TRUE)

for(r in files){
  raster<-rast(r)
  raster<-terra::project(x=raster,crs(clippingpoly))  
  crs(raster,proj = TRUE)==crs(clippingpoly,proj = TRUE)
  print(paste("trying to write raster: ", r,sep = ""))
  writeRaster(
    x = raster,
    filename = r,
    names = names(raster),overwrite=TRUE)
  print(paste("Finished writing raster: ",r,sep=""))
  
}
