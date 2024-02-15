
#############-----------READ ME---------########################################
#This script will Clip  one or more rasters to an AOI defined by a shapefile or featureclass 
##### Required Inputs:
# rasterdir- A directory contining one or more rasters to be clipped. Script is currently limited to *.tif
# clippinggeometry- A path to a shapefile or Geodatabase. If given as GDB, specify the layer name
# clippinglayer- name of the layer in the clipping geometry
#outputdirectory<- specify the output directory
###################################################################
require(batchtools)
require(rgdal)
require(terra)
require(tools)
#reg=loadRegistry(file.dir="/home/wik191/.batch_registry",writeable=TRUE)
#reg = makeRegistry(file.dir = '/home/wik191/batch_registry', seed = 1)
#reg$cluster.fuctions=makeClusterFunctionsMulticore()
#########################################
##---Required Inputs---------############
rasterdir<- "S:\\GCMC\\Data\\Climate\\PRISM"
clippinggeometry<- "S:\\GCMC\\Data\\Massachusetts\\statewide_viewer_fgdb\\MassGIS_Vector_GISDATA.gdb"
clippinglayer<- "OUTLINE_POLY"
outputdirectory<- "S:\\GCMC\\tmp\\PRISM_Daily"
outputname1<- "MA_PRISM_" 
#########################################
vars = list.dirs(path = rasterdir,full.names = FALSE,recursive = FALSE)

clip.prism<-function(vars){
  require(batchtools)
  require(rgdal)
  require(terra)
  require(tools)
  #########################################
  ##---Required Inputs---------############
  rasterdir<- "S:\\GCMC\\Data\\Climate\\PRISM"
  clippinggeometry<- "S:\\GCMC\\Data\\Massachusetts\\statewide_viewer_fgdb\\MassGIS_Vector_GISDATA.gdb"
  clippinglayer<- "OUTLINE_POLY"
  outputdirectory<- "S:\\GCMC\\tmp\\PRISM_Daily"
  outputname1<- "MA_PRISM_" 
  
  ##---- Clipping Boundary
  clippingpoly<-vect(readOGR(dsn=clippinggeometry,layer=clippinglayer))
  crs(clippingpoly)<- "EPSG:26986"
  
  for(v in vars){ 
    rdates <- list.dirs(path=file.path(rasterdir,v,"daily"),full.names = FALSE,recursive = FALSE)
    for (d in rdates){
      ##---- Read in the raster data paths
      allRasters<- list.files(path = file.path(rasterdir,v,"daily",d) ,pattern = "*.bil$",all.files = TRUE,full.names = TRUE,recursive = TRUE,include.dirs = FALSE)
      head(allRasters)
      
      #---- Determine Raster Dates
      dates<-unique(sapply(X = strsplit(file_path_sans_ext(basename(allRasters)),"_"),FUN = function(x){x[5]}))
      dates<-dates[order(dates)]
      head(dates)
      allRasters<-allRasters[order(dates)]
      
      ##---- Create Raster Stack
      rstack<- rast(allRasters)
      names(rstack) <- dates
      crs(rstack,proj = TRUE)
      
      ##---- Reproject Clipping Polygon to appropriate CRS
      crs(clippingpoly,proj = TRUE)
      clippingpoly<-terra::project(x=clippingpoly,crs(rstack))
      crs(rstack) == crs(clippingpoly)
      
      ##---- Clip Rasters to Boundary
      clipstack<-crop(x = rstack, y = clippingpoly, )
      
      print(paste("trying to write raster: ", v,"_",d,".tif",sep = ""))
      writeRaster(
        x = clipstack,
        filename = file.path(outputdirectory,paste(outputname1,v,"_",d,".tif",sep = "")),
        names = names(clipstack),overwrite=TRUE)
      print(paste("Finished writing raster: ",v,"_",d,".tif",sep=""))
    }
  }
}

##---- Setup Jobs
clearRegistry(reg)
batchMap(fun = clip.prism,
         vars = list.dirs(path = rasterdir,full.names = FALSE,recursive = FALSE),
          reg = reg)
getJobTable()
getStatus()

##---- Submit the tasks to the registry
batchtools::submitJobs()
getStatus()

