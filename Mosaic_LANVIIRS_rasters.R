require('terra')
require(tools)
require(utils)
#########################################
##---Download state data from Google Drive---------############
var="VIIRS_LAN"
rasterdir<- "S:\\GCMC\\Data\\BuiltEnvironment\\LANVIIRS\\states/dump"
outdir1<- "S:\\GCMC\\Data\\BuiltEnvironment\\LANVIIRS\\monthly\\"
sourceraster<-rast("S:\\GCMC\\Data\\BuiltEnvironment\\LANVIIRS\\monthly\\VIIRS_LAN_20230101.tif")
# var="NDVI"
# rasterdir<- "S:\\GCMC\\Data\\Greenness\\NDVI\\states"
# outdir1<- "S:\\GCMC\\Data\\Greenness\\NDVI"
# sourceraster<-rast("S:\\GCMC\\Data\\Greenness\\NDVI\\30m/NDVI_30m_2024-01-01.tif")


#rasters<-list.files(path=rasterdir, pattern="*.tif$",full.names=T,recursive=T,include.dirs=F)
#outrasters<-file.path(outdir,basename(rasters))
#file.copy(rasters[21:41],outdir)

#################################################
##--- Mosaic by date and focal stats---------############
localrasterdir<-rasterdir
localrasters<-list.files(path=localrasterdir, pattern="*.tif$",full.names=T,recursive=T,include.dirs=F)
#rasters2<-strsplit(localrasters,"-\\d{10}")
rasters2<-strsplit(localrasters,"_")

rasterdf<-do.call(rbind,rasters2)
aggregate( V1~V2,rasterdf, function(x) length(unique(x)))






#reso<- unique(sapply(rasters2,FUN=function(x) x[1]))
dates<- unique(file_path_sans_ext(unique(sapply(rasters2,FUN=function(x) gsub(pattern = "-\\d{10}\\-\\d{10}","",x[3] )))))

#patterncombos<-expand.grid(reso,dates)
#patterns<-paste0(patterncombos$Var1,"_",patterncombos$Var2)

for(pattern in dates){
 outdir<-outdir1
  temp<- mosaic(sprc(localrasters[grep(pattern,localrasters)]),
                fun="mean")
  
  #temp<-clamp(temp,lower=-1,upper=1,values=F)
  
  if(!compareGeom(temp,sourceraster,stopOnError=F)){
    #print(paste("geoms differ for",pattern))
  resample(x=temp,y=sourceraster, method= 'bilinear',threads=T,filename = file.path(
    outdir,
    paste0(var,"_",gsub("-","",pattern),".tif")),filetype="GTiff",overwrite=T,gdal=c("COMPRESS=LZW")
  )
  }else{writeRaster(temp,
                    filename = file.path(
                      outdir,
                      paste0(var,"_",gsub("-","",pattern),".tif")),filetype="GTiff",overwrite=T,gdal=c("COMPRESS=LZW")
  )
    #print("geoms are the same, all good!")
    
  }
}

