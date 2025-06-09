require('terra')
require(tools)
require(utils)
#########################################
##---Download state data from Google Drive---------############
var="EVI"
rasterdir<- "S:\\GCMC\\Data\\Greenness\\EVI\\states"
outdir1<- "S:\\GCMC\\Data\\Greenness\\EVI"
sourceraster<-rast("S:\\GCMC\\Data\\Greenness\\EVI\\30m/EVI_30_2023-10-01.tif")

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
aggregate( V1~V3,rasterdf, function(x) length(unique(x)))






reso<- unique(sapply(rasters2,FUN=function(x) x[2]))
dates<- unique(file_path_sans_ext(unique(sapply(rasters2,FUN=function(x) gsub(pattern = "-\\d{10}\\-\\d{10}","",x[3] )))))

patterncombos<-expand.grid(reso,dates)
patterns<-paste0(patterncombos$Var1,"_",patterncombos$Var2)

for(pattern in patterns){
  if(grepl('^30',pattern)){
    outdir = file.path(outdir1,"30m")
  }else if(grepl('^270',pattern)){
      outdir = file.path(outdir1,"fs270m")
  }else if(grepl('^1230',pattern)){
        outdir = file.path(outdir1,"fs1230m")}

  temp<- mosaic(sprc(localrasters[grep(pattern,localrasters)]),
         fun="mean")
  
  temp<-clamp(temp,lower=-1,upper=1,values=F)
  
  if(!compareGeom(temp,sourceraster,stopOnError=F)){
  resample(x=temp,y=sourceraster, method= 'bilinear',threads=T,filename = file.path(
    outdir,
    paste0(var,"_",pattern,".tif")
  ),
  overwrite=T,gdal=c("COMPRSS=LZW"))
  }else{writeRaster(temp,
                    filename = file.path(
                      outdir,
                      paste0(var,"_",pattern,".tif")
                    )
  )
    }
}

