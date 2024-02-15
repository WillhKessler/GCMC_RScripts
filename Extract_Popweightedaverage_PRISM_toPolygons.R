

require(terra)
require(rgdal)

#Climate Rasters
rastfiles<-"S:\\GCMC\\Data\\Climate\\PRISM"
vars<-list.files("S:\\GCMC\\Data\\Climate\\PRISM",full.names=FALSE)

#Population Rasters for spatial weights
population<-c("S:\\GCMC\\Data\\Population\\WorldPop\\USA\\usa_m_65_2020_constrained.tif",
              "S:\\GCMC\\Data\\Population\\WorldPop\\USA\\usa_m_70_2020_constrained.tif",
              "S:\\GCMC\\Data\\Population\\WorldPop\\USA\\usa_m_75_2020_constrained.tif",
              "S:\\GCMC\\Data\\Population\\WorldPop\\USA\\usa_m_80_2020_constrained.tif")

#Boston Metro Counties
counties<-readOGR(dsn="S:\\GCMC\\Data\\AdminBoundaries\\CensusGeometry_2020.gdb",layer="Counties")
Bostoncounties<- counties[counties$GEOID %in% c("25009", "25017","25021","25023","25025"),]
Bostoncounties<-vect(Bostoncounties)

#Boston Metro ZCTAs
polygons<-"S:\\GCMC\\Data\\AdminBoundaries\\CensusGeometry_2020.gdb"
polygons<-readOGR(dsn=polygons,layer="ZCTA")
polygons<-vect(polygons)
polygons<-polygons[Bostoncounties]


poprast<- rast(population)

## Reproject everything to the same resolution and CRS
crs(poprast,proj=TRUE)
crs(polygons,proj=TRUE)
climvar<-list.files(file.path(rastfiles,vars[1],"daily"),pattern = paste(".*",vars[1],".*[1-2][0-9][0-9][0-9][0-2][0-9][0-3][0-9].bil$",sep=""),all.files = TRUE,full.names=TRUE,recursive = TRUE,include.dirs = FALSE)[1]
crs(rast(climvar),proj=TRUE)

if (crs(poprast,proj=TRUE) == crs(polygons,proj=TRUE)){
  poprast<-crop(poprast,ext(polygons))
  }else{
      polygons<-terra::project(polygons,poprast)
      poprast<-crop(poprast,ext(polygons))
      
  }

if (crs(poprast,proj=TRUE) == crs(polygons,proj=TRUE) &&
    crs(poprast,proj=TRUE) == crs(rast(climvar),proj=TRUE)){
  print("all good")
}else{
  polygons<-terra::project(polygons,rast(climvar))
  poprast<-terra::project(poprast,rast(climvar),method='sum',align=TRUE) 
}



## Create a composite population raster at the same crs and extent of the climate variables
poprast2<-sum(poprast)


# Loop through ZCTAs, append weighted zonal statistics

for (v in vars){
 
  rasters<-rast(list.files(file.path(rastfiles,v,"daily"),pattern = paste(".*",v,".*[1-2][0-9][0-9][0-9][0-2][0-9][0-3][0-9].bil$",sep=""),all.files = TRUE,full.names=TRUE,recursive = TRUE,include.dirs = FALSE))
  #rasters<-rast(list.files(file.path(rastfiles,v,"daily"),pattern = paste(".*",v,".*(?:19|20)\d\d(?:0[1-9]|1[0-2])(?:0[1-9]|[12][0-9]|3[01]).bil$",sep=""),all.files = TRUE,full.names=TRUE,recursive = TRUE,include.dirs = FALSE))
  
  output<-data.frame()
  for(i in 1:length(polygons)){
    p<-polygons[i]

    popzone = crop(x= poprast2,y= p, mask=TRUE)
    #Scale the population weights to sum to 1
    weights = popzone*(1/sum(values(popzone,na.rm=TRUE)))
    weights<-extend(weights,rasters,fill=NA)
    weightedavg<-zonal(x=rasters,z=p,w=weights, fun = mean)
    output<-rbind(c(p$ZCTA5CE10,weightedavg))

  }
write.csv(output,file=file.path("C:\\Users\\wik191\\OneDrive - Harvard University\\Projects\\Ernani- PRISM_ZCTAs",paste(v,"_","ZCTA","_","POPwavg",".csv")))
}
    
    
    