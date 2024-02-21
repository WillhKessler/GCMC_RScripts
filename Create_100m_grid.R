require(terra)
require(sf)

us<-vect('S:/GCMC/Data/AdminBoundaries/CensusGeometry_2020.gdb',layer="States")
us<-project(us,crs("ESRI:102003"))
us<-us[us$DIVISION !=0,]
#us_extent<-ext(us)


## Create a large US scaled polygons

center <- cbind(-90, 45) |> vect(crs="+proj=longlat")
cprj <- crds(project(center, us))
res<-100000

e <- rep(cprj, each=2) + c(-res, res) / 2 
x <- rast(ext(e), crs=crs(us), ncol=1, nrow=1)
x <- extend(x, us, snap="out")

xpolys<-as.polygons(x)
xpolys<-xpolys[us,]


plot(xpolys,border="blue")
lines(us,col="red")

#Create 100m subpolygons in each grid
test<-vect()
nrow(xpolys)
for( p in 1:nrow(xpolys)){
  #intersecting states
  states<-us[xpolys[p,],]
  
  rp<-rast(xpolys[p,],res=(100))
  v<-as.polygons(rp)
  v<-v[states,]
  writeVector(x=v,filename=paste("S:/GCMC/Data/BuiltEnvironment/NETS/100m_Grid/",paste0("gridchunk_",p,".shp"),sep=""),overwrite=TRUE)
}





       