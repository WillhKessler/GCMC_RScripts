library("terra")
set.seed(1)

f = system.file("ex/lux.shp", package = "terra")
v = vect(f)
v<-project(v,"EPSG:9822")
v2<-v[c(1:5,7:10),]
n = 3000
r = rast(v, resolution=100,names = "value", vals = rnorm(n ^ 2))
rr = rast(v, resolution=100,names = 1:10,nlyrs=10, vals = rnorm(n ^ 2))
rv = rasterize(v, r, field = seq_len(nrow(v)))
rv2 = rasterize(v2, r, field = seq_len(nrow(v2)))
plot(r)
plot(v, add = TRUE)

# single band raster, vector
system.time({
  zonal_vector = extract(r, v, fun = "mean")
})

# single band raster, rasterized vector
system.time({
  rrr = rasterize(v, r, field = seq_len(nrow(v)))
  zonal_raster = zonal(r, rrr, fun = "mean")
})

# multiband raster, vector
system.time({
  zonal_vector=extract(rr,v,fun="mean")
})

# multiband raster, rasterized vector
system.time({
  rrr = rasterize(v, r, field = seq_len(nrow(v)))
  zonal_raster = zonal(rr, rrr, fun = "mean")
})

# multiband raster, vector with holes
system.time({
  zonal_vector=extract(rr,v2,fun="mean")
})

# multiband raster, rasterized vector with holes
system.time({
  rrr = rasterize(v2, r, field = seq_len(nrow(v2)))
  zonal_raster = zonal(rr, rrr, fun = "mean")
})

#############################################################
tvect<-vect("C:\\Users\\wik191\\OneDrive - Harvard University\\_Projects\\Lee_Jessica-HOPE\\USCensusTracts_2020/USCensusTracts_2020_tmp.shp")
trast<-rast(list.files("S:/GCMC/Data/Climate/PRISM/daily/ppt",pattern="*.bil$",full.names = T,recursive = T)[1:365])
#trast2<-trast[1:365]
tvect<-project(tvect,crs(tvect))
#trast<-rast(tvect, resolution=1000,names = "value", vals = rnorm(n ^ 2))
#trast2<-rast(tvect, resolution=1000,nlyrs=10,names = 1:10, vals = rnorm(n ^ 2))

# single band raster, vector

system.time({
  zonal_vector = extract(trast, tvect, fun = "mean")
})

system.time({
  #rrr = rasterize(tvect, trast, field = seq_len(nrow(tvect)))
  zonal_raster = zonal(trast, tvect, fun = "mean",as.polygons=T)
})

# single band raster, rasterized vector
system.time({
  rrr = rasterize(tvect, trast, field = seq_len(nrow(tvect)))
  zonal_raster = zonal(trast, rrr, fun = "mean")
  zonal_vector<-merge(tvect,zonal_raster,all.x=T,by.x=c('OID_1'),by.y=c('layer'))
})

# single band raster, rasterized vector
system.time({
  rrr = rasterize(tvect, trast, field = "OID_1")
  zonal_raster = zonal(trast, rrr, fun = "mean")
  zonal_vector<-merge(tvect,zonal_raster,all.x=T,by.x=c('OID_1'),by.y=c('layer'))
})

out<-lapply(c(1:5,10,20,100,200,365),function(x){system.time({
  rrr = rasterize(tvect, trast, field = "OID_1")
  zonal_raster = zonal(trast[[seq(x)]], rrr, fun = "mean")
  zonal_vector<-merge(tvect,zonal_raster,all.x=T,by.x=c('OID_1'),by.y=c('OID_1'))
})})
plot(x=c(1:5,10,20,100,200,365),y=unlist(lapply(out,function(x){as.numeric(x[3])})))
     