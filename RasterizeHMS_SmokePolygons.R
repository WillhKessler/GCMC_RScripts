require('terra')
require('sf')
## Download NOAA Smoke Polygons

# baseurl<-"https://satepsanone.nesdis.noaa.gov/pub/FIRE/web/HMS/Smoke_Polygons/Shapefile"
# 
# smokedate<-seq.Date(as.Date("2005-08-01"),as.Date("2025-09-30"),by="day")
# for(date in smokedate){
#   year=format(date,"%Y")
#   month=format(date,"%m")
#   basename<-paste0("hms_smoke",format(date,"%Y%m%d"),".zip")
#   fullurl<-file.path(baseurl,year,month,basename)
#   tryCatch({download.file(fullurl,destfile = paste0("S:\\GCMC\\Data\\AirPollution\\WildfireSmoke\\",basename)}error=function(cond){"Error 404"}
#   
# }

vector_layers("S:\\GCMC\\Data\\AirPollution\\WildfireSmoke\\HMS_smoke.gdb")
smoke_polys<-vect("S:\\GCMC\\Data\\AirPollution\\WildfireSmoke\\HMS_smoke.gdb",layer='smoke_poly')
raster<- rast("S:\\GCMC\\Data\\Climate\\PRISM\\daily\\ppt\\2024\\prism_ppt_us_30s_20240101.tif")
values(raster)<-0

days<-as.Date(smoke_polys$Collection_day,"%Y/%m/%d %H:%M:%S+00")
days=seq.Date(from=min(days),to=max(days),by="day")               
               
for(day in 398:length(days)){
  print(days[day])
  smokeday<-smoke_polys[smoke_polys$Collection_day ==days[day]]
  if(length(smokeday)>0){
    smokeday$Density[is.na(smokeday$Density)]<-"DensityUnkown"
    smokecats<-c("DensityUnknown"=1,"Light"=2,"Medium"=3,"Heavy"=4)
    smokeday$presence<-smokecats[smokeday$Density]
    smokeday$presence[is.na(smokeday$presence)]<-1
    smokeday<-project(smokeday,crs(raster))
    smokeday<-makeValid(smokeday)
    tryCatch({
    smokeday<-aggregate(smokeday,by="Density",fun="max")
    smokerast<- rasterize(
      x=smokeday, 
      y=raster,
      fun="max",
      field="max_presence",
      touches=T, 
      background=NA,
      filename=paste0("S:\\GCMC\\Data\\AirPollution\\WildfireSmoke\\new_smokerasters\\","hms_smoke",format(days[day],"%Y%m%d"),".tif"),
      wopt=list(datatype="INT1U",gdal=c("COMPRESS=LZW")),
      overwrite=T
    )
    },error=function(cond){"std::bad_alloc"})
  }else{
      writeRaster(
        raster,
        filename = paste0("S:\\GCMC\\Data\\AirPollution\\WildfireSmoke\\new_smokerasters\\","hms_smoke",format(days[day],"%Y%m%d"),".tif"),wopt=list(datatype="INT1U",gdal=c("COMPRESS=LZW")),overwrite=T)}
}


