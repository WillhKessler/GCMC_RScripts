
library(ncdf4)
library(terra)
library(fields)
library(maps)
library(mapdata)
#library(leaflet)
library(htmlwidgets)


#setwd("/pc/nhair0a/_mock_nhair0a/Raw_Exposure_Data/TMP_DELETED_REGULARLY")
#setwd("B:\\_mock_nhair0a\\Raw_Exposure_Data\\TMP_DELETED_REGULARLY")
setwd("S:\\GCMC\\Data\\AirPollution\\PM10")
#files<-list.files("/pc/nhair0a/_mock_nhair0a/Raw_Exposure_Data/TMP_DELETED_REGULARLY", pattern = "*.nc$",full.names = T)
files<-list.files("S:\\GCMC\\Data\\AirPollution\\PM10", pattern = "*.nc$",full.names = T)
#>#################################################
#> Convenience function to decode the date integer 
#>#################################################
format_date <- function(int_date){
  return(as.Date(paste0(as.integer(int_date / 1000), "-01-01"), format="%Y-%m-%d"))
}

#>#################################################
#> Convenience function to decode the time integer
#>#################################################
format_time <- function(int_time){
  return(sub("(\\d\\d)(\\d\\d)(\\d\\d)", "\\1:\\2:\\3", sprintf("%06.0f",int_time)))
}

###########################################################################################################
#> Assumption buit into M3
EARTH_RADIUS <- 6370000
for(i in files[1:length(files)]){
  
  #> Name of CMAQ netcdf file. 
  #cctm.file <- "HR2DAY_LST_ACONC_EQUATES_v532_12US1_2017.nc"
  cctm.file <- i
  
  #> Open the CCTM file.
  cctm.in <- nc_open(cctm.file)
  
  #> Print information about a netCDF file, including the variables and dimensions it contains.
  #print(cctm.in)
  
  #> Create a list of all model variables in cctm.file. 
  #> CMAQ netcdf files are formatted following I/O API conventions (https://www.cmascenter.org/ioapi/).  I/O API is a data storage library designed specifically for CMAQ data. 
  #> A variable called “TFLAG” will always be the first variable listed in a CMAQ I/O API file, so remove the first element of the list.
  all.mod.names <- unlist(lapply(cctm.in$var, function(var)var$name))[-1]
  all.mod.names
  #> Create a list units for all the model variables in the cctm.file. 
  #> A variable called “TFLAG” will always be the first variable listed in an I/O API file, so remove the first element of the list.
  #> Use gsub to strip out extra spaces.
  all.mod.units <- gsub(" ","",unlist(lapply(cctm.in$var, function(var)var$units))[-1])
  all.mod.units
  
  #> Pull out the time steps and the grid coordinates associated with the data.
  sdate <- ncatt_get(cctm.in, varid=0, attname="SDATE")$value
  sdate
  stime <- ncatt_get(cctm.in, varid=0, attname="STIME")$value
  stime
  tstep <- ncatt_get(cctm.in, varid=0, attname="TSTEP")$value
  tstep
  time.series.length <- cctm.in$dim$TSTEP$len
  time.series.length
  
  # julian day minus one gives days *after* january first
  start.date <- as.Date((sdate %% 1000) - 1, origin = format_date(sdate)) 
  start.date
  # start datetime
  start.date.time <- strptime(paste0(as.character(start.date), " ", 
                                     format_time(stime)),
                              format="%Y-%m-%d %H:%M:%S", tz="GMT")
  # timestep in seconds
  timestep <- eval(parse(text = sub("(\\d\\d)(\\d\\d)(\\d\\d)", 
                                    "\\1*3600+\\2*60+\\3", 
                                    sprintf("%06.0f",tstep))))
  # the time dimension in date format
  date.seq <- start.date.time + timestep * 0:(time.series.length - 1)
  # the time dimension in string format
  format.date.seq <- format.Date(date.seq,"%m/%d/%Y")
  format.date.seq <- format.Date(date.seq,"%Y-%m-%d")
  #> Lambert projected coordinates of the grid cell CENTERS (unit=km).
  #> These are the unique x, y coordinates of the grid cell centers -- NOT the coordinates for every grid cell, since the data are on a regular grid.
  #> You can also use the get.coord.for.dimension() function to extract the grid cell edges by changing the “position” argument.
  ncols <- ncatt_get(cctm.in, varid=0, attname="NCOLS")$value
  nrows <- ncatt_get(cctm.in, varid=0, attname="NROWS")$value
  x.origin <- ncatt_get(cctm.in, varid=0, attname="XORIG")$value
  y.origin <- ncatt_get(cctm.in, varid=0, attname="YORIG")$value
  x.cell.width <- ncatt_get(cctm.in, varid=0, attname="XCELL")$value
  y.cell.width <- ncatt_get(cctm.in, varid=0, attname="YCELL")$value
  #> for the center of each cell
  grid.offset <- 0.5
  #> x-coordinate locations of grid centers
  x.proj.coord <- seq(from = x.origin + grid.offset * x.cell.width, 
                      by = x.cell.width, 
                      length = ncols)
  length(x.proj.coord)
  #[1] 459
  #> y-coordinate locations of grid centers
  y.proj.coord <- seq(from = y.origin + grid.offset * y.cell.width, 
                      by = y.cell.width, 
                      length = nrows)
  length(y.proj.coord)
  #[1] 299
  
  #> Also get the grid cell centers of all of the grid cell with units=meters.  We will use this later when we convert the data to an object raster.
  xy.proj.coord.meters <- as.matrix(expand.grid(x.proj.coord, y.proj.coord))
  colnames(xy.proj.coord.meters) <- c("x", "y")
  dim(xy.proj.coord.meters)
  #[1] 137241      2
  
  #> Convert lambert coordinates for grid cell centers to long/lat
  p.alpha <-  ncatt_get(cctm.in, varid=0, attname="P_ALP")$value
  p.beta <-  ncatt_get(cctm.in, varid=0, attname="P_BET")$value
  p.gamma <-  ncatt_get(cctm.in, varid=0, attname="P_GAM")$value
  y.center <- ncatt_get(cctm.in, varid=0, attname="YCENT")$value
  #> Get the projection string from the file
  lambert.proj.string <- paste0("+proj=lcc +lat_1=", p.alpha, 
                                " +lat_2=", p.beta,
                                " +lat_0=", y.center,
                                " +lon_0=", p.gamma,
                                " +a=", EARTH_RADIUS, 
                                " +b=", EARTH_RADIUS)
  lambert.proj.string
  #[1] "+proj=lcc +lat_1=33 +lat_2=45 +lat_0=40 +lon_0=-97 +a=6370000 +b=6370000"
  lonlat.proj.string <- "+proj=longlat"
  #> Project from Lambert to longitude/latitude (using terra::project)
  long.lat.coord <- as.data.frame(project(xy.proj.coord.meters, 
                                          from = lambert.proj.string,
                                          to = lonlat.proj.string))
  
  
  #> Extract PM25_AVG (daily 24hr LST averaged PM2.5 concentrations for 1/1/2017 - 12/31/2017).
  for(ii in all.mod.names){
    print(paste("Creating rasters:",ii))
    mod.name <- ii
    mod.unit <- all.mod.units[all.mod.names==mod.name]
    mod.array <- ncvar_get(cctm.in,var=mod.name) 
    dim (mod.array)
    
    #> Create raster object using projection information extracted from the I/O API file. 
    # xyz <- data.frame(x=xy.proj.coord.meters[,1],y=xy.proj.coord.meters[,2],z=matrix(mod.annual.avg))
    for(iii in 1:dim(mod.array)[3]){
      print(paste("creating raster date:",format.date.seq[iii]))
      xyz <- data.frame(x=xy.proj.coord.meters[,1],y=xy.proj.coord.meters[,2],z=matrix(mod.array[,,iii]))
      #> Create the raster object from the meter coordinates because they 
      #>   the grid is regularly spaced
      mod.raster <- rast(xyz, crs = lambert.proj.string) # using terra::rast
      #> Now project it on latitude/longitude
      mod.raster.lonlat <- project(mod.raster, lonlat.proj.string) # using terra::project
      names(mod.raster.lonlat)<-format.date.seq[iii]
      dir.create(file.path("S:\\GCMC\\Data\\AirPollution\\PM10",mod.name),showWarnings=F,mode="0777")
      filenamesout<-file.path("S:\\GCMC\\Data\\AirPollution\\PM10",mod.name,paste0("PM10_",mod.name,"_",names(mod.raster.lonlat),".tif"))
      writeRaster(mod.raster.lonlat,filename=filenamesout,gdal=c("COMPRESS=LZW"),overwrite=T)
    }
  }
}



outfiles<-list.files("S:\\GCMC\\Data\\AirPollution\\PM10",pattern="*.tif$",full.names = T,recursive = T)
rast(outfiles)
