##---- An example Function
get_random_mean <- function(mu, sigma, ...){
  stim<-Sys.time()
  x <- rnorm(100, mean = mu, sd = sigma)
  out<- c(sample_mean = mean(x), sample_sd = sd(x), Node=system("hostname", intern=TRUE),
                  Rversion=paste(R.Version()[6:7], collapse="."),starttime=stim,endtime=Sys.time())
}


##---- An example Function
myFct <- function(cpucore) {
  Sys.sleep(10) # to see job in queue, pause for 10 sec
  result <- cbind(iris[cpucore, 1:4],
                  Node=system("hostname", intern=TRUE),
                  Rversion=paste(R.Version()[6:7], collapse="."))
  return(result)
}


##---- An example inner Parallel Function
innerParallel <- function(cpu){
  
  #Must supply inner function inside the outer function
  myFct <- function(cpucore) {
    stim<-Sys.time() # logging clock time shows that the inner function is called at the same time across all cores, not sequentially
    Sys.sleep(10) # to see job in queue, pause for 10 sec
    etim<-Sys.time()
    result <- cbind(iris[cpucore, 1:4,],
                    Node=system("hostname", intern=TRUE),
                    Rversion=paste(R.Version()[6:7], collapse="."),
                    start = stim,
                    end = etim)
    return(result)
  }
  
  parallelMap::parallelMap(myFct,1:cpu)
}


# Function to extract Raster Data to points or polygons weighted/unweighted based on other rasters
##---- REQUIRED INPUTS ----##
#PROJECT_NAME<-"Bellavia_polygon_LINKAGE" # string with a project name
#rasterdir<- "/pc/nhair0a/Built_Environment/BE_Data/Geographic_Data/PRISM_daily/PRISM_data/an" # string with a file path to raster covariates to extract- function will try to pull variable names from sub directories i.e /PRISM/ppt or /PRISM/tmean or /NDVI/30m
#extractionlayer = "/d/tmp/nhairs/nhair0a/BellaviaLinkage/sites_10M/sites_10M.shp" # string with path to spatial layer to use for extraction. Can be a CSV or SHP or GDB 
#layername = "sites_10M" # Layer name used when extraction layer is an SHP or GDB
#IDfield<-"ORIG_FID" # Field in extraction layer specifying IDs for features, can be unique or not, used to chunk up batch jobs
#Xfield<- "X"
#Yfield<- "Y"
#startdatefield = "start_date" # Field in extraction layer specifying first date of observations
#enddatefield = "end_date" # Field in extraction layer specifying last date of observations
#predays = 0 # Integer specifying how many days preceding 'startingdatefield' to extract data. i.e. 365 will mean data extraction will begin 1 year before startdatefield
#weights = NA # string specifying file path to raster weights, should only be used when extraction layer is a polygon layer
extract.rast= function(vars,piece,rasterdir,extractionlayer,layername,IDfield,Xfield,Yfield,startdatefield,enddatefield,predays=0,weightslayers = NA){
  
  ##---- Load required packages, needs to be inside function for batch jobs
  require(terra)
  #require(sp)
  require(tools)
  require(ids)
  print(system("hostname",intern=TRUE))
  print(paste('Current working directory:',getwd()))
  print(paste('Current temp directory:',tempdir()))
  
  ##---- Climate Rasters
  rastfiles<-rasterdir
  climvars<-list.files(file.path(rastfiles,vars),pattern = paste(".*",vars,".*[1-2][0-9][0-9][0-9]-?[0-1][0-9]-?[0-3][0-9]\\.(tif|bil)$",sep=""),recursive=TRUE,full.names=TRUE)
  # Determine unique raster dates
  rdates<-unique(sapply(X = strsplit(file_path_sans_ext(basename(climvars)),"_"),FUN = function(x){x[length(x)]}))
  rdates<-rdates[order(rdates)]
  print(rdates)
  
  
  ##---- Extraction Features Layer
  if(file_ext(extractionlayer)=='csv'){
    extlayer<-read.csv(extractionlayer,stringsAsFactors = FALSE)
    extlayer<-extlayer[extlayer[IDfield]==piece,]
    polygons<- vect(x = extlayer,geom = c(Xfield,Yfield), keepgeom=TRUE)
  }else if (file_ext(extractionlayer) %in% c("gdb")){
    polygons<-vect(x=extractionlayer,layer = layername,query = paste("SELECT * FROM ",layername," WHERE ",IDfield," = ",piece))  
  }else if (file_ext(extractionlayer) %in% c("shp")){
    polygons<-vect(x=extractionlayer, query = paste0("SELECT * FROM ",layername," WHERE ",IDfield," = ","'",as.character(piece),"'"))
  }
  polygons$extract_start<- as.character(as.Date(unlist(as.data.frame(polygons[,startdatefield])),tryFormats=c("%m/%d/%Y","%Y%m%d","%Y/%m/%d"))-predays)
  polygons$stop_date<-as.character(as.Date(unlist(as.data.frame(polygons[,enddatefield])),tryFormats=c("%m/%d/%Y","%Y%m%d","%Y/%m/%d")))
  
  
  ##---- Create extraction date ranges for points
  polygonstartSeasonIndex<- sapply(polygons$extract_start, function(i) which((as.Date(rdates,tryFormats = c("%Y-%m-%d","%Y%m%d"))-as.Date(i)) <= 0)[which.min(abs(as.Date(rdates,tryFormats = c("%Y-%m-%d","%Y%m%d"))-as.Date(i))[(as.Date(rdates,tryFormats = c("%Y-%m-%d","%Y%m%d"))-as.Date(i)) <= 0])])
  polygonsendSeasonIndex<- sapply(polygons$stop_date, function(i) which((as.Date(rdates,tryFormats = c("%Y-%m-%d","%Y%m%d"))-as.Date(i)) <= 0)[which.min(abs(as.Date(rdates,tryFormats = c("%Y-%m-%d","%Y%m%d"))-as.Date(i))[(as.Date(rdates,tryFormats = c("%Y-%m-%d","%Y%m%d"))-as.Date(i)) <= 0])])
  polygons$first_extract<-as.Date(rdates[polygonstartSeasonIndex],tryFormats=c("%Y-%m-%d","%Y%m%d"))
  polygons$last_extract<-as.Date(rdates[polygonsendSeasonIndex],tryFormats=c("%Y-%m-%d","%Y%m%d"))
  
  
  ##---- Determine which raster dates fall within the data range
  rasterDateRange<-rdates[as.Date(rdates,tryFormats = c("%Y-%m-%d","%Y%m%d"))>=min(polygons$first_extract) & as.Date(rdates,tryFormats = c("%Y-%m-%d","%Y%m%d"))<=max(polygons$last_extract)]
  # Load Climate Rasters
  print("loading the climvars to rast()")
  climvars2<-sapply(rasterDateRange, function(x){climvars[grep(x,climvars)]})
  rasters<- rast(climvars2)
  names(rasters)<-rasterDateRange
  #################################################################
  #################################################################
  ##---- Weights Rasters for spatial weights
  calc.spatialweights<- function(weightslayers,rasters,polygons){
    rweights<-list.files(weightslayers,full.names = TRUE)
    print(rweights)
    
    ## Rasterize the Weights data
    print('the weights rasters')
    weightrast<- rast(rweights)
    print(weightrast)
    
    ## Reproject everything to the same resolution and CRS
    print('reprojecting clim vars')
    crs(polygons)<-crs(rasters)
    crs(weightrast)<-crs(rasters)
    
    print('cropping weightrasters')
    weightrast<-crop(weightrast,polygons,snap="out")
    
    ## Create a composite population raster at the same crs and extent of the climate variables
    weightrast2<-sum(weightrast)
    print(weightrast2)
    
    # Crop and resample climate rasters to weights
    print("croppings rasters with weightrast2") 
    rasters2<-crop(rasters, weightrast2,snap='out')
    print("resampling rasters2")
    print("the tempdir(): ")
    print(tempdir())
    print("the current working directory")
    print(getwd())
    
    print("starting resample")
    rasters2<-resample(rasters2,weightrast2,method='bilinear',wopt=list(gdal=c("BIGTIFF=YES")))
    output<-data.frame()
    print('cropping the weightrast2 to polygon')  
    weightzone = crop(x= weightrast2,y= polygons, touches=FALSE,mask=TRUE)
    
    #Scale the population weights to sum to 1
    print('scaling the population weights')
    weights = weightzone*(1/sum(values(weightzone,na.rm=TRUE)))
    weights<-extend(weights,rasters2,fill=NA)
    weightedavg<-zonal(x=rasters2,z=polygons,w=weights, fun = mean,na.rm=TRUE)
    
    print("the weights average: ")
    print(weightedavg)
    output<-rbind(c(values(polygons[,IDfield]),weightedavg))
    return (output)
  }
  
  #################################################################
  #################################################################
  
  ##---- Perform Extractions
  if(is.polygons(polygons)){
    if(is.na(weightslayers)){
      rasters2<- crop(x = rasters, y = polygons,snap = 'out')
      tempoutput<-zonal(x=rasters2,z=polygons,fun=mean,na.rm=TRUE)
      output<-rbind(c(values(polygons[,IDfield]),tempoutput))
    }else{output<-calc.spatialweights(weightslayers= weightslayers,rasters= rasters,polygons= polygons)}
  }else if(is.points(polygons)){
    output<-extract(x = rasters,y = polygons,ID=FALSE)
    names(output)<-names(rasters)
    output<-cbind(polygons,output)
    
    "Do the extract for points" 
  }  
  
  #return(list(exposure=vars,piece=piece,result=output,node = system("hostname",intern=TRUE), Rversion = paste(R.Version()[6:7],collapse=".") ))
  return(list(exposure=vars,piece=piece,result=wrap(output),node = system("hostname",intern=TRUE), Rversion = paste(R.Version()[6:7],collapse=".") ))

}

