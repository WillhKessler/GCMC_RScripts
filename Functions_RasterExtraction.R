##---- An example Function
get_random_mean <- function(mu, sigma, ...){
  x <- rnorm(100, mean = mu, sd = sigma)
  c(sample_mean = mean(x), sample_sd = sd(x))
}


##---- An example Function
myFct <- function(cpucore) {
  Sys.sleep(10) # to see job in queue, pause for 10 sec
  result <- cbind(iris[cpucore, 1:4,],
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
##---- REQUIRED INPUTS ----##
PROJECT_NAME<-"Bellavia_polygon_LINKAGE" # string with a project name
rasterdir<- "S:/GCMC/Data/Climate/PRISM/daily" # string with a file path to raster covariates to extract- function will try to pull variable names from sub directories i.e /PRISM/ppt or /PRISM/tmean or /NDVI/30m
extractionlayer = "S:/GCMC/_Code/TESTING_datasets/VITAL_toycohort57.csv" # string with path to spatial layer to use for extraction. Can be a CSV or SHP or GDB
layername = NA # Layer name used when extraction layer is a GDB
IDfield<-"subject_ID" # Field in extraction layer specifying IDs for features, can be unique or not, used to chunk up batch jobs
Xfield<- "x"
Yfield<- "y"
startdatefield = "start_date" # Field in extraction layer specifying first date of observations
enddatefield = "stop_date" # Field in extraction layer specifying last date of observations
predays = 365 # Integer specifying how many days preceding 'startingdatefield' to extract data. i.e. 365 will mean data extraction will begin 1 year before startdatefield
weights = "S:/GCMC/Data/Population/WorldPop/USA/usa_ppp_2020_constrained.tif" # string specifying file path to raster weights, should only be used when extraction layer is a polygon layer, or NA

extract.rast= function(vars,pieces,rasterdir,extractionlayer,layername,IDfield,Xfield,Yfield,startdatefield,enddatefield,predays=0,weightslayers = NA){
  
  ##---- Load required packages, needs to be inside function for batch jobs
  require(terra)
  require(tools)
  require(ids)
  print(system("hostname",intern=TRUE))
  print(paste('Current working directory:',getwd()))
  print(paste('Current temp directory:',tempdir()))
  
  ##---- Climate Rasters
  rastfiles<-rasterdir
  climvars<-list.files(file.path(rastfiles,vars),pattern = paste(".*",vars,".*[1-2][0-9][0-9][0-9]-?[0-1][0-9]-?[0-3][0-9]\\.(tif|bil)$",sep=""),recursive=TRUE,full.names=TRUE)
  
  ##---- Load Rasters,set names, define time
  rasters<-rast(climvars)
  names(rasters)<-gsub("^.*_","",names(rasters))
  rtime<-as.Date(names(rasters),try="%Y%m%d")
  time(rasters)<-rtime
  
  ##---- Extraction Features Layer
  if(file_ext(extractionlayer)=='csv'){
    extlayer<-read.csv(extractionlayer,stringsAsFactors = FALSE)
    extlayer<- vect(x = extlayer,geom = c(Xfield,Yfield))
    extlayer<-terra::subset(x = extlayer,extlayer[IDfield] %in% pieces)
  }else if (file_ext(extractionlayer) %in% c("gdb")){
    extlayer<-vect(x=extractionlayer,layer = layername) 
  }else if (file_ext(extractionlayer) %in% c("shp")){
    extlayer<-vect(x=extractionlayer)
  }
  extlayer$extract_start<-as.Date(apply(as.data.frame(extlayer[,startdatefield]),1,function(x){as.character(as.Date(x)-predays)}))
  extlayer$stop_date<-as.Date(apply(as.data.frame(extlayer[,enddatefield]),1,function(x){as.character(as.Date(x))}))
  
  
  
  ##---- Calculate first and last extraction dates
  ## start dates
  featuretartSeasonIndex<- apply(X = as.data.frame(extlayer[,"extract_start"]),
                                 MARGIN = 1,
                                 FUN = function(x){sapply(x, function(i) which((time(rasters)-as.Date(i)) <= 0)[which.min(abs(time(rasters)-as.Date(i))[(time(rasters)-as.Date(i)) <= 0])])}
  )
  
  ## end dates
  featureendSeasonIndex<- apply(X = as.data.frame(extlayer[,"stop_date"]),
                                MARGIN = 1,
                                FUN= function(x){sapply(x, function(i) which((time(rasters)-as.Date(i)) <= 0)[which.min(abs(time(rasters)-as.Date(i))[(time(rasters)-as.Date(i)) <= 0])])}
  )
  
  ## bind to feature layer
  extlayer[,c("first_extract","last_extract")]<-lapply(X = as.data.frame(cbind(featuretartSeasonIndex,featureendSeasonIndex)),
         FUN = function(x){time(rasters)[x]})
  
  ## determine spatRast index position for start and ends
  mstart<-match(extlayer$first_extract,time(rasters))
  mend<-match(extlayer$last_extract,time(rasters))
  
  
  
  
  ##---- Perform the extractions
  
 
   
    
    #################################################################
    #################################################################
    ##---- Weights Rasters for spatial weights
    calc.spatialweights<- function(weightslayers,rasters,extlayer){
      out<-vect()
      for(featurenum in 1:length(extlayer)){
        feature<- extlayer[featurenum,]
        
        ##---- Determine which raster dates fall within the data range
        rsubset<- subset(rasters,time(rasters)>=min(feature$first_extract) & time(rasters)<=max(feature$last_extract))
       
        #rasterDateRange<-rdates[as.Date(rdates,tryFormats = "%Y%m%d")>=feature$first_extract & as.Date(rdates,tryFormats = "%Y%m%d")<=feature$last_extract]
        ##---- Read in weights Rasters
        rweights<-list.files(weightslayers,full.names = TRUE)
        print(rweights)
      
        ## Rasterize the Weights data
        print('the weights rasters')
        weightrast<- rast(rweights)
        print(weightrast)
        
        ## Reproject everything to the same resolution and CRS
        print('reprojecting clim vars')
        crs(feature)<-crs(rsubset)
        crs(weightrast)<-crs(rsubset)
        
        
        #print('cropping weightrasters')
        weightrast<-crop(weightrast,feature,snap="out")
        
        ## Create a composite population raster at the same crs and extent of the climate variables
        weightrast2<-sum(weightrast)
        print(weightrast2)
        
        # Crop and resample climate rasters to weights
        #print("croppings rasters with weightrast2") 
        rsubset2<-crop(rsubset, weightrast2,snap='out')
        #print("resampling rsubset2")
        #print("the tempdir(): ")
        #print(tempdir())
        #print("the current working directory")
        #print(getwd())
        
        #print("starting resample")
        rsubset2<-resample(weightrast2,rsubset2,method='bilinear')
        output<-data.frame()
        # print('cropping the weightrast2 to polygon')  
        weightzone = crop(x= weightrast2,y= feature, touches=FALSE,mask=TRUE)
        
        #Scale the population weights to sum to 1
        # print('scaling the population weights')
        weights = weightzone*(1/sum(values(weightzone,na.rm=TRUE)))
        weights<-extend(weights,rsubset2,fill=NA)
        weightedavg<-zonal(x=rsubset2,z=feature,w=weights, fun = mean,na.rm=TRUE,as.polygons=TRUE)
        
        print("the weights average: ")
        print(weightedavg)
        output<-c(output,weightedavg)
      }
      return(output)
      
    }
    
    #################################################################
    #################################################################
    
    ##---- Perform Extractions
    
    if(is.na(weightslayers)){
        output<-terra::extractRange(x=rasters,
                                    y=extlayer,
                                    first=mstart,
                                    last=mend,
                                    geom_fun=mean,
                                    na.rm=TRUE,
                                    bind=TRUE)
    }else{
      output<-calc.spatialweights(weightslayers= weightslayers,
                                  rsubset= rsubset,
                                  extlayer = extlayer)
      }
     
  return(list(exposure=vars,pieces=pieces,result=output,node = system("hostname",intern=TRUE), Rversion = paste(R.Version()[6:7],collapse=".") ))
  
}

