##---- Check SLURM partitions
get.partitions<- function(partition){
  ## Check if supplied partition name is in the list of available partitions
  if(partition %in% trimws(system("sinfo -O PartitionName",intern=TRUE)[-1])){
    return(TRUE)
  }else{
    return(FALSE)
  }
}





##---- Load Required Packages
load.packages<- function(){
  listOfPackages <- c("batchtools","terra","tools","reshape2","ids","lubridate")

  for (i in listOfPackages){
    if(! i %in% installed.packages()){
      install.packages(i, dependencies = TRUE)
    }
    require(i,character.only=TRUE)
  }
}





##---- Create a temporary registry item
set.parallel.registry = function(PROJECT_NAME){
  if(file.exists(paste(PROJECT_NAME,"Registry",sep="_"))){
    reg = loadRegistry(paste(PROJECT_NAME,"Registry",sep="_"),writeable = TRUE)
  }else{
    reg = makeRegistry(file.dir = paste(PROJECT_NAME,"Registry",sep="_"), seed = 42)
  }
  return(reg)
}





##---- Select and Set Cluster Function Settings
select.Cluster = function(projectdirectory=getwd(),projectname,scheduler="socket"){
  #setwd(projectdirectory)
  load.packages()
  reg = set.parallel.registry(projectname)
  if (scheduler=="SLURM"){
    if(!file.exists("slurm.tmpl")){
      download.file("https://raw.githubusercontent.com/WillhKessler/GCMC_RScripts/main/slurm.tmpl","slurm.tmpl")
    }else{
      print("template exists")
    }
    if(!file.exists("batchtools.conf.R")){
      download.file("https://raw.githubusercontent.com/WillhKessler/GCMC_RScripts/main/batchtools.conf.R", "batchtools.conf.R")
    }else{
      print("conf file exists")
    }
  }
  else if (scheduler == "socket"){
    reg$cluster.functions=makeClusterFunctionsSocket()
  }
  else if (scheduler == "interactive"){
    reg$cluster.functions=makeClusterFunctionsInteractive()
  }
return(reg)
}





##---- Create batchgrid
create.jobgrid = function(rasterdir,extractionlayer,layername,IDfield,Xfield,Yfield,startdatefield,enddatefield,predays,period,weightslayers){
  require("tools")
  
  ##---- Set up the batch processing jobs
  pvars = list.dirs(path = rasterdir,full.names = FALSE,recursive = FALSE)
  
  if(file_ext(extractionlayer)=="csv"){
    feature<-read.csv(extractionlayer,stringsAsFactors = FALSE)
    feature$OID<-1:nrow(feature)
    write.csv(x = feature,file = paste0(file_path_sans_ext(extractionlayer),"_tmp",".csv"),row.names = FALSE)
    feature<-feature$OID
    layername = NA
    weightslayers = NA
    extractionlayer<-paste0(file_path_sans_ext(extractionlayer),"_tmp",".csv")
    IDfield="OID"
  }else if(file_ext(extractionlayer) %in% c("shp","gdb")){
    require('terra')
    vectorfile<- vect(x=extractionlayer,layer=layername)
    vectorfile$OID<-1:nrow(vectorfile)
    writeVector(x = vectorfile,filename = paste0(file_path_sans_ext(extractionlayer),"_tmp.",file_ext(extractionlayer)),layer=layername,overwrite=TRUE)
    feature<- unlist(unique(values(vectorfile[,"OID"])))
    Xfield = NA
    Yfield = NA
    extractionlayer<-paste0(file_path_sans_ext(extractionlayer),"_tmp.",file_ext(extractionlayer))
    IDfield="OID"
    if (file_ext(extractionlayer)=="shp"){
      layername<-paste0(extractionlayer,"_tmp")
    }
  }
  
  output<- expand.grid(vars = pvars,
                       period=period,
                       piece = feature,
                       rasterdir = rasterdir,
                       extractionlayer = extractionlayer,
                       layername = layername,
                       IDfield = IDfield,
                       Xfield = Xfield,
                       Yfield = Yfield,
                       startdatefield = startdatefield,
                       enddatefield = enddatefield,
                       predays = predays,
                       weightslayers = weightslayers,
                       stringsAsFactors = FALSE)
  return(output)
}





##---- Initialize submission of jobs to cluster
init.jobs = function(func = extract.rast, rasterdir, extractionlayer, layername, IDfield, Xfield, Yfield, 
                     startdatefield, enddatefield, period, predays,weightslayers, chunk.size = 1000,
                     memory = 2048,partition="linux01", projectdirectory, projectname, scheduler = "socket", email, reg){
  
# init.jobs = function(func = extract.rast,rasterdir = rasterdir,extractionlayer = extractionlayer,layername = layername,IDfield = IDfield,Xfield = Xfield,
#                     Yfield = Yfield,startdatefield = startdatefield,enddatefield = enddatefield,predays = predays,weightslayers = weights,chunk.size = 1000,
#                     memory = 2048,partition="linux01", projectdirectory = projectdirectory,projectname=PROJECT_NAME, scheduler = "interactive",email=email,reg=reg){
  ##---- Clear the R registry
  clearRegistry(reg)
  
  ##---- Create jobs
  ##----  create jobs from variable grid
  jobs<- batchMap(fun = func,
                  create.jobgrid(rasterdir = rasterdir,
                                 extractionlayer = extractionlayer,
                                 layername = layername,
                                 IDfield = IDfield,
                                 Xfield = Xfield,
                                 Yfield = Yfield,
                                 startdatefield = startdatefield,
                                 enddatefield = enddatefield,
                                 predays = predays,
                                 weightslayers = weights),
                  reg = reg)
  
  setJobNames(jobs,paste(abbreviate(PROJECT_NAME),jobs$job.id,sep=""),reg=reg)
  jobs$chunk <- chunk(jobs$job.id, chunk.size = chunk.size)
  
  
  getJobTable()
  getStatus()
  
  ##---- Submit Jobs
  if(toupper(scheduler) == "SLURM"){
    if(partition == "linux12h"){walltime<- 43200}else{walltime=36000000}
    done <- batchtools::submitJobs(jobs, 
                                   reg=reg, 
                                   resources=list(partition=partition, walltime=walltime, ntasks=1, ncpus=1, memory=memory,email=email))
  }else if(toupper(scheduler)=="SOCKET"){
  done<- batchtools::submitJobs(jobs,resources = list(memory=memory),reg = reg)
  }else{
    done<- batchtools::submitJobs(resources = c(walltime=3600000000, memory=memory),reg = reg)
    }
  waitForJobs()
  
  # If any of the jobs failed, they will be displayed here as 'Errors"
  getStatus()
  
  # Look at the Error Messages to see what the errors are:
  getErrorMessages()
}





##---- Helper function for adjusting extraction periods when averaging time periods
set.period<- function(polygons,period){
if(period=="monthly"){
    polygons$extract_start<- as.character(floor_date(as.Date(unlist(as.data.frame(polygons[,startdatefield])),tryFormats=c("%Y-%m-%d","%m/%d/%Y","%Y%m%d","%Y/%m/%d"))-predays,"month"))
    polygons$extract_stop<-as.character(ceiling_date(as.Date(unlist(as.data.frame(polygons[,enddatefield])),tryFormats=c("%Y-%m-%d","%m/%d/%Y","%Y%m%d","%Y/%m/%d")),"month")-1)
  }else if(period=="yearly"){
    polygons$extract_start<- as.character(floor_date(as.Date(unlist(as.data.frame(polygons[,startdatefield])),tryFormats=c("%Y-%m-%d","%m/%d/%Y","%Y%m%d","%Y/%m/%d"))-predays,"year"))
    polygons$extract_stop<-as.character(ceiling_date(as.Date(unlist(as.data.frame(polygons[,enddatefield])),tryFormats=c("%Y-%m-%d","%m/%d/%Y","%Y%m%d","%Y/%m/%d")),"year")-1)
  }else{
  polygons$extract_start<- as.character(as.Date(unlist(as.data.frame(polygons[,startdatefield])),tryFormats=c("%Y-%m-%d","%m/%d/%Y","%Y%m%d","%Y/%m/%d"))-predays)
  polygons$extract_stop<-as.character(as.Date(unlist(as.data.frame(polygons[,enddatefield])),tryFormats=c("%Y-%m-%d","%m/%d/%Y","%Y%m%d","%Y/%m/%d")))
  }
  return(polygons)
}





##---- Helper function for setting calculating spatial weights 
 calc.spatialweights<- function(weightslayers,rasters,polygons){
    rweights<-list.files(weightslayers,full.names = TRUE)
    print(rweights)
    
    ## Rasterize the Weights data
    print('the weights rasters')
    weightrast<- rast(rweights)
    print(weightrast)
    
    ## Reproject everything to the same resolution and CRS
    print('reprojecting clim vars')
    polygons<-project(polygons,crs(rasters))
    #crs(weightrast)<-crs(rasters)
    weightrast<-project(weightrast,rasters)
    
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
    rasters2<-resample(rasters2,weightrast2,method='bilinear',wopt=list(gdal = c("BIGTIFF=YES")))
    output<-data.frame()
    print('cropping the weightrast2 to polygon')  
    weightzone = crop(x= weightrast2,y= polygons, touches=FALSE,mask=TRUE)
    
    #Scale the population weights to sum to 1
    print('scaling the population weights')
    weights = weightzone*(1/sum(values(weightzone,na.rm=TRUE)))
    weights<-extend(weights,rasters2,fill=NA)
    weightedavg<-zonal(x=rasters2,z=polygons,w=weights, fun = mean,na.rm=TRUE,as.polygons=TRUE)
    print(str(weightedavg))
    print("the weights average: ")
    #print(weightedavg)
    output<-cbind(polygons,weightedavg)
    print(str(output))
    #output<-weightedavg
    return(output)
  }





##---- Function to extract Raster Data to points or polygons weighted/unweighted based on other rasters
extract.rast= function(vars,period,piece,rasterdir,extractionlayer,layername,IDfield,Xfield,Yfield,startdatefield,enddatefield,predays=0,weightslayers = NA){
  
  ##---- Load required packages, needs to be inside function for batch jobs
  require(terra)
  require(reshape2)
  require(tools)
  require(ids)
  require(lubridate)
  source("https://raw.githubusercontent.com/WillhKessler/GCMC_RScripts/refs/heads/main/Functions_RasterExtraction.R")
  print(system("hostname",intern=TRUE))
  print(paste('Current working directory:',getwd()))
  print(paste('Current temp directory:',tempdir()))
  
  ##---- Climate Rasters
  rastfiles<-rasterdir
  climvars<-list.files(file.path(rastfiles,vars),pattern = paste(".*",vars,".*[1-2][0-9][0-9][0-9]-?[0-1][0-9]-?[0-3][0-9]\\.(tif|bil)$",sep=""),recursive=TRUE,full.names=TRUE)
  # Determine unique raster dates
  rdates<-unique(sapply(X = strsplit(file_path_sans_ext(basename(climvars)),"_"),FUN = function(x){x[length(x)]}))
  rdates<-rdates[order(rdates)]
  #print(rdates)
  
  
  ##---- Extraction Features Layer
  if(file_ext(extractionlayer)=='csv'){
    extlayer<-read.csv(extractionlayer,stringsAsFactors = FALSE)
    extlayer<-extlayer[extlayer[IDfield]==piece,]
    polygons<- vect(x = extlayer,geom = c(Xfield,Yfield),crs="EPSG:4326",keepgeom=TRUE)
    rm(extlayer)
  }else if (file_ext(extractionlayer) %in% c("gdb")){
    polygons<-vect(x=extractionlayer,layer = layername,query = paste("SELECT * FROM ",layername," WHERE ",IDfield," = ",piece))  
  }else if (file_ext(extractionlayer) %in% c("shp")){
    polygons<-vect(x=extractionlayer, query = paste0("SELECT * FROM ",layername," WHERE ",IDfield," = ","'",as.character(piece),"'"))
  }
  
  ##---- Set summary period for extractions (monthly, yearly)
  polygons<-set.period(polygons,period)
  
  ##---- Create extraction date ranges for points
  polygonstartSeasonIndex<- sapply(polygons$extract_start, function(i) which((as.Date(rdates,tryFormats = c("%Y-%m-%d","%Y%m%d"))-as.Date(i)) <= 0)[which.min(abs(as.Date(rdates,tryFormats = c("%Y-%m-%d","%Y%m%d"))-as.Date(i))[(as.Date(rdates,tryFormats = c("%Y-%m-%d","%Y%m%d"))-as.Date(i)) <= 0])])
  polygonsendSeasonIndex<- sapply(polygons$extract_stop, function(i) which((as.Date(rdates,tryFormats = c("%Y-%m-%d","%Y%m%d"))-as.Date(i)) <= 0)[which.min(abs(as.Date(rdates,tryFormats = c("%Y-%m-%d","%Y%m%d"))-as.Date(i))[(as.Date(rdates,tryFormats = c("%Y-%m-%d","%Y%m%d"))-as.Date(i)) <= 0])])
  
  ##---- Handle cases where extraction dates are outside available data
  if(length(unlist(polygonstartSeasonIndex))==0 & length(unlist(polygonsendSeasonIndex))==0){
    output<- polygons
    longoutput<-reshape2::melt(as.data.frame(output),id.vars=names(polygons),variable.names="date",value.name=vars,na.rm=FALSE)
    return(list(exposure=vars,piece=piece,result=wrap(output),longresult=longoutput,node = system("hostname",intern=TRUE), Rversion = paste(R.Version()[7:8],collapse=".") ))
    } else if (length(unlist(polygonstartSeasonIndex))==0 & length(unlist(polygonsendSeasonIndex))>0){
      polygonstartSeasonIndex<- sapply(polygons$extract_start, function(i) which.min(as.Date(rdates,tryFormats = c("%Y-%m-%d","%Y%m%d"))))
    }else{
  }

  polygons$first_extract<-as.Date(rdates[polygonstartSeasonIndex],tryFormats=c("%Y-%m-%d","%Y%m%d"))
  polygons$last_extract<-as.Date(rdates[polygonsendSeasonIndex],tryFormats=c("%Y-%m-%d","%Y%m%d"))
  
  ##---- Perform Extractions
  ##---- Determine which raster dates fall within the data range
  rasterDateRange<-rdates[as.Date(rdates,tryFormats = c("%Y-%m-%d","%Y%m%d"))>=min(polygons$first_extract) & as.Date(rdates,tryFormats = c("%Y-%m-%d","%Y%m%d"))<=max(polygons$last_extract)]
                                       
  # Load Climate Rasters
  print("loading the climvars to rast()")
  climvars2<-sapply(rasterDateRange, function(x){climvars[grep(x,climvars)]})
  rasters<- rast(climvars2)
  names(rasters)<-rasterDateRange
  if(is.polygons(polygons)){
    if(is.na(weightslayers)){
      polygons<-project(polygons,crs(rasters))
      rasters2<- crop(x = rasters, y = polygons,snap = 'out')
      tempoutput<-zonal(x=rasters2,z=polygons,fun=mean,na.rm=TRUE,as.polygons=TRUE)
      tempnames<-names(tempoutput)
      
      output<-cbind(polygons,tempoutput)
      longoutput<-reshape2::melt(as.data.frame(output),id.vars=names(polygons),variable.names="date",value.name=vars,na.rm=FALSE)
      
    }else{
      output<-calc.spatialweights(weightslayers= weightslayers,rasters= rasters,polygons= polygons)
      }
  }else if(is.points(polygons)){
    polygons<-project(polygons,crs(rasters))
    output<-terra::extract(x = rasters,y = polygons,ID=FALSE)
    names(output)<-names(rasters)
    
  } 
  
  row_average_function <- function(row_data,timeperiod) {
    tapply(as.numeric(row_data), timeperiod, mean)
  }
  
  # Calculate period averages
  if(period == "monthly"){
      timeperiod<-substr(colnames(output),1,6)
      monthlyaverages<-t(apply(output,1,row_average_function))
      output<-cbind(polygons,as.data.frame(monthlyaverages))
      longoutput<-reshape2::melt(as.data.frame(output),id.vars=names(polygons),variable.names="date",value.name=vars,na.rm=FALSE)
      
    }else if(period == "yearly"){
      timeperiod<-substr(colnames(output),1,4)
      yearaverages<-t(apply(output,1,row_average_function))
      output<-cbind(polygons,as.data.frame(yearaverage))
      longoutput<-reshape2::melt(as.data.frame(output),id.vars=names(polygons),variable.names="date",value.name=vars,na.rm=FALSE)
    }else{
      output<-cbind(polygons,output)
      longoutput<-reshape2::melt(as.data.frame(output),id.vars=names(polygons),variable.names="date",value.name=vars,na.rm=FALSE)
    }
    

  
  #return(list(exposure=vars,piece=piece,result=output,node = system("hostname",intern=TRUE), Rversion = paste(R.Version()[6:7],collapse=".") ))
  return(list(exposure=vars,piece=piece,result=wrap(output),longresult=longoutput,node = system("hostname",intern=TRUE), Rversion = paste(R.Version()[7:8],collapse=".") ))

}




                                       
##---- Function to perform time invariant raster data extraction to points or polygons with or without raster weighting
simple.extract.rast= function(vars,piece,rasterdir,extractionlayer,layername,IDfield,Xfield,Yfield,startdatefield,enddatefield,predays=0,weightslayers = NA){
  
  ##---- Load required packages, needs to be inside function for batch jobs
  require(terra)
  require(reshape2)
  require(tools)
  require(ids)
  print(system("hostname",intern=TRUE))
  print(paste('Current working directory:',getwd()))
  print(paste('Current temp directory:',tempdir()))
  
  ##---- Climate Rasters
  rastfiles<-rasterdir
  climvars<-list.files(file.path(rastfiles,vars),pattern = paste(".*",vars,".*.(tif|bil)$",sep=""),recursive=TRUE,full.names=TRUE)
  
  
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
  
  
  rasters<- rast(climvars)
  names(rasters)<-basename(file_path_sans_ext(climvars))
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
    polygons<-project(polygons,crs(rasters))
    #crs(weightrast)<-crs(rasters)
    weightrast<-project(weightrast,rasters)
    
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
    rasters2<-resample(rasters2,weightrast2,method='bilinear',wopt=list(gdal = c("BIGTIFF=YES")))
    output<-data.frame()
    print('cropping the weightrast2 to polygon')  
    weightzone = crop(x= weightrast2,y= polygons, touches=FALSE,mask=TRUE)
    
    #Scale the population weights to sum to 1
    print('scaling the population weights')
    weights = weightzone*(1/sum(values(weightzone,na.rm=TRUE)))
    weights<-extend(weights,rasters2,fill=NA)
    weightedavg<-zonal(x=rasters2,z=polygons,w=weights, fun = mean,na.rm=TRUE,as.polygons=TRUE)
    print(str(weightedavg))
    print("the weights average: ")
    #print(weightedavg)
    output<-cbind(polygons,weightedavg)
    print(str(output))
    #output<-weightedavg
    return(output)
  }
  
  #################################################################
  #################################################################
  
  ##---- Perform Extractions
  if(is.polygons(polygons)){
    if(is.na(weightslayers)){
      polygons<-project(polygons,crs(rasters))
      rasters2<- crop(x = rasters, y = polygons,snap = 'out')
      tempoutput<-zonal(x=rasters2,z=polygons,fun=mean,na.rm=TRUE,as.polygons=TRUE)
      tempnames<-names(tempoutput)
      
      output<-cbind(polygons,tempoutput)
      longoutput<-reshape2::melt(as.data.frame(output),id.vars=names(polygons),variable.names="variable",value.name=vars,na.rm=FALSE)
      
    }else{output<-calc.spatialweights(weightslayers= weightslayers,rasters= rasters,polygons= polygons)}
  }else if(is.points(polygons)){
    polygons<-project(polygons,crs(rasters))
    output<-extract(x = rasters,y = polygons,ID=FALSE)
    names(output)<-names(rasters)
    output<-cbind(polygons,output)
    longoutput<-reshape2::melt(as.data.frame(output),id.vars=names(polygons),variable.names="variable",value.name=vars,na.rm=FALSE)
    
    
  }  
  
  #return(list(exposure=vars,piece=piece,result=output,node = system("hostname",intern=TRUE), Rversion = paste(R.Version()[6:7],collapse=".") ))
  return(list(exposure=vars,piece=piece,result=wrap(output),longresult=longoutput,node = system("hostname",intern=TRUE), Rversion = paste(R.Version()[7:8],collapse=".") ))
  
}





##---- An example inner Parallel Function
p.extract.rast <- function(pieces,vars,rasterdir,extractionlayer,layername,IDfield,Xfield,Yfield,startdatefield,enddatefield,predays=0,weightslayers = NA){
  multicore.extract.rast<-function(pieces2,vars,rasterdir,extractionlayer,layername,IDfield,Xfield,Yfield,startdatefield,enddatefield,predays=0,weightslayers = NA){
    ##---- Load required packages, needs to be inside function for batch jobs
    require(terra)
    require(reshape2)
    require(tools)
    require(ids)
    require(lubridate)
    source("https://raw.githubusercontent.com/WillhKessler/GCMC_RScripts/refs/heads/main/Functions_RasterExtraction.R")
    print(system("hostname",intern=TRUE))
    print(paste('Current working directory:',getwd()))
    print(paste('Current temp directory:',tempdir()))
    print(pieces2)
    print(vars)
    print(rasterdir)
    print(extractionlayer)
    print(IDfield)
    print(Xfield)
    print(Yfield)
    print(startdatefield)
    print(enddatefield)
    print(predays)
  
    ##---- Climate Rasters
    rastfiles<-rasterdir
    climvars<-list.files(file.path(rastfiles,vars),pattern = paste(".*",vars,".*[1-2][0-9][0-9][0-9]-?[0-1][0-9]-?[0-3][0-9]\\.(tif|bil)$",sep=""),recursive=TRUE,full.names=TRUE)
    # Determine unique raster dates
    rdates<-unique(sapply(X = strsplit(file_path_sans_ext(basename(climvars)),"_"),FUN = function(x){x[length(x)]}))
    rdates<-rdates[order(rdates)]
    
    ##---- Extraction Features Layer
    if(file_ext(extractionlayer)=='csv'){
      extlayer<-read.csv(extractionlayer,stringsAsFactors = FALSE)
      #extlayer<-extlayer[extlayer[IDfield]==piece,]
      extlayer<-extlayer[extlayer[IDfield] == pieces2,]
      polygons<- vect(x = extlayer,geom = c(Xfield,Yfield), crs="EPSG:4326" ,keepgeom=TRUE)
    }else if (file_ext(extractionlayer) %in% c("gdb")){
      polygons<-vect(x=extractionlayer,layer = layername,query = paste("SELECT * FROM ",layername," WHERE ",IDfield," = ",piece))  
    }else if (file_ext(extractionlayer) %in% c("shp")){
      polygons<-vect(x=extractionlayer, query = paste0("SELECT * FROM ",layername," WHERE ",IDfield," = ","'",as.character(piece),"'"))
    }
    polygons$extract_start<- as.character(as.Date(unlist(as.data.frame(polygons[,startdatefield])),tryFormats=c("%Y-%m-%d","%m/%d/%Y","%Y%m%d","%Y/%m/%d"))-predays)
    polygons$extract_stop<-as.character(as.Date(unlist(as.data.frame(polygons[,enddatefield])),tryFormats=c("%Y-%m-%d","%m/%d/%Y","%Y%m%d","%Y/%m/%d")))
    
    
    ##---- Create extraction date ranges for points
    polygonstartSeasonIndex<- sapply(polygons$extract_start, function(i) which((as.Date(rdates,tryFormats = c("%Y-%m-%d","%Y%m%d"))-as.Date(i)) <= 0)[which.min(abs(as.Date(rdates,tryFormats = c("%Y-%m-%d","%Y%m%d"))-as.Date(i))[(as.Date(rdates,tryFormats = c("%Y-%m-%d","%Y%m%d"))-as.Date(i)) <= 0])])
    polygonsendSeasonIndex<- sapply(polygons$extract_stop, function(i) which((as.Date(rdates,tryFormats = c("%Y-%m-%d","%Y%m%d"))-as.Date(i)) <= 0)[which.min(abs(as.Date(rdates,tryFormats = c("%Y-%m-%d","%Y%m%d"))-as.Date(i))[(as.Date(rdates,tryFormats = c("%Y-%m-%d","%Y%m%d"))-as.Date(i)) <= 0])])
    polygons$first_extract<-as.Date(rdates[polygonstartSeasonIndex],tryFormats=c("%Y-%m-%d","%Y%m%d"))
    polygons$last_extract<-as.Date(rdates[polygonsendSeasonIndex],tryFormats=c("%Y-%m-%d","%Y%m%d"))
    
    ##---- Determine which raster dates fall within the data range
    ##---- Determine required rasters
    minrdate<-min(polygons$first_extract)
    maxrdate<-max(polygons$last_extract)
    rdaterange<-rdates[which(as.Date(rdates,tryFormats = c("%Y-%m-%d","%Y%m%d")) %in% as.Date(minrdate:maxrdate))]
    
    ##---- Filter Raster List
    climvars2<-sapply(rdaterange, function(x){climvars[grep(x,climvars)]})
    rm(climvars)
    rdates2<-unique(sapply(X = strsplit(file_path_sans_ext(basename(climvars2)),"_"),FUN = function(x){x[length(x)]}))
    rdates2<-rdates2[order(rdates2)]
    rasterDateRange<-mapply(function(first_extract,last_extract) which(as.Date(rdates2,tryFormats = c("%Y-%m-%d","%Y%m%d")) >=first_extract & as.Date(rdates2,tryFormats = c("%Y-%m-%d","%Y%m%d")) <= last_extract),polygons$first_extract,polygons$last_extract,SIMPLIFY=F)
    
    ##---- Load Climate Rasters
    print("loading the climvars to rast()")
    rasters<- rast(climvars2)
    names(rasters)<-rdaterange
    
    ##---- Reformat polygons
    polygons<-lapply(1:length(polygons),FUN = function(x) polygons[x])
    
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
      polygons<-project(polygons,crs(rasters))
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
      rasters2<-resample(rasters2,weightrast2,method='bilinear',wopt=list(gdal = c("BIGTIFF=YES")))
      output<-data.frame()
      print('cropping the weightrast2 to polygon')  
      weightzone = crop(x= weightrast2,y= polygons, touches=FALSE,mask=TRUE)
      
      #Scale the population weights to sum to 1
      print('scaling the population weights')
      weights = weightzone*(1/sum(values(weightzone,na.rm=TRUE)))
      weights<-extend(weights,rasters2,fill=NA)
      weightedavg<-zonal(x=rasters2,z=polygons,w=weights, fun = mean,na.rm=TRUE,as.polygons=TRUE)
      print(str(weightedavg))
      print("the weights average: ")
      #print(weightedavg)
      output<-cbind(polygons,weightedavg)
      print(str(output))
      #output<-weightedavg
      return(output)
    }
    
    
    #################################################################
    #################################################################
    
    ##---- Perform Extractions
    if(is.polygons(polygons[[1]])){
      if(is.na(weightslayers)){
        rasters2<- crop(x = rasters, y = polygons,snap = 'out')
        tempoutput<-zonal(x=rasters2,z=polygons,fun=mean,na.rm=TRUE,as.polygons=TRUE)
        tempnames<-names(tempoutput)
        
        output<-cbind(polygons,tempoutput)
        longoutput<-reshape2::melt(as.data.frame(output),id.vars=names(polygons),variable.names="date",value.name=vars,na.rm=FALSE)
        
      }else{output<-calc.spatialweights(weightslayers= weightslayers,rasters= rasters,polygons= polygons)}
    }else if(is.points(polygons[[1]])){
      print("performing extraction")
      output<-mapply(function(x,y){extract(rasters[[x]],y,ID=FALSE)},rasterDateRange,polygons)
      output<-mapply(cbind,polygons,output)
      output<-lapply(X = output,as.data.frame) 
      output<-Reduce(function(dtf1,dtf2){merge(dtf1,dtf2,all=TRUE)},output)
      #output<-extract(x = rasters,y = polygons,ID=FALSE)
      #print("assigning names")
      #names(output)<-names(rasters)
      #output<-cbind(polygons,output)
      
      longoutput<-reshape2::melt(as.data.frame(output),id.vars=names(polygons[[1]]),variable.names="date",value.name=vars,na.rm=FALSE)
    }
    jobout<-output
    longout<-longoutput
    
    
    
    
   return(list(exposure=vars,result=jobout,longresult=longout,node = system("hostname",intern=TRUE), Rversion = paste(R.Version()[6:7],collapse=".") ))
  
  
  }
  
  parallelMap::parallelMap(
    fun=multicore.extract.rast,
    pieces,
    more.args = list(vars,rasterdir,extractionlayer,layername,IDfield,Xfield,Yfield,startdatefield,enddatefield,predays,weightslayers))
}





##---- Function to append all Recombine Outputs from Parallelization
combine.results= function(projectname=PROJECT_NAME){
  require('batchtools')
  require('tidyr')
  require('terra')
  
  ##---- Load Registry
  reg<- loadRegistry(paste(PROJECT_NAME,"Registry",sep="_"))
  
  ##---- Create Jobs Table
  jobs<-getJobPars(reg=reg)
  #jobpars<-as.data.frame(lapply(data.frame(do.call(rbind,jobs$job.pars)),unlist))
  #jobpars2<-jobpars

  
  #jobpars2[]<-lapply(jobpars2[],as.character)
  
  
  ##---- Combine all the outputs into a dataframe
  
  results<- do.call("rbind",lapply(1:nrow(jobs),loadResult))
  #results<- do.call("rbind",lapply(1:20,loadResult))
  
  for(v in as.character(unique(unlist(results[,1])))){
    out<- lapply(results[as.character(unlist(results[,1]))==v,3],terra::unwrap)
    out2<- lapply(out,terra::as.data.frame)
    rm(out)
    out3<-Reduce(function(dtf1,dtf2){merge(dtf1,dtf2,all=TRUE)},out2)
  
    #longout<-lapply(out2,function(x){reshape2::melt(melt(x,id.vars=colnames(x)[!grepl("^\\d{4}\\-?\\d{2}\\-?\\d{2}\\b",colnames(x))],
    #                                                     variable.names="date",value.name=v,na.rm=FALSE))})
    longout<- lapply(out2,function(x){as.data.frame(x%>% pivot_longer(cols= colnames(x)[grepl("^\\d{4}\\-?\\d{2}\\-?\\d{2}\\b",colnames(x))],names_to = "date",values_to = v))})
    rm(out2)
    longout<-do.call("rbind",longout)
    write.csv(longout,paste(projectname,"_LONG_",v,".csv",sep=""))
    write.csv(out3,paste(projectname,"_",v,".csv",sep=""))
    saveRDS(out3,file=paste(projectname,"_",v,".rds",sep=""))
  }
  
}
















#################################################################################

##---- An example Function
get_random_mean <- function(mu, sigma, ...){
  stim<-Sys.time()
  Sys.sleep(runif(1)*22)
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

                                  

