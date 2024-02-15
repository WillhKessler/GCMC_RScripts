#### R batchtools
library(batchtools)
require(sp)
require(terra)
require(tools)
require(parallel)
require(MASS)

##REQUIRED##
#Create a temporary registry item
reg = makeRegistry(file.dir = NA, seed = 1)
reg$cluster.functions=makeClusterFunctionsSocket()
##############################################################
set.seed(42)

## The function to perform the extraction
extract.rast= function(vars,chunk){
  ##---- Load required packages, needs to be inside function for batch jobs
  require(terra)
  require(sp)
  require(tools)
  
  ##---- Initialize raster list and cohort Point to raster directory
  rasterdir<- "S:\\GCMC\\Data\\AirPollution"
  allRasterPaths<- list.files(path = rasterdir,pattern = paste(".*",vars,".*[0-2][0-9][0-3][0-9][0-1][0-9][0-9][1-9].tif$",sep=""),all.files = TRUE,full.names = TRUE,recursive = TRUE,include.dirs = FALSE)
  cohort<-read.csv("S:\\GCMC\\_Code\\TESTING_datasets\\toyCohort_nurses51.csv")
  
  ##---- Get the min and max start and end date of the entire cohort
  minstart<-min(cohort$start_date)
  maxend<-max(cohort$end_date)
  
  ##---- Split the cohort into the appropriate chunk
  cohort<-split(cohort, rep(1:ceiling(nrow(cohort)/floor(nrow(cohort)/10)), each=floor(nrow(cohort)/10), length.out=nrow(cohort)))
  cohort<- cohort[[chunk]]
  
  ##---- Vectorize Cohort Data, EPSG:4326 = WGS84
  cohort<-wrap(vect(x = cohort, geom = c("longitude","latitude"), crs = "epsg:4326",keepgeom=TRUE))
  cohort<-vect(cohort)
  cohort<-project(cohort,"ESRI:102010") # either ESRI:102010 or epsg:102010 depending on version of terra......
  
  ##---- Determine start and end days 
  #minstart<-min(cohort$start_date)
  #maxend<-max(cohort$end_date)
  
  ##---- Create your output dataset
  outputcohort<-cohort
  
  ##---- Perform the extraction process
  d<-minstart
  while (d<=maxend){
    subcohort<-outputcohort[(d>=outputcohort$start_date & (d) <=outputcohort$end_date),]
    
    if (nrow(subcohort)==0){
      outputcohort<- cbind(outputcohort,data.frame(matrix(NA,nrow(outputcohort),ncol=1)))
    }else{
      print(paste0("n in cohort: ",nrow(subcohort)))
      ##---- Perform the extraction
      if (any(grepl(gsub("-","",d),allRasterPaths))){
        ##Read the rasters
        mrast<-rast(lapply(allRasterPaths[grep(gsub("-","",d),allRasterPaths)],function(path){rast(path)}))
        ##Extract the raster values
        valuematrix<-extract(mrast,subcohort,ID=FALSE)
        ## Add an empty column to the outputcohort
        outputcohort<- cbind(outputcohort,data.frame(matrix(NA,nrow(outputcohort),ncol=1)))
        ## Bind the raster values to the subcohort
        # subcohort<-cbind(subcohort,valuematrix)
        
        ##---- Recombine the cohort
        ##Find the subcohort index in the output
        ind<-match(subcohort$UUID,outputcohort$UUID)
        ## bind the raster values to output
        outputcohort[ind,ncol(outputcohort)]<-valuematrix
        # outputcohort[ind,ncol(outputcohort)]<-subcohort[[daysbetween[d]]]
        print(d)
      }else{
        outputcohort<-cbind(outputcohort,data.frame(matrix(NA,nrow(outputcohort),ncol=1)))
      }
    }
    d=format(as.Date(d)+1,"%Y-%m-%d")
  }
  return(list(exposure=vars,chunk=chunk,result=wrap(outputcohort)))
}
 

##---- Set up the batch processing jobs
pars<- expand.grid(vars=c("PM25","NO3","O3"),chunk=1:10)
#clearRegistry(reg)
batchMap(fun = extract.rast,
         pars,
         reg = reg)
getJobTable()
getStatus()
batchtools::submitJobs()
waitForJobs()

for (e in pars$vars){
  reduce = function(aggr,res,job){
    if (job$pars$vars !=e){
      return(aggr)
    append(aggr,res$result)
    }
  }
  output<-reduceResult(reduce,init=0,reg=reg)
}

regroup<-function(x,y){list(exposure=append(x,y))
}
res<-reduceResults(regroup,reg = reg)
# res<-reduceResultsDataTable()
# res<-cbind(pars,res)


## Comparison for benchmarking
# system.time({
# outputs<-list()
# for (r in 1:nrow(pars)){
#   out<-extract.rast(vars=pars[[r,1]],chunk = pars[[r,2]])
#   outputs<-append(outputs,out)
# }})
