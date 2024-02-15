

## Create Toy Data of n random points within contiguous US sampled 
##between two dates/observation periods in NAD83 EPSG 4269
##--------------------------------------------##
require(sp)                                   ##
require(spData)                               ##
require(readr)                                ##
set.seed(123)                                 ##
data(us_states)                               ##
##--------------------------------------------##

## INPUTS
npnts<-100
reps<- 1
startdate<- "2005-01-01"
# startdate<- "1984-01-01"
# enddate<- "2022-12-31"
enddate<- "2005-12-31"
#period<- "random" #Number of consecutive days in observation period. alternatively, enter a number
period<- 10 #Number of consecutive days in observation period. alternatively, enter a number

outputdir<- "S:/GCMC/_Code/TESTING_datasets/"
outputname<-"toyCohort_nurses"
format<-"csv" 

## ToY FUNCTIONS
generateCohort<- function(npnts= npnts, startdate=startdate,enddate=enddate,outputdir,outputname, format = "RDS"){

## Generate Points within the specified Geometry
geo <- sf::as_Spatial(us_states[us_states$NAME %in% c("Alaska","Hawaii")==FALSE,][1])
points<- spsample(geo, n=npnts, "random")


# Generate random dates
num_dates <- npnts  # Number of dates to generate
if (period =="random"){
rand_startdates <- sample(seq(as.Date(startdate),as.Date(enddate),by="day"), num_dates, replace = TRUE)
rand_enddates<-rand_startdates+round(rnorm(npnts,mean=runif(1,1,365*2),sd=10))
}else if (is.numeric(period)){
  rand_startdates <- sample(seq(as.Date(startdate),as.Date(enddate),by="day"), num_dates, replace = TRUE)
  rand_enddates<-rand_startdates+period
  #rand_enddates<-sapply(rand_startdates, FUN = function(x){as.Date(x)+period})
}else{rand_startdates <- sample(seq(as.Date(startdate),as.Date(enddate),by="day"), num_dates, replace = TRUE)
rand_enddates<-rand_startdates}

# Create "cohort" by adding date columns to point data
cohort<-data.frame(UUID= 1:npnts , latitude = points@coords[,"y"], longitude = points@coords[,"x"], start_date = rand_startdates, end_date = rand_enddates)



## Write Files to Directory
filenames <- list.files(outputdir,pattern = format)
if(length(filenames) ==0){num = 0}else{num <- max(readr::parse_number(basename(filenames)))}

if (format == "RDS"){
  saveRDS(cohort, file = paste(outputdir,"/",outputname,num+1,".RDS",sep=""))
  }else if (format == "csv"){
    write.csv(x = cohort,file = paste(outputdir,"/",outputname,num+1,".csv",sep=""),row.names = FALSE)
  }
}


# generateCohort(
#   npnts = npnts,
#   startdate = startdate,
#   enddate = enddate,
#   outputdir = outputdir, 
#   outputname = outputname, 
#   format = "csv")

sapply(1:reps,function(x){generateCohort(
  npnts = npnts,
  startdate = startdate,
  enddate = enddate,
  outputdir = outputdir, 
  outputname = outputname, 
  format = "csv")})
