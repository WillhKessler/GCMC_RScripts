

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
npnts<-100 #number of observations in sample
npartic<-34
reps<- 1 # how many replicates of this dataset
firststartdate<- "2012-01-01" # Earliest starting observation
# startdate<- "1984-01-01"
# enddate<- "2022-12-31"
laststartdate<- "2022-12-31" # Last starting observation date, if this is equal to 'firststartdate'
period<- "random" #Number of consecutive days in observation period. Use the string 'random' for uniform random number up to 2 years, alternatively, enter an integer

outputdir<- "S:/GCMC/_Code/TESTING_datasets/"
outputname<-"VITAL_toycohort"
format<-"csv"
IDfield<- 'subject_ID'
xfield<-'x'
yfield<-'y'
startdatefield<-"start_date"
enddatefield<-"stop_date"


###################################################################################################################
## ToY FUNCTIONS
generateCohort<- function(npnts= npnts, 
                          npartic = npartic,
                          startdate=firststartdate,
                          enddate=firstenddate,
                          outputdir,
                          outputname, 
                          IDfield="UUID",
                          yfield='latitude',
                          xfield='longitude',
                          startdatefield='start_date',
                          enddatefield='end_date',
                          format = "RDS"){

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
  
}else{
  rand_startdates <- sample(seq(as.Date(startdate),as.Date(enddate),by="day"), num_dates, replace = TRUE)
  rand_enddates<-rand_startdates
}

# Create UUIDs
if (npnts==npartic){
  UUIDs <-1:npnts
}else{
  UUIDs<-sample(1:npartic,npnts,replace=TRUE)
}
# Create "cohort" by adding date columns to point data
cohort<-data.frame(UUID= UUIDs , latitude = points@coords[,"y"], longitude = points@coords[,"x"], start_date = rand_startdates, end_date = rand_enddates)

# assign fieldnames
colnames(cohort)<-c(IDfield, yfield, xfield,startdatefield,enddatefield)



## Write Files to Directory
filenames <- list.files(outputdir,pattern = format)
if(length(filenames) ==0){num = 0}else{num <- max(readr::parse_number(basename(filenames)))}

if (format == "RDS"){
  saveRDS(cohort, file = paste(outputdir,"/",outputname,num+1,".RDS",sep=""))
  }else if (format == "csv"){
    write.csv(x = cohort,file = paste(outputdir,"/",outputname,num+1,".csv",sep=""),row.names = FALSE)
  }
}



sapply(1:reps,function(x){generateCohort(
  npnts= npnts, 
  npartic = npartic,
  startdate=firststartdate,
  enddate=laststartdate,
  outputdir=outputdir,
  outputname=outputname, 
  IDfield=IDfield,
  yfield=yfield,
  xfield=xfield,
  startdatefield=startdatefield,
  enddatefield=enddatefield, 
  format = "csv")})
