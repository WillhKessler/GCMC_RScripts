
#############-----------READ ME---------########################################
#This script will extract Joel Schwartz monthly average PM2.5 values from Daily  geotiff files to RDS file  of nurses cohort 

#Raster data: filename: "YYYYmmdd.tif"
# The code limits only considers members of the cohort where their yearin or yearout are between 2000 and 2016 (where there is data).
# All others are ignored (this is for computational speed)
# The Code pulls "yearin","monthin","yearout","monthout" from features in cohort to determine the encompassing months
# and then subsets the cohort to common time periods to perform the extractions. 
#raster data is stacked my month, extracted and averaged for each subset. the output is merged onto the subcohort and then recombined 
# with the full cohort after all months have been run for a given time period

# The final outputs i.e."write.csv" "saveRDS()" are commented out to prevent overwriting of existing data

###################################################################

require(sp)
require(terra)
require(tools)
require(haven)
#########################################
## ## Required
AirPollutionDir<-"/udd/nhwhk/Daily"
cohortdir<- "/pc/nhair0a/2019_AP_exposures/JoelS/nhs768618_full.sas7bdat"
outputdirectory<- "/pc/nhair0a/2019_AP_exposures/JoelS"
filename<- "nhs768618_JS_PM25"
#########################################


##Read in the Cohort Data
OGcohort<-as.data.frame(read_sas(cohortdir))
cohort<-OGcohort
head(cohort)

##---- Read in the raster data paths
allFilePaths<- list.files(path = AirPollutionDir,pattern = "*.tif$",all.files = TRUE,full.names = TRUE,recursive = TRUE,include.dirs = FALSE)
head(allFilePaths)

#---- Determine Unique Raster Dates
dates<-unique(sapply(X = strsplit(file_path_sans_ext(basename(allFilePaths)),"_"),FUN = function(x){x}))
dates<-dates[order(dates)]
yearmonths<-unique(substr(dates,1,6))

##----Vectorize Cohort Data, EPSG:4326 = WGS84
cohort<-vect(x = cohort, geom = c("nlong","nlat"), crs = "epsg:4326",keepgeom=TRUE)
cohort<-project(cohort,"epsg:102010")

##---- Determine which Season the data occurs
cohort$start_date<-format(as.Date(paste(cohort$yearin,cohort$monthin,"01",sep="-"),"%Y-%m-%d"),"%Y%m")
cohort$end_date<-format(as.Date(paste(cohort$yearout,cohort$monthout,"01",sep="-"),"%Y-%m-%d"),"%Y%m")
head(cohort)

uniqueranges<-unique(cohort[(cohort$yearout %in% 2000:2016) | (cohort$yearin %in% 2000:2016),][[c("start_date","end_date")]])
#uniqueranges<-unique(cohort[[c("start_date","end_date")]])
allmonthsbetween<-apply(X=uniqueranges2,
                        MARGIN =1,
                        FUN=function(x){format(seq(as.Date(paste(x[1],"01",sep=""),"%Y%m%d"), as.Date(paste(x[2],"01",sep=""),"%Y%m%d"), 'months'), format="%Y%m")
                        })
maxlength<-max(lengths(allmonthsbetween))
#cohort$UID<-1:nrow(cohort)

##---- Get Original list of column names in cohort data
outputcohort<-cohort
addmnthcolumns<-unlist(lapply(1:24,function(x){paste0("month",x)}))
outputcohort[,addmnthcolumns]<-NA

for (c in 1:nrow(uniqueranges)){
  print(paste("Starting subCohort: ", c))
  subcohort<-outputcohort[outputcohort$start_date ==uniqueranges[[c,1]] & outputcohort$end_date ==uniqueranges[[c,2]],]
  monthsbetween<-format(seq(as.Date(paste(uniqueranges[[c,1]],"01",sep=""),"%Y%m%d"), as.Date(paste(uniqueranges[[c,2]],"01",sep=""),"%Y%m%d"), 'months'), format="%Y%m")
  print(length(monthsbetween))
  
  for (m in 1:length(monthsbetween)){
    print(paste("beginning month: ",monthsbetween[m]))
    if (monthsbetween[m] > 201612 | monthsbetween[m] < 200001){
      monthmean = rep(NA,length(subcohort))
      subcohort[,match(addmnthcolumns[m],names(subcohort))]<-monthmean
      print(paste("finish month: ",monthsbetween[m]))	
      print.data.frame(head(subcohort))
    }else{
      mrast<-rast(lapply(allFilePaths[grep(monthsbetween[m],allFilePaths)],function(path){rast(path)}))
      valuematrix<-extract(mrast,subcohort,ID=FALSE)
      monthmean<-rowMeans(valuematrix,na.rm = TRUE)
      subcohort[,match(addmnthcolumns[m],names(subcohort))]<-monthmean
      print(paste("finish month: ",monthsbetween[m]))
      print.data.frame(head(subcohort))
    }
  }
  ind<-match(subcohort$key,outputcohort$key)
  outputcohort[ind,addmnthcolumns]<-subcohort[[addmnthcolumns]]
  print(paste("Done with subcohort: ", c, sep=""))
  
}
saveRDS(outputcohort, file = file.path(outputdirectory,paste(filename,".rds",sep="")))
write.csv(outputcohort, file =file.path(outputdirectory,paste(filename,".csv",sep="")) )




# ## Perform the raster Extraction
# for (c in 1:nrow(uniqueranges)){
#   print(paste("Starting subCohort: ", c))
#   subcohort<-cohort[cohort$start_date ==uniqueranges[[c,1]] & cohort$end_date ==uniqueranges[[c,2]],]
#   #subcohort$ID<-1:nrow(subcohort)
#   monthsbetween<-format(seq(as.Date(paste(uniqueranges[[c,1]],"01",sep=""),"%Y%m%d"), as.Date(paste(uniqueranges[[c,2]],"01",sep=""),"%Y%m%d"), 'months'), format="%Y%m")
#   print(length(monthsbetween))
#   PMvalues<-c()
# 
#   for (m in monthsbetween){
#     print(paste("beginning month: ",m))
#     if (m > 201612 | m< 200001){
#       monthmean = rep(NA,length(subcohort))
#       PMvalues<-cbind(PMvalues,monthmean)
#       print(paste("finish month: ",m))	
#     }else{
#         mrast<-rast(lapply(allFilePaths[grep(m,allFilePaths)],function(path){rast(path)}))
#         valuematrix<-extract(mrast,subcohort,ID=FALSE)
#         monthmean<-rowMeans(valuematrix,na.rm = TRUE)
#         PMvalues<-cbind(PMvalues,monthmean)
#         print(paste("finish month: ",m))
#         }
#   }
#   PMvalues<-split(PMvalues,seq(nrow(PMvalues)))
#   print("finished splitting PM values into lists")
#   subcohort$PM25values<-I(PMvalues)
#   ind<-match(subcohort$UID,outputcohort$UID)
#   outputcohort[ind,c("PM25values")]<-subcohort["PM25values"]
#   print("finished binding lists to subcohort df colum")
#  # outputcohort<-merge(x=outputcohort,y=subcohort[,c('UID','PMvalues')], by.x = "UID",by.y="UID", all.x = TRUE)
#  
#   print("finished merging output cohort and subcohort")
#   print(paste("Done with subcohort: ", c, sep=""))
# }
# 
# saveRDS(outputcohort2, file = outputdirectory)
# 
# 
# 


