####
#Use this code snippet to pull census variables for the 
# Add key to .Renviron
Sys.setenv(CENSUS_KEY="1a3746f7563b180e81219f7b1df9f6f293df6898")
# Reload .Renviron
readRenviron("~/.Renviron")
# Check to see that the expected key is output in your R console
Sys.getenv("CENSUS_KEY")

require(censusapi)
require(plyr)
####################################################################
surveyname<- "dec/sf2"
year = 2000
surveynames<-c("acs/acs5/profile","dec/sf3profile",)
#years<-c(1980, 1990,seq(2000,2023,1))

allnames<- listCensusApis()
# Limit APIs to specific group of surveys and year
allnames<-allnames[grep("acs/",allnames$name),]
allnames<-allnames[allnames$vintage ==2010,]
#View Metadata 
tablemeta<-listCensusMetadata(name = "dec/sf2",vintage = 2010)
outputdirectory <- "S:/GCMC/Data/Census"
####################################################################

# # Generate a list of all available APIs
# allnames<- listCensusApis()
# # Limit APIs to specific group of surveys and year
# allnames<-allnames[grep("dec",allnames$name) & allnames$vintage ==2000,]

# 
# #Generate list of variables to pull
varlist<-makeVarlist(name = surveyname,find = "Profile",varsearch = "concept",vintage = 2000)
# 
# #Make call to census API
data<-getCensus(
  name = surveyname,
  vintage = 2000,
  vars = varlist,
  region = "zip code tabulation area:*"
  )

# # Download ACS data 
# sf3meta<-       listCensusMetadata(name = surveyname,vintage = 2012)
# surveyMetaData<- listCensusMetadata(name = surveyname,vintage = 2012)
# census_to_df<-function(yr){
#   tryCatch(
#     {
#       surveyMetaData<- listCensusMetadata(name = surveyname, vintage = yr)
#       varlist<-makeVarlist(name = surveyname,find = "*",varsearch = "concept",vintage = yr)[-c(1:3)]
#       data<-getCensus(
#         name = surveyname,
#         vintage = yr,
#         vars = varlist,
#         region = "zip code tabulation area:*"
#       )
#       return(data)
#     }
#   )
# }
# 
# output<-lapply(years,census_to_df)
# Write output to CSV
write.csv(data,file = paste(outputdirectory,"/",surveyname,"_",year,".csv",sep=""),row.names = FALSE,col.names = TRUE,na = "")

