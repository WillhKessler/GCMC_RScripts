# require(terra)
require(sp)
require(raster)
require(tools)


## Generate all Seasonal periods
##allDays<-c("01")
##allMonths<-c("01","04","07","10")
##allYears<-c("1984","1985","1986","1987","1988","1989","1990","1991","1992",
##            "1993","1994","1995","1996","1997","1998","1999","2000","2001",
##            "2002","2003","2004","2005","2006","2007","2008","2009","2010",
##            "2011","2012","2013","2014","2015","2016","2017","2018")
##allDates<-paste(allYears,allMonths,allDays,sep="-")

##allRegions <-c("MontanaPart1","NC1","NC2","Texas1","Texas2","TX3","CaliPart1","CaliPart2", "MontanaPart2", "WashingtonOregon", "ArkansasLouisiana", 
##               "Oklahoma", "NewMexico", "Arizona", "Colorado", "Utah", "Nevada", "Wyoming", "Idaho", "Florida", "SouthCarolinaGeorgia", 
##               "SouthAtlantic1", "Kansas", "MissouriIowa", "Nebraska", "Minnesota", "NorthSouthDakota", "MississippiAlabama", "KentuckyTennessee", "IndianaOhio", 
##               "Illinois", "Michigan", "Wisconsin", "MiddleAtlantic", "NewEngland")


##Cohort Data
cdata<-read.csv("/pc/n3mhs00/Cindy/For_WilliamK/RawGPSDataRINX.csv",header=TRUE,stringsAsFactors = FALSE)

##Raster Data

allFilePaths<- list.files(path = greennessDir,pattern = "*.tif$",all.files = TRUE,full.names = TRUE,recursive = TRUE,include.dirs = FALSE)
head(allFilePaths)



##----Vectorize Cohort Data

cohort<-vect(cdata,geom=c("longitude","latitude"), crs="EPSG:4326", type="points",what="",keepgeom=TRUE)


cohort<-project(x = cohort, y="epsg:4269")


## Determine which members of the cohort should be extracted from each raster
rasterDate <- c("2023-01-01","2023-04-01","2023-07-01","2023-10-01")

cohort <- c("2023-03-15", "2023-08-10", "2023-09-20", "2023-10-21","2023-11-21")

cohortSeasonIndex<- sapply(cohort, function(i) which((as.Date(rasterDate)-as.Date(i)) < 0)[which.min(abs(as.Date(rasterDate)-as.Date(i))[(as.Date(rasterDate)-as.Date(i)) < 0])])

#Subset the Cohort
for (i in 1:length(allDates)){
  eachraster<- raster(allDates[grep(allDates[i],allFilePaths)])
  subcohort<-cohort[cohortSeasonIndex==i]
  
  subcohort<-extract(x = eachraster,y = subcohort)
  
  
  
}





