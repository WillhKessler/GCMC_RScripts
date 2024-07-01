
require(tools)

##Constants
allRegions <-c("Arizona", "ArkansasLouisiana", "CaliPart1", "CaliPart2", "Colorado","Florida", "Idaho", "Illinois", "IndianaOhio", 
               "Kansas", "KentuckyTennessee", "Michigan", "Minnesota", "MississippiAlabama", "MissouriIowa", "MontanaPart1", "MontanaPart2",
               "Nebraska", "Nevada",   "NewMexico", "NewEngland", "NorthCarolina1","NorthCarolina2","NorthSouthDakota", 
               "Oklahoma", "SouthCarolinaGeorgia", "Texas1","Texas2","Texas3","Utah",  
               "WashingtonOregon", "Wisconsin","Wyoming", "MiddleAtlantic", "SouthAtlantic1")
allRegions<- allRegions[order(allRegions)]
allDays<-c("01")
allMonths<-c("01","04","07","10")
allYears<-c("1985","1986","1987","1988","1989",
            "1990","1991","1992","1993","1994","1995","1996","1997","1998","1999",
            "2000","2001","2002","2003","2004","2005","2006","2007","2008","2009",
            "2010","2011","2012","2013","2014","2015","2016","2017","2018","2019",
            "2020","2021","2022","2023")
allDates<-apply(expand.grid(allYears,allMonths,allDays),1,paste,collapse="-")
allDates<-allDates[order(allDates)]



## Input Directory
greennessDir<- "S:/GCMC/Data/Greenness/NDVI/30m/"

#Recursively list all paths to TIFF rasters in the directory
allFilePaths<- list.files(path = greennessDir,pattern = "*.tif$",all.files = TRUE,full.names = TRUE,recursive = TRUE,include.dirs = FALSE)
head(allFilePaths)



#Get file names of rasters from path
allFiles<-file_path_sans_ext(basename(allFilePaths))
head(allFiles)

## Return Unique Combinations of:
#Year
years<-unique(sapply(
  X = strsplit(allFiles,"_"),
  FUN = function(x){
    strsplit(x[length(x)],"-")[[1]][1]
    
}))
#Season
dates<-unique(sapply(
  X = strsplit(allFiles,"_"),
  FUN = function(x){
    substring(x[length(x)],regexpr("-",x[length(x)])+1)
    
  }))
#Year and Season
dates<-unique(sapply(X = strsplit(allFiles,"_"),FUN = function(x){x[2]}))
dates<-dates[order(dates)]
#Region
region<-unique(sapply(X =strsplit(allFiles,"_"),FUN = function(x){x[1]})
)
region<-region[order(region)]

datecounts<-list()
for (date in dates){
  count<-sum(grepl(date,allFilePaths))
  datecounts[date]=count
  }
datecounts

regioncounts<-list()
for (r in region){
  count<-sum(grepl(r,allFilePaths))
  regioncounts[r]=count
}
regioncounts


##Check all season/region combinations
rasterchecklistdf<-setNames(data.frame(matrix(ncol=length(allDates),nrow=length(allRegions)),row.names = allRegions),allDates)
row_names<-rownames(rasterchecklistdf)
col_names<-colnames(rasterchecklistdf)

#Fill the raster checklist to find what is missing
for (r in row_names){
  for (c in col_names){
    if (any(grepl(r,allFilePaths) & grepl(c,allFilePaths))){
      rasterchecklistdf[r,c]=1
    }
    
  }
}

# List incomplete rows
rasterchecklistdf[!complete.cases(rasterchecklistdf),]
out<-rasterchecklistdf
out<-out[!complete.cases(out),unique(which(is.na(out),arr.ind = TRUE)[,2])]
print("The following are missnamed: ")
print(region[!(region %in% allRegions)])
print("The following regions are missing one or more NDVI tiles: ")
out<-out[do.call(order,c(out,na.last=FALSE)),]
print(out)
View(out)
