
require(tools)
require(dplyr)
#########################################
## ## Required
inputdirectory<-"/pc/n3mhs00/Cindy/For_WilliamK/tmp"
outputdirectory<- "/pc/n3mhs00/Cindy/For_WilliamK/"



allFilePaths<- list.files(path = inputdirectory,pattern = "*.csv$",all.files = TRUE,full.names = TRUE,recursive = TRUE,include.dirs = FALSE)
prismVars<-unique(sapply(X = strsplit(file_path_sans_ext(basename(allFilePaths)),"_"),FUN = function(x){x[3]}))



for (pvar in prismVars){
  
  # varname<- strsplit(subdir,"/")[[1]][length(strsplit(subdir,"/")[[1]])]
  
  varFiles<- allFilePaths[grep(pvar,allFilePaths)]
  head(varFiles)
  cohorts<- lapply(varFiles,function(path){read.csv(path,stringsAsFactors = FALSE,header = TRUE)})
  cohorts<-lapply(cohorts,function(df){
    colnames(df)[ncol(df)]<- pvar 
    return (df)})
  cohort<-do.call(rbind,cohorts)
  
  assign(paste("cdf","_",pvar,sep=""),cohort)
}

Pattern1<-grep("^cdf",names(.GlobalEnv),value=TRUE)
Pattern1_list<-do.call("list",mget(Pattern1))

final<- Reduce(function(x, y) merge(x, y, by = c("uid","Id","longitude","latitude","start_date","end_date"), all = TRUE), Pattern1_list)


write.csv(x = final,file = paste(outputdirectory,"GPSdata_PRISM.csv"),row.names = FALSE)


