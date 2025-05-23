require('batchtools')
require('tidyr')

##---- Load Registry
reg<- loadRegistry('VITAL_NDVI_Registry')

##---- Create Jobs Table
jobs<-getJobPars(reg=reg)

##---- Combine all the outputs into a dataframe
results<- do.call("rbind",lapply(1:nrow(jobs),loadResult))

# Loop through all unique variables , writing results to file
for(v in as.character(unique(unlist(results[,1])))){
  # Load and unwrap the spatial vector results
  out<- lapply(results[as.character(unlist(results[,1]))==v,3],terra::unwrap)

  # Convert results to list of dataframes 
  out2<- lapply(out,terra::as.data.frame)

  # Reduce all outputs to a dataframe
  out3<-Reduce(function(dtf1,dtf2){merge(dtf1,dtf2,all=TRUE)},out2)

  # Convert to LONG format
  longout<- lapply(out2,function(x){as.data.frame(x%>% pivot_longer(cols= colnames(x)[grepl("^\\d{4}\\-?\\d{2}\\-?\\d{2}\\b",colnames(x))],names_to = "date",values_to = v))})
  longout<-do.call("rbind",longout)

  # Write tabular outputs: wide, long, RDS
  write.csv(longout,paste("VITAL_NDVIDAILY_LONG",v,".csv",sep=""))
  write.csv(out3,paste("VITAL_NDVIDAILY_",v,".csv",sep=""))
  saveRDS(out3,file=paste("VITAL_NDVIDAILY_",v,".rds",sep=""))
}
