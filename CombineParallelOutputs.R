require('batchtools')
require('tidyr')

##---- Load Registry
reg<- loadRegistry('VITAL_NDVI_Registry')

##---- Create Jobs Table
jobs<-getJobPars(reg=reg)
jobpars<-as.data.frame(lapply(data.frame(do.call(rbind,jobs$job.pars)),unlist))
jobpars2<-jobpars

jobpars2[]<-lapply(jobpars2[],as.character)


##---- Combine all the outputs into a dataframe

results<- do.call("rbind",lapply(1:nrow(jobs),loadResult))
#results<- do.call("rbind",lapply(1:20,loadResult))

for(v in as.character(unique(unlist(results[,1])))){
  out<- lapply(results[as.character(unlist(results[,1]))==v,3],terra::unwrap)
  out2<- lapply(out,terra::as.data.frame)
  out3<-Reduce(function(dtf1,dtf2){merge(dtf1,dtf2,all=TRUE)},out2)
  longout<-lapply(out2,function(x){reshape2::melt(melt(x,id.vars=colnames(x)[!grepl("^\\d{4}\\-\\d{2}\\-\\d{2}\\b",colnames(x))],
  variable.names="date",value.name=v,na.rm=FALSE))})
  longout<- lapply(out2,function(x){as.data.frame(x%>% pivot_longer(cols= colnames(x)[grepl("^\\d{4}\\-\\d{2}\\-\\d{2}\\b",colnames(x))],names_to = "date",values_to = v))})
  #out<- do.call(as.data.frame,out)
  #out<-bind_rows(out)
  #out<-do.call("rbind",out)
  #out<- do.call("rbind",results[as.character(unlist(results[,1]))==v,3])
  longout<-do.call("rbind",longout)
  write.csv(longout,paste("VITAL_NDVIDAILY_LONG",v,".csv",sep=""))
  write.csv(out3,paste("VITAL_NDVIDAILY_",v,".csv",sep=""))
  saveRDS(out3,file=paste("VITAL_NDVIDAILY_",v,".rds",sep=""))
}
