require('batchtools')

##---- Load Registry
reg<- loadRegistry("Bellavia_NDVI_Registry")

##---- CompletedJobs
jobs<-getJobPars(ids=findDone(),reg=reg)
head(jobs)
##---- Create Jobs Table
#jobs<-getJobPars(reg=reg)
jobpars<-as.data.frame(lapply(data.frame(jobs$job.id,do.call(rbind,jobs$job.pars)),unlist))
head(jobpars)
jobpars2<-jobpars
head(jobpars2)
jobpars2[]<-lapply(jobpars2[],as.character)
head(jobpars2)

##---- Combine all the outputs into a dataframe
#results<- do.call("rbind",lapply(1:nrow(jobs),loadResult))
results<- do.call("rbind",lapply(as.integer(jobpars2$jobs.job.id),loadResult))
head(results)

for(v in as.character(unique(unlist(results[,1])))){
  #out<- lapply(results[as.character(unlist(results[,1]))==v,3],terra::vect)
  out<- do.call("rbind",results[as.character(unlist(results[,1]))==v,3])
  write.csv(out,paste("Bellavia_NDVI_",v,".csv",sep=""))
  #saveRDS(out,file=paste("Bellavia_NDVI_",v,".rds",sep=""))
}
