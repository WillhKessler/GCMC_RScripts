require('batchtools')
  require('tidyr')
  require('terra')
  projectname="ExampleLinkage"
  ##---- Load Registry
  reg<- loadRegistry(paste(projectname,"Registry",sep="_"))
  
  ##---- Create Jobs Table
  jobs<-getJobPars(reg=reg)
   
  ##---- Combine all the outputs into a dataframe
  
  results<- do.call("rbind",lapply(1:nrow(jobs),loadResult))
  #results<- do.call("rbind",lapply(1:20,loadResult))
  
  for(v in as.character(unique(unlist(results[,1])))){
    out<- lapply(results[as.character(unlist(results[,1]))==v,2],terra::unwrap)
    out2<- lapply(out,terra::as.data.frame)
    rm(out)
    out3<-Reduce(function(dtf1,dtf2){merge(dtf1,dtf2,all=TRUE)},out2)
  
    longout<- lapply(out2,function(x){as.data.frame(x%>% pivot_longer(cols= colnames(x)[grepl("^\\d{4}\\-?\\d{2}\\-?\\d{2}\\b",colnames(x))],names_to = "date",values_to = v))})
    rm(out2)
    longout<-do.call("rbind",longout)
    write.csv(longout,paste(projectname,"_LONG_",v,".csv",sep=""))
    saveRDS(longout,paste(projectname,"_LONG_",v,".rds",sep=""))
    write.csv(out3,paste(projectname,"_",v,".csv",sep=""))
    saveRDS(out3,file=paste(projectname,"_",v,".rds",sep=""))
  }
