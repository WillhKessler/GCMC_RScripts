##---- REQUIRED INPUTS ----##
PROJECT_NAME<-"innerParallelTest" # string with a project name


##---- Required Packages
library(batchtools)
require(terra)
require(tools)

##REQUIRED##
##---- Initialize batchtools configuration files and template
if(!file.exists("slurm.tmpl")){
  download.file("https://raw.githubusercontent.com/WillhKessler/GCMC_RScripts/main/slurm.tmpl","slurm.tmpl")
}else{
  print("template exists")
}
if(!file.exists("batchtools.conf.R")){
  download.file("https://raw.githubusercontent.com/WillhKessler/GCMC_RScripts/main/batchtools.conf.R", "batchtools.conf.R")
}else{
  print("conf file exists")
}

#Create a temporary registry item
if(file.exists(paste(PROJECT_NAME,"Registry",sep="_"))){
  reg = loadRegistry(paste(PROJECT_NAME,"Registry",sep="_"),writeable = TRUE,conf.file="batchtools.conf.R")
}else{
  reg = makeRegistry(file.dir = paste(PROJECT_NAME,"Registry",sep="_"), seed = 42,conf.file="batchtools.conf.R")
}

##########Input PROCESSING HERE####################################################
## Call Desired functions from Functions_RasterExtraction source file
## The desired functions are mapped in creating the jobs via batchMap
source("https://raw.githubusercontent.com/WillhKessler/GCMC_RScripts/main/Functions_RasterExtraction.R")

##############################################################
##---- Set up the batch processing jobs

mcoreParallel<- function(mu,sigma,...){
  # Inner Parallel loop must have access to source functions
  source("https://raw.githubusercontent.com/WillhKessler/GCMC_RScripts/main/Functions_RasterExtraction.R")
  
  parallelMap::parallelMap(get_random_mean,mu,sigma,nrep...)
  
}

##---- Define Jobs
## Each job should consist of one or more variable combinations to ensure parallelMap is utilized to full potential
par_grid <- expand.grid(mu = -5:5, sigma = list(seq(3, 33, 10)), nrep = 1:50)



##----  Make sure registry is empty
clearRegistry(reg)


jobs<-batchMap(fun=mcoreParallel,par_grid)

jobs$chunk<-chunk(jobs$job.id,chunk.size = 200)
setJobNames(jobs,paste(abbreviate(PROJECT_NAME),jobs$job.id,sep=""),reg=reg)

getJobTable()
getStatus()

##---- Submit jobs to scheduler
done <- batchtools::submitJobs(jobs,
                               resources=list(partition="linux12h",
                                              walltime=3600,
                                              ntasks=1,
                                              ncpus=4,
                                              memory=80000,
                                              pm.backend = "multicore"))
getStatus()

waitForJobs() # Wait until jobs are completed



