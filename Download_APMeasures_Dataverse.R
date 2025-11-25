
#############-----------READ ME---------########################################
# this script will download files 

###################################################################

#require(sp)
require(dataverse)
require(rgdal)
require(terra)
require(tools)
require(httr2)
require(RCurl)
#########################################
##---Required Inputs---------############
server = 'dataverse.harvard.edu'
PM_traccomponents_doi<-"10.7910/DVN/3H7DNP"

destinationdir<-"S:/GCMC/tmp/pmcomp_temp"

# List all files in Repo
dat<-dataset_files(
  dataset = PM_traccomponents_doi,
  server= server,
)


#Existing Files
success<-basename(list.files(destinationdir,full.name=T,recursive=T))

failed_urls<-c()
for (file in dat) {
  if(file[["dataFile"]]$filename %in% success){
    

    fileID<- file[["dataFile"]]$id
    
    fileurl<-get_url_by_id(
      fileid=fileID,
      dataset = PM_traccomponents_doi,
      format = "original",
      server = server,
      original = TRUE
    )
    
    out_filename<-file[["dataFile"]]$filename
    out_dirlabel<-file[["dataFile"]]$directoryLabel
    if(!is.null(out_dirlabel)){
    dir.create(path = file.path(destinationdir,out_dirlabel),recursive = T)
    out_destfile<-file.path(destinationdir,out_dirlabel,out_filename)
    }
    else{
      dir.create(path = file.path(destinationdir))
      out_destfile<-file.path(destinationdir,out_filename)
    }
    tryCatch({
      download.file(fileurl, 
                    destfile = out_destfile
                    )},
      error=function(e){failed_urls<<-c(failed_urls,fileurl)},
      warning=function(w){failed_urls<<-c(failed_urls,fileurl)})
  }  
}

