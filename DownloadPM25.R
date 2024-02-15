
#############-----------READ ME---------########################################
# this script will download files 

###################################################################

#require(sp)
require(rgdal)
require(terra)
require(tools)
require(httr2)
#########################################
##---Required Inputs---------############
netrc_path<- "C:/Users/wik191/_netrc"
cookie_path<- "C:/Users/wik191/.urs_cookies"

baseurl<-"https://sedac.ciesin.columbia.edu/downloads/data/aqdh/aqdh-pm2-5-concentrations-contiguous-us-1-km-2000-2016/"
#baseurl<-"https://sedac.ciesin.columbia.edu/downloads/data/aqdh/aqdh-no2-concentrations-contiguous-us-1-km-2000-2016/"
filename<- "aqdh-pm2-5-concentrations-contiguous-us-1-km-2000-2016"
#filename<- "aqdh-no2-concentrations-contiguous-us-1-km-2000-2016"
outputdirectory<- "S:\\GCMC\\Data\\AirPollution\\PM_25\\Daily\\"
setwd(outputdirectory)
range = format(seq(from = as.Date('2000-01-01','%Y-%m-%d'),to = as.Date('2016-12-01','%Y-%m-%d'),by ="month"),"%Y%m")
#########################################

urls = unlist(lapply(range, function(x){paste(paste(baseurl,filename,sep=""),as.character(x,"%Y%m"),"geotiff.zip",sep="-")}))
urls
dest = unlist(lapply(range,function(x){paste(paste(outputdirectory,filename,sep=""),as.character(x,"%Y%m"),"geotiff.zip",sep="-")}))
dest


set_config(config(netrc=TRUE,netrc_file=netrc_path))


#Map(function(u, d) download.file(u, d, mode="wb"), urls, dest)


httr::GET(url = urls[1],set_cookies("LC" = "cookies"))

Map(function(u,d)httr::GET(url = u,write_disk(d),overwrite=TRUE),urls,dest)
#sapply(X = dest,FUN = function(x) unzip(zipfile = x,exdir = ouputdirectory,overwrite = TRUE))


