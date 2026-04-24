## Download Annual NLCD Canopy Cover
require('tools')
## TCC
baseurl<- "https://data.fs.usda.gov/geodata/rastergateway/treecanopycover/docs/v2023-5"
start_date<- "1992"
end_date<- "2010"
outputpath<- "S:\\GCMC\\Data\\Greenness\\CanopyCover\\NLCD"
dates<-format(seq(as.Date(start_date,format="%Y"),as.Date(end_date,format="%Y"),by="year"),"%Y")
filenames<-paste0("nlcd_tcc_CONUS_",dates,"_v2023-5_wgs84.zip")
  
urls<-file.path(baseurl,filenames)
options(timeout = 10000)
Map(function(u, d) download.file(u, d, mode="wb"), urls, file.path(outputpath,filenames))
  
##################
## unzip all directories
zippath<- list.files("S:\\GCMC\\Data\\Greenness\\CanopyCover\\NLCD",full.names = T,pattern="*.zip$")
for(zip in zippath){
  unzip(zipfile = zip,exdir = file_path_sans_ext(zip))
  unlink(zip)
}
##################
files<- list.files("S:\\GCMC\\Data\\Greenness\\CanopyCover\\NLCD\\",full.names = T,recursive=T,pattern="*nlcd_tcc_conus_wgs84_v2023-5_.*.tif$")
outdir<-"S:\\GCMC\\Data\\Greenness\\CanopyCover\\NLCD\\nlcd_tcc_CONUS_30m/"
fnames<-basename(files)
fnames<-gsub("conus","CONUS_30m",fnames)
fnames<-gsub("_wgs84_v2023-5_\\d{8}","",fnames)
fnames<-gsub("(\\d{4})\\d{4}", "\\1-01-01", fnames)

require('terra')
old<-rast("S:\\GCMC\\Data\\Greenness\\CanopyCover\\NLCD\\nlcd_tcc_CONUS_30m/nlcd_tcc_CONUS_30m_2023-01-01.tif")
res(new)==res(old)
ext(new)==ext(old)
crs(old)==crs(new)
crs(old,describe=T)
crs(new,describe=T)

for (f in 1:length(files)) {
  new<- terra::project(rast(files[f]),old)
  resample(new,old,filename=file.path(outdir,fnames[f]))
}

#######################################################
files<-list.files(outdir,full.names=T,recursive=T,pattern="*.tif$")
files<-"S:\\GCMC\\Data\\Greenness\\CanopyCover\\NLCD\\nlcd_tcc_CONUS_30m/nlcd_tcc_CONUS_30m_1991-01-01.tif"
for(f in files){
  frast<-rast(f)
  fname<-gsub(pattern = ".tif",replacement = "_v2.tif",f)
  terra::clamp(frast,lower=0,upper=100,values=F,filename=fname)
}

####################################################################
##---- Required Packages
##---- Required Packages
setwd("S:/GCMC/tmp/")
listOfPackages <- c("batchtools","terra","tools","reshape2","ids","parsedate")
for (i in listOfPackages){
  if(! i %in% installed.packages()){
    install.packages(i, dependencies = TRUE)
  }
  require(i,character.only=TRUE)
}



##REQUIRED##
##---- Initialize conf files and template
##---- Initialize batchtools
PROJECT_NAME<-"Create_nlcdTCC"
##---- Create a temporary registry item
if(file.exists(paste(PROJECT_NAME,"Registry",sep="_"))){
  reg = loadRegistry(paste(PROJECT_NAME,"Registry",sep="_"),writeable = TRUE)
  reg$cluster.functions=makeClusterFunctionsSocket()
}else{
  reg = makeRegistry(file.dir = paste(PROJECT_NAME,"Registry",sep="_"), seed = 42)
  reg$cluster.functions=makeClusterFunctionsSocket()
}

create_focalrasters<-function(rfiles,focaldist){
  require('tools')
  require('terra')
  terraOptions(memfrac=0.2)
 
  files<-rfiles
  fs<-focaldist
  
  frast<-rast(files)
  fw<- focalMat(x=frast,d=fs,type='circle',fillNA=T)
  outname<-gsub("30m",paste0(fs,"mfs"),basename(files))
  outname<-gsub("_v2","",outname)
  outdir<- gsub("30m",paste0(fs,"mfs"),dirname(files))
  focalfile<-focal(frast,w=fw,fun="mean",na.policy='all',fillvalue=NA,na.rm=T,filename=file.path(outdir,outname),overwrite=T)
}
##---- Set up the batch processing jobs
##---- grid should contain columns for all desired variable combinations
rasterdir<- "S:\\GCMC\\Data\\Greenness\\CanopyCover\\NLCD\\nlcd_tcc_CONUS_30m/"
rfiles<-list.files(rasterdir,full.names=T,recursive=T,pattern="*_v2.tif$")
rfiles<-rfiles[c(37,38)]
focaldist<-c(1230)

batchgrid = function(rfiles,focaldist){
  require("tools")
  
  
  output<- expand.grid(rfiles = rfiles,
                       focaldist = focaldist,
                       stringsAsFactors = FALSE)
  return(output)
}


##---- Clear the R registry
clearRegistry(reg)

##---- Create jobs
##----  create jobs from variable grid
jobs<- batchMap(fun = create_focalrasters,
                batchgrid(rfiles=rfiles,
                          focaldist=focaldist),
                reg = reg)
batchtools::submitJobs(jobs,resources = list(memory=100000),reg = reg)


