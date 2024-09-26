
#Load Required Packages
require('stringdist')
require('tools')

#List all NDVI files
#files<- list.files("E:\\.shortcut-targets-by-id\\1h9cmIyzx_TtG1VTHvrQ_Cq2IQrJzIwqF\\NDVI",pattern = "*.tif$",full.names = TRUE,recursive = TRUE,include.dirs = FALSE)
files<- list.files("H:\\My Drive\\LAN_VIIRS",pattern = "*.tif$",full.names = TRUE,recursive = TRUE,include.dirs = FALSE)
outdir<-"S:\\GCMC\\Data\\BuiltEnvironment\\LAN_VIIRS\\"

n=1
#Loop through files
nfiles<- length(files)
for(i in files){
  print(paste("processing: ", n, " of ",nfiles,sep=""))
  #Check that file name roughly follows correct format
  if(length(strsplit(basename(i),"_")[[1]])>1){
    
    #Get the old file names
    oldname=unlist(strsplit(basename(i),"_"))
    outname<-basename(i)
    file.copy(i,to=paste0(outdir,outname))
    
    
    
    
    # Copy file from Google Drive to Network data archive, rename in the process
    #file.copy(i,to=paste0("S:\\GCMC\\Data\\Greenness\\NDVI\\focalstats_",oldname[2],"m\\",region,"\\",newname))
    
    # Remove file from Earth Engine Folder on Google drive
    file.remove(i)
    
    
  }else{
    print("filename doesn't follow the standard NDVI naming convention from Google Earth Engine Code- Check file names follow form 'region_Xm_YYYY-mm-dd.tif'")
    print(i)}
  n=n+1     
}

