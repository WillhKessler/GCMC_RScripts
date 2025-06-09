
#Load Required Packages
require('stringdist')
require('tools')

#List all NDVI files
#files<- list.files("E:\\.shortcut-targets-by-id\\1h9cmIyzx_TtG1VTHvrQ_Cq2IQrJzIwqF\\NDVI",pattern = "*.tif$",full.names = TRUE,recursive = TRUE,include.dirs = FALSE)
files<- list.files("H:\\My Drive\\NDVI",pattern = "*.tif$",full.names = TRUE,recursive = TRUE,include.dirs = FALSE)
outdir<-"S:\\GCMC\\Data\\Greenness\\NDVI\\"

n=1
#Loop through files
nfiles<- length(files)
for(i in files){
  print(paste("processing: ", n, " of ",nfiles,sep=""))
  #Check that file name roughly follows correct format
  if(length(strsplit(basename(i),"_")[[1]])>2){
    
    #Get the old file names
    oldname=unlist(strsplit(basename(i),"_"))
    print(oldname)
    #Pull the resolution from the old file name
    res=oldname[2]
    print(paste("resolution: ",res))
    
    if(oldname[2]=="30"){
      #Get the NDVI regions from the Data archive
      matchregions=list.dirs(paste0(outdir,res,"m"),full.names = F)
      #Use fuzzy matching to match file name to a region
      region=matchregions[amatch(basename(oldname[1]),matchregions,nomatch=1,maxDist = 6)]
      newname=paste(oldname[-2],collapse='_')
      print(paste("oldfile: ",i))
      print(paste("new file name: ",newname))
      print(paste("new file path:",paste0(outdir,res,"m","\\",region,"\\",newname)))
      file.copy(i,to=paste0(outdir,res,"m","\\",region,"\\",newname))
      
    }else{
      #Get the NDVI regions from the Data archive
      matchregions=list.dirs(paste0(outdir,"focalstats_",res,"m"),full.names = F)
      
      #Use fuzzy matching to match file name to a region
      region=matchregions[amatch(basename(oldname[1]),matchregions,nomatch=1,maxDist = 6)]
      newname=paste(oldname,collapse="_") 
      print(paste("oldfile: ",i))
      print(paste("new file name: ",newname))
      print(paste("new file path: ",paste0(outdir,"focalstats_",res,"m","\\",region,"\\",newname)))
      file.copy(i,to=paste0(outdir,"focalstats_",res,"m","\\",region,"\\",newname))
      
    }
    
  
    
    # Copy file from Google Drive to Network data archive, rename in the process
    #file.copy(i,to=paste0("S:\\GCMC\\Data\\Greenness\\NDVI\\focalstats_",oldname[2],"m\\",region,"\\",newname))
    
    # Remove file from Earth Engine Folder on Google drive
    file.remove(i)
    

  }else{
  print("filename doesn't follow the standard NDVI naming convention from Google Earth Engine Code- Check file names follow form 'region_Xm_YYYY-mm-dd.tif'")
  print(i)}
n=n+1     
}

