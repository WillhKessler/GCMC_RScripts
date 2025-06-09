
require(terra)
baseurl<-"https://www.northwestknowledge.net/metdata/data/permanent"
outputpath<- "S:/GCMC/tmp/gridMET"
datadir<-"S:/GCMC/Data/Climate/gridMET/"
options(timeout = max(1000, getOption("timeout")))

years<-seq(1993,2024)


dates<-seq.Date(from = as.Date(paste0(years[1],"-01-01"),tryFormats = "%Y-%m-%d"),to = as.Date(paste0(years[length(years)],"-12-31"),tryFormats = "%Y-%m-%d"),by='day')



for(i in 1:length(dates)){
  downloadurl<-file.path(baseurl,format(dates[i],format="%Y"),paste0("permanent_gridmet_",format(dates[i],format="%Y%m%d"),".nc"))
  
  tryCatch(
    {
      # Just to highlight: if you want to use more than one
      # R expression in the "try" part then you'll have to
      # use curly brackets.
      # 'tryCatch()' will return the last evaluated expression
      # in case the "try" part was completed successfully
      
      download.file(url = downloadurl,destfile = file.path(outputpath,basename(downloadurl)),mode = 'wb',overwrite=T)
      
    
      tmpout<-terra::rast(file.path(outputpath,basename(downloadurl)))
      rnames<-c("tmax",
                "tmin",
                "rhmax",
                "rhmin",
                "sh",
                "winds",
                "ppt",
                "windd",
                "soltotal",
                "retgrass",
                "ercg",
                "big",
                "dfm100hr",
                "dfm1000hr",
                "retalf",     
                "vpdmean")
      vardirs<-file.path(datadir,rnames,format(dates[i],format="%Y"))
      
      sapply(vardirs,FUN = function(x) if(dir.exists(x)){"do nothing"} else{dir.create(path=x,recursive=T)})
      
      outnames<-file.path(datadir,rnames,format(dates[i],format="%Y"),paste0("gridmet_",rnames,"_",format(dates[i],"%Y%m%d"),".tif"))
      writeRaster(x = tmpout,outnames,overwrite=T)
      file.remove(file.path(outputpath,basename(downloadurl)))
      # The return value of `readLines()` is the actual value
      # that will be returned in case there is no condition
      # (e.g. warning or error).
    })
  
}
    

                