# Install and load the RCurl package
install.packages("RCurl")
library(RCurl)

setwd("S:/GCMC/Data/Climate/PRISM/")

resolution<- c("4km","800m")
vars= c("tmin","tmax","tmean","vpdmin","vpdmax","tdmean","ppt")
period<-c("daily","monthly")
start_date=as.Date("2025-01-01")
end_date=as.Date("2025-12-31")
dates<-seq(start_date,end_date,by="days")

# Define the FTP URL and credentials
ftp_url <- "https://data.prism.oregonstate.edu/time_series/us/an/"

for(res in resolution){
  if(res=="4km"){r="25m"}else if(res=="800m"){r="30s"}else{}
  for(v in vars){
    for(p in period){
      download_files<-sapply(dates,FUN = function(x){file.path(ftp_url,res,v,p,format(x,"%Y"),paste0(paste("prism",v,"us",r,format(x,"%Y%m%d"),sep="_"),".zip"))})
      
      i=1
      # Loop through the list of files and download each one
      for (file_url in download_files) {
        if(i %% 20 ==0){Sys.sleep(30)}
        fname<-basename(file_url)
        
        # Define the local path where the temp file will be saved
        destination<-file.path("S:/GCMC/tmp/PRISM_processing",res,p,v,basename(dirname(file_url)))
        dir.create(destination,recursive = T,showWarnings = F)
        local_file<- file.path(destination,fname)
        
        # Download the file from the FTP server
        binary_data<- getBinaryURL(file_url)
        writeBin(binary_data, local_file)
        
        cat("Downloaded:", file_name, "\n")
        
        i=i+1
      }
      
      cat("All files downloaded successfully.")
    }
  }
}



