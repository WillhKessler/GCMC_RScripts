# Install and load the RCurl package
install.packages("RCurl")
library(RCurl)

setwd("S:/GCMC/Data/PRISM/4km")
# Define the FTP URL and credentials
ftp_url <- "https://ftp.prism.oregonstate.edu/daily"

dates<-seq(as.Date("19810101",tryFormats="%Y%m%d"),as.Date("20241231",tryFormats="%Y%m%d"),by="days")
var= c("tmin","tmax","tmean","vpdmin","vpdmax","tdmean","ppt")

# Generate names
files2<-lapply(X = dates,FUN = function(x) {paste0("/","XXXXX","/",format(x,"%Y"),"/",paste("PRISM","XXXXX","stable","4kmD2",format(x,"%Y%m%d"),sep="_"),"_bil.zip")})
files<-unlist(files2)

files<-unlist(lapply(var,function(x){gsub("XXXXX",x,files)}))



i=1
# Loop through the list of files and download each one
for (file_name in files) {
  if(i %% 20 ==0){Sys.sleep(30)}
  # Construct full FTP URL for each file
  file_url <- paste0(ftp_url, file_name)
  
  # Define the local path where the file will be saved
  dir.create(file.path(getwd(),dirname(file_name)),recursive = T,showWarnings = F)
  local_file<- file.path(getwd(),file_name)
  
  # Download the file from the FTP server
  binary_data<- getBinaryURL(file_url)
  writeBin(binary_data, local_file)
  
  cat("Downloaded:", file_name, "\n")
  
  i=i+1
}

cat("All files downloaded successfully.")

