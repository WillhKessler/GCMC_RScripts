# Install and load the RCurl package
#install.packages("RCurl")
library(RCurl)
library(datasets)

#2020
setwd("S:/GCMC/Data/AirPollution/HAPs/AmbientConcentrations/2020")
ftp_url <- "https://gaftp.epa.gov/rtrmodeling_public/AirToxScreen/2020/Ambient%20Concentrations/"
regions<- c(paste0("Region",1:10))
files<-paste(regions,"2020ATS_Ambient_Concentrations.xlsx",sep="_")

# #2017
# setwd("S:/GCMC/Data/AirPollution/HAPs/AmbientConcentrations/2017")
# ftp_url <- "https://www.epa.gov/system/files/other-files/2022-03/"
# regions<- tolower(c(state.abb))
# files<-paste("concexprisk_tract_poll_",regions,".zip",sep="")

# #2014
# setwd("S:/GCMC/Data/AirPollution/HAPs/AmbientConcentrations/2014")
# ftp_url <- "https://www.epa.gov/sites/production/files/2018-08/"
# regions<- tolower(c(state.abb))
# files<-paste("concexprisk_tract_poll_",regions,".zip",sep="")

# #2011
# setwd("S:/GCMC/Data/AirPollution/HAPs/AmbientConcentrations/2011")
# ftp_url <- "https://www.epa.gov/sites/default/files/2015-12/"
# regions<- tolower(c(state.abb))
# files<-paste("concexprisk_tract_poll_state_",regions,"_23nov15.zip",sep="")

# #2005
# setwd("S:/GCMC/Data/AirPollution/HAPs/AmbientConcentrations/2005")
# ftp_url <- "https://www.epa.gov/sites/default/files/2015-12/"
# regions<- tolower(c(state.abb))
# files<-paste(regions,"_nata05_v4_bytract_allhapcat.zip",sep="")

# #2002
# setwd("S:/GCMC/Data/AirPollution/HAPs/AmbientConcentrations/2002")
# ftp_url <- "https://archive.epa.gov/nata2002/web/tractmdb/statemdb/"
# regions<- tolower(c(state.abb))
# files<-paste(regions,"_nata_bytract_allhapcat.mdb",sep="")

# #1999
# setwd("S:/GCMC/Data/AirPollution/HAPs/AmbientConcentrations/1999")
# ftp_url <- "https://archive.epa.gov/airtoxics/nata1999/web/zip99/"
# regions<- tolower(c(state.abb))
# files<-paste(regions,sep="")

# #1996
# setwd("S:/GCMC/Data/AirPollution/HAPs/AmbientConcentrations/1996")
# ftp_url <- "https://archive.epa.gov/airtoxics/nata/web/xls/"
# regions<- tolower(c(state.abb))
# files<-paste(regions,"_conc.xls",sep="")

require('utils')
i=1
# Loop through the list of files and download each one
for (file_name in files) {
  # Construct full FTP URL for each file
  file_url <- paste0(ftp_url, file_name)
  
  # Define the local path where the file will be saved
  local_file<- file.path(getwd(),file_name)
  tryCatch(
    {
      message("Trying to download file")
      download.file(file_url,local_file)
      suppressWarnings(readLines(file_url))
    },
    error = function(cond) {
      message(paste("URL does not seem to exist:", file_url))
      message("Here's the original error message:")
      message(conditionMessage(cond))
      NA
    },
    warning = function(cond) {
      message(paste("URL caused a warning:", file_url))
      message("Here's the original warning message:")
      message(conditionMessage(cond))
      NULL
    },
    finally = {
      message(paste("Processed URL:", file_url))
      message("Some other message at the end")
    }
  )
}
  #download.file(file_url,local_file)
  # Download the file from the FTP server
  #binary_data<- getBinaryURL(file_url)
  #writeBin(binary_data, local_file)
  
  #cat("Downloaded:", file_name, "\n")
  
  i=i+1


cat("All files downloaded successfully.")



