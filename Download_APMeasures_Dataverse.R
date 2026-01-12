
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
require(purrr)
#########################################
# Set constants
dataset_doi <- "10.7910/DVN/3H7DNP"
server <- Sys.getenv("DATAVERSE_SERVER", "dataverse.harvard.edu")
download_root <- "S:/GCMC/tmp/pmcomp_temp"

# Create base download folder
if (!dir.exists(download_root)) {
  dir.create(download_root)
}

# Get dataset metadata
ds_info <- get_dataset(dataset = dataset_doi, server = server)
files_meta <- ds_info$files

# Function to safely construct destination paths
get_destination_path <- function(fmeta) {
  
  # directoryLabel may be NULL or missing
  dir_label <- fmeta$directoryLabel
  
  if (!is.null(dir_label) && nzchar(dir_label)) {
    # Construct nested directory inside root
    full_dir <- file.path(download_root, dir_label)
  } else {
    # No folder → root
    full_dir <- download_root
  }
  
  # Create directory if needed
  if (!dir.exists(full_dir)) {
    dir.create(full_dir, recursive = TRUE)
  }
  
  # Full path to the destination file
  file.path(full_dir, fmeta$label)
}

# Iterate through files and download each
for(i in 1:nrow(files_meta)){
  fmeta<-files_meta[i,]
  
  dest <- get_destination_path(fmeta)
  fname <- fmeta$label
  
  fid   <- fmeta$id
  
  if (!file.exists(dest)) {
    message("Downloading: ", dest)
    out<-get_file_by_id(
      file = fid,
      dataset = dataset_doi,
      server = server,
      destfile = dest,
      overwrite = FALSE
    )
    writeBin(out,dest)
  } else {
    message("Exists, skipping: ", dest)
  }
}

message("All downloads complete.")
