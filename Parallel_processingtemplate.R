####---- READ ME ----###
# Use this script as a template for setting up and running batch processing jobs on your local machine. 
# This utilizes one or more cores in a multicored machine.
# The required inputs are standardized for ALL functions defined in the Raster Extractions R source file 

##---- REQUIRED INPUTS ----##
projectdirectory<-"" # This is where all your outputs, registry items, and intermediate files will be created
scheduler = "SLURM" # String to set where the jobs will run. i.e. SLURM scheduler on cluster, multicore workstation, or local interactive session. Can be values of "SLURM", "interactive","socket"
PROJECT_NAME<-"GCMC2_ParallelTest" # String with a project name
rasterdir<-"S:/GCMC/Data/Climate/PRISM/" # string with a file path to raster covariates to extract- function will try to pull variable names from sub directories i.e /PRISM/ppt or /PRISM/tmean or /NDVI/30m
extractionlayer = "C:/Users/wik191/OneDrive - Harvard University/_Projects/Andrea_Bellavia/sites_10M.shp" # string with path to spatial layer to use for extraction. Can be a CSV or SHP or GDB 
layername = "sites_10M" # Layer name used when extraction layer is an SHP or GDB
IDfield<-"ORIG_FID" # Field in extraction layer specifying IDs for features, can be unique or not, used to chunk up batch jobs
Xfield<- "X" 
Yfield<- "Y"
startdatefield = "start_date" # Field in extraction layer specifying first date of observations
enddatefield = "end_date" # Field in extraction layer specifying last date of observations
predays = 0 # Integer specifying how many days preceding 'startingdatefield' to extract data. i.e. 365 will mean data extraction will begin 1 year before startdatefield
weights = NA # string specifying file path to raster weights, should only be used when extraction layer is a polygon layer


## Load Required Functions
source("https://raw.githubusercontent.com/WillhKessler/GCMC_RScripts/main/Functions_RasterExtraction.R")

## select desired cluster function  
select.Cluster()

## Initialize Jobs
init.jobs()









