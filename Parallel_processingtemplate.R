####---- READ ME ----###
# Use this script as a template for setting up and running batch processing jobs on your local machine. 
# This utilizes one or more cores in a multicored machine.
# The required inputs are standardized for ALL functions defined in the Raster Extractions R source file 

##---- REQUIRED INPUTS ----##
projectdirectory = getwd() # This is where all your outputs, registry items, and intermediate files will be created
scheduler = "socket" # String to set where the jobs will run. i.e. SLURM scheduler on cluster, multicore workstation, or local interactive session. Can be values of "SLURM", "interactive","socket"
PROJECT_NAME = "simple_ParallelTest" # String with a project name
rasterdir = "S:/GCMC/Data/Climate/PRISM/daily" # string with a file path to raster covariates to extract- function will try to pull variable names from sub directories i.e /PRISM/ppt or /PRISM/tmean or /NDVI/30m
extractionlayer = "S:\\GCMC\\_Code\\TESTING_datasets\\csv\\toyCohort_nurses55.csv" # string with path to spatial layer to use for extraction. Can be a CSV or SHP or GDB 
layername = "sites_10M" # Layer name used when extraction layer is an SHP or GDB
IDfield = "UUID" # Field in extraction layer specifying IDs for features, can be unique or not, used to chunk up batch jobs
Xfield = "longitude" 
Yfield = "latitude"
startdatefield = "start_date" # Field in extraction layer specifying first date of observations
enddatefield = "end_date" # Field in extraction layer specifying last date of observations
predays = 0 # Integer specifying how many days preceding 'startingdatefield' to extract data. i.e. 365 will mean data extraction will begin 1 year before startdatefield
weights = NA # string specifying file path to raster weights, should only be used when extraction layer is a polygon layer
period = "daily" #specify a period to summarize values: daily, monthly, yearly, defaults to daily
email = "" # enter your email and SLURM will send you an email when each chunk of jobs is complete

## Load Required Functions
source("https://raw.githubusercontent.com/WillhKessler/GCMC_RScripts/refs/heads/main/Functions_RasterExtraction.R")

## select desired cluster function  
reg=select.Cluster(scheduler = scheduler)

## Initialize Jobs
init.jobs(func = extract.rast,rasterdir = rasterdir,extractionlayer = extractionlayer,layername = layername,IDfield = IDfield,Xfield = Xfield,
          Yfield = Yfield,startdatefield = startdatefield,enddatefield = enddatefield,predays = predays,weightslayers = weights,chunk.size = 1000,
          memory = 2048, projectdirectory = projectdirectory,projectname=PROJECT_NAME, email=email, scheduler=scheduler,reg=reg)









