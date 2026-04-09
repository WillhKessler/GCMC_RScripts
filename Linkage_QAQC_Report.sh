#!/bin/bash


#SBATCH --job-name=pizza          # Slurm job name
#SBATCH --output=pizza_%j.log	   # combine output and error log
#SBATCH --time=01:00:00            # Time limit request
#SBATCH --mem-per-cpu=5G           # Memory request
#SBATCH --partition=linux12h	  # 12-hour wall time partition


### your commands go below this section

## Download the necessary R markdownfile
wget "https://raw.githubusercontent.com/WillhKessler/GCMC_RScripts/refs/heads/main/GenerateReport_cohort_linkage_QAQC.Rmd"
wget "https://raw.githubusercontent.com/WillhKessler/GCMC_RScripts/refs/heads/main/render_report.R"


## Load R version
module load R/4.3.0
## When submitting this bash file with sbatch, supply the filepath/name as an argument following this file. 
## The sbatch script will pass the desired filename to the R script using $1 (which captures the first argument provided to the sbatch command)


## Run R:

Rscript render_report.R $1

rm render_report.R
rm GenerateReport_cohort_linkage_QAQC.Rmd
