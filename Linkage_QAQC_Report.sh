#!/bin/bash


#SBATCH --job-name=pizza	  # Slurm job name
#SBATCH --output=pizza_%j.log      # combine output and error log
#SBATCH --time=01:00:00            # Time limit request
#SBATCH --mem-per-cpu=5G           # Memory request
#SBATCH --partition=linux12h	  # 12-hour wall time partition


### your commands go below this section

module load R/4.3.0
# When submitting this bash file with sbatch, supply the filepath/name as an argument following this file. The sbatch script will pass the desired filename to the R script using $1 (which captures the first argument provided to the sbatch command), $2 for the second, etc.
# sbatch
filenamecsv="$1"

## Run R:
Rscript -e "rmarkdown::render(
  'GenerateReport_cohort_linkage_QAQC.Rmd',
  params = list(file_name = '$filenamecsv'))"
