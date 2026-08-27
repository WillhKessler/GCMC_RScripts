#!/bin/bash
#SBATCH --job-name=submit_recombine_nh3_AP          # Slurm job name
#SBATCH --output=submit_recombine_nh3_AP_%j.log   # combine output and error log
#SBATCH --time=360:00:00              # Time limit request
#SBATCH --mem-per-cpu=180G             # Memory request
#SBATCH --partition=r9      # 12-hour wall time partition

### Your R commands go below this line


module load terra/4.5.2


Rscript NHS3_APlinkage_recombine.R

