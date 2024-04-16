# HOW TO:
The principal code provided in this repository is a workflow for using the 'terra' R package to extract raster data to points or polygons based on specific time intervals. It was originally written to assist in linking public health cohort data to environmental exposure data in the form of daily or monthly raster data. It is implemented with `batchtools` in R to run in Parallel on a compute cluster using the SLURM job manager, or in parallel or serial locally. 

There are up to 4 files necessary for the workflow:
1. ParallelXXXXX_processingtemplate.R
2. Functions_RasterExtraction.R
3. slurm.tmpl
4. batchtools.conf.R
   
The ParallelXXXXX_processingtemplate.R contains all organizational information required for `batchtools` to set up and execute your processing jobs; they are fairly standard implementations of `batchtools` workflows with additional user inputs for running this workflow. Next, you will need an R file containing one or more functions you wish to run. These functions can be placed directly in the ParallelXXXXX_processingtemplate.R or sourced from an external file. In this workflow, the required functions are sourced from an external file called `Functions_RasterExtraction.R`. Thirdly, depending on whether you are implementing the workflow on a computing cluster with the SLURM job manager, or locally with either multisocket or interactive workflows. You may also need an R configuration file, and a `brew` `slurm.tmpl` template.   

## Parallel Processing Template


## Batchtools tmpl 

## slurm.tmpl
