#!/bin/bash
#
#SBATCH -p shared # partition (queue)
#SBATCH -c 32 # number of cores 
#SBATCH --mem 96GB # memory 
#SBATCH -t 0-01:00 # time (D-HH:MM)

singularity exec $HOME/singularity_images/satellite_pm25_raster2polygon_latest.sif snakemake --cores 16 -C polygon_name=zcta temporal_freq=monthly
