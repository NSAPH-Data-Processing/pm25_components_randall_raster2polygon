#!/bin/bash
#
#SBATCH -p serial_requeue # partition (queue)
#SBATCH -c 16 # number of cores per job in the array
#SBATCH --mem 96GB # memory per job in the array
#SBATCH -t 0-02:00 # time (D-HH:MM)

singularity exec $HOME/singularity_images/satellite_pm25_raster2polygon_latest.sif snakemake --cores 16 -C polygon_name=zcta
 temporal_freq=monthly
