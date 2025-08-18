#!/bin/bash
#SBATCH -p shared # (queue)
#SBATCH -c 48 # number of cores per job in the array (increased for speed)
#SBATCH --mem 128GB # memory per job in the array (increased proportionally)
#SBATCH -t 0-08:00 # time (D-HH:MM) (reduced since we're using more cores)

snakemake --cores 12 --rerun-incomplete
# snakemake download_all_components --cores 