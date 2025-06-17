#!/bin/bash
#
#SBATCH -p intermediate # partition (queue)
#SBATCH -c 12 # number of cores per job in the array
#SBATCH --mem 48GB # memory per job in the array
#SBATCH -t 0-08:00 # time (D-HH:MM)

snakemake --cores 12
# snakemake download_all_components --cores 8