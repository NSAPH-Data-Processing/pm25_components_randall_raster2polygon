#!/bin/bash
#SBATCH -p hsph # (queue)
#SBATCH -c 48 # number of cores per job in the array 
#SBATCH --mem 128GB # memory per job in the array 
#SBATCH -t 1-12:00 # time (D-HH:MM) 

snakemake --cores 12 --rerun-incomplete