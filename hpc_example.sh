#!/bin/bash
#SBATCH -J tava
#SBATCH -N 1
#SBATCH --cpus-per-task=12
#SBATCH -t 24:45:00
#SBATCH --mem=220GB
#SBATCH --partition=amd
# each laz files roughtly consume 300~400 MB , 500 laz batch will result in ~200 GB RAM
export GOOGLE_APPLICATION_CREDENTIALS=<path to GCP access json>
cd $HOME/lidar_processing/apps/lidar_processor
configpath='./myconfig_2017_tava_0_500.yaml'
mv $configpath $configpath.$SLURM_JOB_ID.yaml
$HOME/micromamba/envs/lidar_processing/bin/python lidar_processor/main.py -c $configpath.$SLURM_JOB_ID.yaml -i $SLURM_JOB_ID
