FROM condaforge/mambaforge:23.3.1-1

# install build essentials
RUN apt-get update && apt-get install -y build-essential

WORKDIR /app

# Clone your repository
RUN git clone https://github.com/NSAPH-Data-Processing/satellite_pm25_raster2polygon . 

# Update the base environment
RUN mamba env update -n base -f requirements.yaml 
#&& mamba clean -a

# Create paths to data placeholders
RUN python utils/create_dir_paths.py datapaths.input.satellite_pm25.annual=null datapaths.input.satellite_pm25.monthly=null

# snakemake --configfile conf/config.yaml --cores 4 -C temporal_freq=annual
ENTRYPOINT ["snakemake", "--configfile", "conf/config.yaml"]
CMD ["--cores", "4", "-C", "temporal_freq=annual"]
