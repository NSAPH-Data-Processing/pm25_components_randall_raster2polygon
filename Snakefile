import yaml
from src.aggregate_components import available_shapefile_year
from hydra import compose, initialize

conda: "requirements.yaml"
configfile: "conf/snakemake.yaml"

# === Define Job Matrix ===
# gather the variables to construct a job matrix over all parameter combinations
# from the snakemake.yaml file
temporal_frequencies = config['temporal_freq']
polygon_names = config['polygon_name']
shapefile_years = config['shapefile_year']
components = config['components']
months_list = [str(i).zfill(2) for i in range(1, 12 + 1)]
years_list = list(range(2000, 2022 + 1)) #TODO : should this be 2000-2022? This was the range of files I found in the site: https://wustl.app.box.com/s/tfyt4uyuzbt4hbnw7bhos16aep9b5u7g/folder/251943064650


# === Load Hydra Config ===
# get hydra config variables from the config.yaml file
with initialize(version_base=None, config_path="conf"):
    hydra_cfg = compose(config_name="config")

satellite_component_cfg = hydra_cfg.satellite_component
shapefiles_cfg = hydra_cfg.shapefiles

# == Define rules ==
# Rule all will contain the list of final output files. To keep intermediate files,
# add the file stem to the input of this rule.
rule all:
    input:
        # yearly aggregated component files
        expand(
            f"data/output/satellite_components/{{component}}/" +
            f"yearly/{{component}}_{{polygon_name}}_{{temporal_freq}}_" +  
            f"{{year}}.parquet", 
            temporal_freq=temporal_frequencies,
            year=years_list,
            component=components,
            polygon_name=polygon_names
        ),
        # monthly aggregated component files
        expand(
            f"data/output/satellite_components/{{component}}/" +
            f"monthly/{{component}}_{{polygon_name}}_{{temporal_freq}}_" +  
            f"{{year}}_{{month}}.parquet", 
            temporal_freq=temporal_frequencies,
            year=years_list,
            month=months_list,
            component=components,
            polygon_name=polygon_names
        )

# This rule gets the shapefiles for the polygons of interest, ie zctas and counties
# importantly, this is defined once in shapefiles.yaml; you can select which polygon's year to do
# the agggregation for. This is NOT parallelized over all polygons and years. The current setting will
# generate aggregations for the currently hardcoded polygon_name and shapefile_year variables.
# Possible #TODO: make this a job matrix as well
rule download_shapefiles:
    output:
        "data/input/shapefiles/shapefile_{polygon}_{shapefile_year}/shapefile.shp"
    shell:
        "python src/download_shapefile.py polygon_name={wildcards.polygon} shapefile_year={wildcards.shapefile_year}"

# this rule launches the download of all the components. It essentially forces download_component rule to run
rule download_all_components:
    input:
        expand(
            f"data/input/satellite_components/{{component}}/{{temporal_freq}}/",
            component=components,
            temporal_freq=temporal_frequencies
        )

# this is the individual component download rule. It will run for each combination of variables in the job matrix
# note that this only needs to download once, so no need for temporal_freq or year wildcards
rule download_component:
    output:
        directory(f"data/input/satellite_components/{{component}}/{{temporal_freq}}/")
    log:    
        "logs/download_components_{component}_{temporal_freq}.log"
    shell:
       f"python src/download_components.py "
       "component={wildcards.component} +temporal_freq={wildcards.temporal_freq} &> {log}"


def get_shapefile_input(wildcards):
    shapefile_year = available_shapefile_year(int(wildcards.year), years_list)
    return f"data/input/shapefiles/shapefile_{wildcards.polygon_name}_{shapefile_year}/shapefile.shp"

rule aggregate_components_yearly:
    input:
        get_shapefile_input,
        lambda wildcards: f"data/input/satellite_components/{wildcards.component}/yearly/"
    output:
        "data/output/satellite_components/{component}/yearly/{component}_{polygon_name}_yearly_{year}.parquet"
    log:
        "logs/aggregate_yearly_{component}_{polygon_name}_yearly_{year}.log"
    shell:
        (
            "PYTHONPATH=. python src/aggregate_components.py component={wildcards.component} "
            "polygon_name={wildcards.polygon_name} +temporal_freq=yearly +year={wildcards.year} " +
            "&> {log}"
        )

rule aggregate_components_monthly:
    input:
        get_shapefile_input,
        lambda wildcards: directory(f"data/input/satellite_components/{wildcards.component}/monthly/")
    output:
        "data/output/satellite_components/{component}/monthly/{component}_{polygon_name}_monthly_{year}-{month}.parquet"
    log:
        "logs/aggregate_monthly_{component}_{polygon_name}_monthly_{year}-{month}.log"
    shell:
        (
            "PYTHONPATH=. python src/aggregate_components.py component={wildcards.component} " +
            "polygon_name={wildcards.polygon_name} +temporal_freq=monthly " +
            "&> {log}"
        )
