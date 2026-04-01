import yaml
from src.aggregate_components import available_shapefile_year
from hydra import compose, initialize

conda: "environment.yaml"
configfile: "conf/snakemake.yaml"

# === Define Job Matrix ===
# gather the variables to construct a job matrix over all parameter combinations
# from the snakemake.yaml file
temporal_frequencies = config['temporal_freq']
polygon_names = config['polygon_name']
shapefile_years = config['shapefile_year']
components = config['components']
version = config.get('version', 'V5NA')  # V5NA or V6NA — selects data source and directory layout
months_list = [str(i).zfill(2) for i in range(1, 12 + 1)]
years_list = list(range(2000, 2023 + 1))

# Map algorithm version to its satellite_component Hydra config group name
satellite_component_config = {
    'V5NA': 'us_components',
    'V6NA': 'us_components_v6na',
}.get(version, 'us_components')

# === Load Hydra Config ===
# get hydra config variables from the config.yaml file, applying the version-specific satellite_component
with initialize(version_base=None, config_path="conf"):
    hydra_cfg = compose(config_name="config", overrides=[f"satellite_component={satellite_component_config}"])

satellite_component_cfg = hydra_cfg.satellite_component
shapefiles_cfg = hydra_cfg.shapefiles

# == Define rules ==
# Rule all will contain the list of final output files. To keep intermediate files,
# add the file stem to the input of this rule.
rule all:
    input:
        # merged files with all components (one file per year) - directly aggregated
        expand(
            f"data/output/pm25_components__randall/{version}/{{polygon_name}}_{{temporal_freq}}/" +
            f"pm25_components__randall__{{polygon_name}}_{{temporal_freq}}_{{year}}.parquet",
            temporal_freq=temporal_frequencies,
            year=years_list,
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
            f"data/input/pm25_components__randall/{version}/{{temporal_freq}}/{{component}}/",
            component=components,
            temporal_freq=temporal_frequencies
        )

# this is the individual component download rule. It will run for each combination of variables in the job matrix
# note that this only needs to download once, so no need for temporal_freq or year wildcards
rule download_component:
    output:
        directory(f"data/input/pm25_components__randall/{version}/{{temporal_freq}}/{{component}}/")
    log:
        f"logs/download_components_{{component}}_{{temporal_freq}}_{version}.log"
    shell:
       f"python src/download_components.py "
       f"satellite_component={satellite_component_config} "
       f"++version={version} "
       "component={wildcards.component} ++temporal_freq={wildcards.temporal_freq} &> {log}"


def get_shapefile_input(wildcards):
    # Get the available shapefile years for this polygon type from config
    shapefile_years_list = [int(year) for year in shapefiles_cfg[wildcards.polygon_name].keys()]
    shapefile_year = available_shapefile_year(int(wildcards.year), shapefile_years_list)
    return f"data/input/shapefiles/shapefile_{wildcards.polygon_name}_{shapefile_year}/shapefile.shp"

# Individual component aggregation rule - one rule execution per component
rule aggregate_single_component:
    input:
        get_shapefile_input,
        f"data/input/pm25_components__randall/{version}/{{temporal_freq}}/{{component}}/"
    output:
        f"data/intermediate/pm25_components__randall/{version}/{{temporal_freq}}/{{component}}/{{component}}__{{polygon_name}}_{{temporal_freq}}_{{year}}.parquet"
    log:
        f"logs/aggregate_{{component}}_{{polygon_name}}_{{temporal_freq}}_{{year}}_{version}.log"
    shell:
        (
            "PYTHONPATH=. python src/aggregate_components.py " +
            f"satellite_component={satellite_component_config} " +
            f"++version={version} " +
            "polygon_name={wildcards.polygon_name} ++temporal_freq={wildcards.temporal_freq} ++year={wildcards.year} ++component={wildcards.component} " +
            "&> {log}"
        )

rule merge_components_yearly:
    input:
        lambda wildcards: expand(
            f"data/intermediate/pm25_components__randall/{version}/yearly/{{component}}/{{component}}__{{polygon_name}}_yearly_{{year}}.parquet",
            component=components,
            polygon_name=wildcards.polygon_name,
            year=wildcards.year
        )
    output:
        f"data/output/pm25_components__randall/{version}/{{polygon_name}}_yearly/pm25_components__randall__{{polygon_name}}_yearly_{{year}}.parquet"
    log:
        f"logs/merge_yearly_components_{{polygon_name}}_yearly_{{year}}_{version}.log"
    shell:
        (
            "PYTHONPATH=. python src/merge_components.py " +
            f"satellite_component={satellite_component_config} " +
            f"++version={version} " +
            "polygon_name={wildcards.polygon_name} ++temporal_freq=yearly ++year={wildcards.year} " +
            "&> {log}"
        )

rule merge_components_monthly:
    input:
        lambda wildcards: expand(
            f"data/intermediate/pm25_components__randall/{version}/monthly/{{component}}/{{component}}__{{polygon_name}}_monthly_{{year}}.parquet",
            component=components,
            polygon_name=wildcards.polygon_name,
            year=wildcards.year
        )
    output:
        f"data/output/pm25_components__randall/{version}/{{polygon_name}}_monthly/pm25_components__randall__{{polygon_name}}_monthly_{{year}}.parquet"
    log:
        f"logs/merge_monthly_components_{{polygon_name}}_monthly_{{year}}_{version}.log"
    shell:
        (
            "PYTHONPATH=. python src/merge_components.py " +
            f"satellite_component={satellite_component_config} " +
            f"++version={version} " +
            "polygon_name={wildcards.polygon_name} ++temporal_freq=monthly ++year={wildcards.year} " +
            "&> {log}"
        )
