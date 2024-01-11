import yaml

conda: "requirements.yaml"

# obtain list of shapefile years
defaults_dict = {key: value for d in config['defaults'] if isinstance(d, dict) for key, value in d.items()}

shapefiles_cfg = yaml.safe_load(open(f"conf/shapefiles/{defaults_dict['shapefiles']}.yaml", 'r'))
shapefile_years_list = list(shapefiles_cfg[config["shapefile_polygon_name"]].keys())

temporal_freq_cfg = yaml.safe_load(open(f"conf/temporal_freq/{defaults_dict['temporal_freq']}.yaml", 'r'))

years_list = list(range(1998, 2022 + 1))

# == Define rules ==
rule all:
    input:
        expand("data/output/satellite_pm25_raster2polygon/satellite_pm25_{polygon_name}_{year}.parquet", 
            polygon_name=config["shapefile_polygon_name"], 
            year=years_list
        )

rule download_shapefiles:
    params:
        polygon_name = "{polygon_name}",
        shapefile_year = "{shapefile_year}"
    output:
        "data/input/shapefiles/shapefile_{polygon_name}_{shapefile_year}/shapefile.shp" 
    shell:
        "python src/download_shapefile.py shapefile_year={params.shapefile_year}"

rule aggregate_annual:
    params:
        year = "{year}"
    input:
        expand("data/input/shapefiles/shapefile_{polygon_name}_{shapefile_year}/shapefile.shp", 
               polygon_name=config["shapefile_polygon_name"], 
               shapefile_year=shapefile_years_list)
    output:
        "data/output/satellite_pm25_raster2polygon/satellite_pm25_{polygon_name}_{year}.parquet"
    shell:
        "python src/aggregate.py time_freq=annual year={params.year}"
