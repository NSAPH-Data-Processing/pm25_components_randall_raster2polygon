import yaml
from src.aggregate_pm25 import available_shapefile_year

conda: "requirements.yaml"

defaults_dict = {key: value for d in config['defaults'] if isinstance(d, dict) for key, value in d.items()}

shape = defaults_dict['shapefiles']
shapefiles_cfg = yaml.safe_load(open(f"conf/shapefiles/{shape}.yaml", 'r'))
shapefile_years_list = list(shapefiles_cfg[config["shapefile_polygon_name"]].keys())
temporal_freq = defaults_dict['satellite_pm25']
satellite_pm25_cfg = yaml.safe_load(open(f"conf/satellite_pm25/{temporal_freq}.yaml", 'r'))
polygon_name=config["shapefile_polygon_name"]

years_list = list(range(1998, 2022 + 1))
months_list = "01" if temporal_freq == 'annual' else [str(i).zfill(2) for i in range(1, 12 + 1)]

# == Define rules ==
rule all:
    input:
        expand(f"data/output/satellite_pm25_raster2polygon/{temporal_freq}/satellite_pm25_{shape}_" + 
                ("{year}.parquet" if temporal_freq == 'annual' else "{year}_{month}.parquet"), 
            year=years_list,
            month=months_list
        )

rule download_shapefiles:
    output:
        f"data/input/shapefiles/shapefile_{polygon_name}_" + "{shapefile_year}/shapefile.shp" 
    shell:
        "python src/download_shapefile.py shapefile_year={wildcards.shapefile_year}"

def get_shapefile_input(wildcards):
    shapefile_year = available_shapefile_year(int(wildcards.year), shapefile_years_list)
    return f"data/input/shapefiles/shapefile_{polygon_name}_{shapefile_year}/shapefile.shp"

rule aggregate_pm25:
    input:
        get_shapefile_input,
        (
            f"data/input/satellite_pm25/{temporal_freq}/{satellite_pm25_cfg['file_prefix']}." + 
            ("{year}01-{year}12.nc" if temporal_freq == 'annual' else "{year}{month}-{year}{month}.nc")
        )

    output:
        (
            f"data/output/satellite_pm25_raster2polygon/{temporal_freq}/satellite_pm25_{shape}_" + 
            ("{year}.parquet" if temporal_freq == 'annual' else "{year}_{month}.parquet")
        )
    shell:
        (
            f"python src/aggregate_pm25.py satellite_pm25={temporal_freq} " + 
            ("year={wildcards.year}" if temporal_freq == 'annual' else "year={wildcards.year} month={wildcards.month}")
        )
