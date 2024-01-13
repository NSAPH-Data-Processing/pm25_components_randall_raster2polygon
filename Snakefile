import yaml

conda: "requirements.yaml"

# obtain list of shapefile years
defaults_dict = {key: value for d in config['defaults'] if isinstance(d, dict) for key, value in d.items()}

shapefiles_cfg = yaml.safe_load(open(f"conf/shapefiles/{defaults_dict['shapefiles']}.yaml", 'r'))
shapefile_years_list = list(shapefiles_cfg[config["shapefile_polygon_name"]].keys())

satellite_pm25_cfg = yaml.safe_load(open(f"conf/satellite_pm25/{defaults_dict['satellite_pm25']}.yaml", 'r'))

years_list = list(range(1998, 2022 + 1))

# == Define rules ==
rule all:
    input:
        expand("data/output/satellite_pm25_raster2polygon/satellite_pm25_{shape}_{year}.parquet", 
            shape=defaults_dict['shapefiles'], 
            year=years_list, 
            month=[str(i).zfill(2) for i in range(1, 12 + 1)]
        ),
        expand("data/output/satellite_pm25_raster2polygon/satellite_pm25_{shape}_{year}_{month}.parquet", 
            shape=defaults_dict['shapefiles'], 
            year=years_list, 
            month=[str(i).zfill(2) for i in range(1, 12 + 1)]
        )

rule download_shapefiles:
    params:
        shapefile_year = "{shapefile_year}"
    output:
        "data/input/shapefiles/shapefile_{polygon_name}_{shapefile_year}/shapefile.shp" 
    shell:
        "python src/download_shapefile.py shapefile_year={params.shapefile_year}"

rule aggregate_annual_pm25:
    params:
        year = "{year}",
        month = "{month}"
    input:
        expand("data/input/shapefiles/shapefile_{polygon_name}_{shapefile_year}/shapefile.shp", 
            polygon_name=config["shapefile_polygon_name"], 
            shapefile_year=shapefile_years_list
        ),
        f"data/input/satellite_pm25/{satellite_pm25_cfg['zipname']}/{satellite_pm25_cfg['file_prefix']}." + "{year}01-{year}12.nc"
    output:
        "data/output/satellite_pm25_raster2polygon/satellite_pm25_{shape}_{year}.parquet"
    shell:
        "python src/aggregate_pm25.py satellite_pm25=annual year={params.year}"

rule aggregate_monthly_pm25:
    params:
        year = "{year}",
        month = "{month}"
    input:
        expand("data/input/shapefiles/shapefile_{polygon_name}_{shapefile_year}/shapefile.shp", 
            polygon_name=config["shapefile_polygon_name"], 
            shapefile_year=shapefile_years_list
        ),
        f"data/input/satellite_pm25/{satellite_pm25_cfg['zipname']}/{satellite_pm25_cfg['file_prefix']}." + "{year}{month}-{year}{month}.nc"
    output:
        "data/output/satellite_pm25_raster2polygon/satellite_pm25_{shape}_{year}_{month}.parquet"
    shell:
        "python src/aggregate_pm25.py satellite_pm25=monthly year={params.year} month={params.month}"

