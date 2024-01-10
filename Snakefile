import yaml

conda: "requirements.yaml"

# obtain list of shapefile years
defaults_dict = {key: value for d in config['defaults'] if isinstance(d, dict) for key, value in d.items()}
shapefiles_cfg = yaml.safe_load(open(f"conf/shapefiles/{defaults_dict['shapefiles']}.yaml", 'r'))
shapefile_years = shapefiles_cfg[config["shapefile_polygon_name"]].keys()
temporal_freq_cfg = yaml.safe_load(open(f"conf/temporal_freq/{defaults_dict['temporal_freq']}.yaml", 'r'))

# == Define rules ==
rule all:
    input:
        expand("data/output/satellite_pm25_raster2polygon/satellite_pm25_{polygon_name}_{year}.parquet", 
               polygon_name=config["shapefile_polygon_name"], 
               year=list(range(1998, 2022 + 1))), 
        expand("data/output/satellite_pm25_raster2polygon/satellite_pm25_{polygon_name}_{year}{month}.parquet", 
               polygon_name=config["shapefile_polygon_name"], 
               year=list(range(1998, 2022 + 1)), 
               month=[str(i).zfill(2) for i in range(1, 12 + 1)])

rule download_shapefiles:
    output:
        expand("data/input/shapefiles/shapefile_{polygon_name}_{year}/shapefile.{ext}", 
               polygon_name=config["shapefile_polygon_name"], 
               year=shapefile_years, 
               ext = ["shp", "shx", "dbf", "prj", "cpg", "xml"]) 
    shell:
        "python src/download_shapefile.py"

rule download_annual_pm25:
    output:
        expand("data/input/satellite_pm25/{zipname}/{file_prefix}.{year}01-{year}12.nc", 
               zipname=temporal_freq_cfg["zipname"],
               file_prefix=config["file_prefix"],
               year=list(range(1998, 2022 + 1)))
    shell:
        "python src/download_pm25.py time_freq=annual"

rule download_monthly_pm25:
    output:
        expand("data/input/satellite_pm25/{zipname}/{file_prefix}.{year}{month}-{year}{month}.nc", 
               zipname=temporal_freq_cfg["zipname"],
               file_prefix=config["file_prefix"],
               year=list(range(1998, 2022 + 1)),
               month=[str(i).zfill(2) for i in range(1, 12 + 1)])
    shell:
        "python src/download_pm25.py time_freq=monthly"

rule aggregate_annual:
    input:
        expand("data/input/shapefiles/shapefile_{polygon_name}_{year}/shapefile.{ext}", 
               polygon_name=config["shapefile_polygon_name"], 
               year=shapefile_years, 
               ext = ["shp", "shx", "dbf", "prj", "cpg", "xml"]), 
        expand("data/input/satellite_pm25/{zipname}/{file_prefix}.{year}01-{year}12.nc", 
                zipname=temporal_freq_cfg["zipname"],
               file_prefix=config['file_prefix'],
               year=list(range(1998, 2022 + 1)))
    output:
        expand("data/output/satellite_pm25_raster2polygon/satellite_pm25_{polygon_name}_{year}.parquet", 
               polygon_name=config["shapefile_polygon_name"], 
               year=list(range(1998, 2022 + 1)))
    shell:
        "python src/aggregate_annual.py time_freq=annual"

rule aggregate_monthly:
    input:
        expand("data/input/shapefiles/shapefile_{polygon_name}_{year}/shapefile.{ext}", 
               polygon_name=config["shapefile_polygon_name"], 
               year=shapefile_years, 
               ext = ["shp", "shx", "dbf", "prj", "cpg", "xml"]), 
        expand("data/input/satellite_pm25/{zipname}/{file_prefix}.{year}{month}-{year}{month}.nc", 
                zipname=temporal_freq_cfg["zipname"],
                file_prefix=config['file_prefix'],
                year=list(range(1998, 2022 + 1)), 
                month=[str(i).zfill(2) for i in range(1, 12 + 1)])
    output:
        expand("data/output/satellite_pm25_raster2polygon/satellite_pm25_{polygon_name}_{year}{month}.parquet", 
               polygon_name=config["shapefile_polygon_name"], 
               year=list(range(1998, 2022 + 1)), 
               month=[str(i).zfill(2) for i in range(1, 12 + 1)])
    shell:
        "python src/aggregate_annual.py time_freq=monthly"
