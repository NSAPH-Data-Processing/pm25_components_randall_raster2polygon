import xarray
import rasterio
import rasterstats
import pandas as pd
import geopandas as gpd
import numpy as np
import hydra
from hydra.core.hydra_config import HydraConfig
import logging  

# configure logger to print at info level
logging.basicConfig(level=logging.INFO)
LOGGER = logging.getLogger(__name__)

def available_shapefile_year(year, shapefile_years_list: list):
    """
    Given a list of shapefile years,
    return the latest year in the shapefile_years_list that is less than or equal to the given year
    """
    available_year = max(shapefile_years_list)  # Returns the last element if year is greater than the last element

    for shapefile_year in sorted(shapefile_years_list):
        if year < shapefile_year:
            break
        available_year = shapefile_year

    return available_year

@hydra.main(config_path="../conf", config_name="config", version_base=None)
def main(cfg):
    # get shape name from config defaults
    shape = HydraConfig.get().runtime.choices.shapefiles
    LOGGER.info(f"Running satellite_pm25_raster2polygon for shape: {shape}")

    # == load netcdf file
    path = f"data/input/satellite_pm25/{cfg.satellite_pm25.zipname}/{cfg.satellite_pm25.file_prefix}.{cfg.year}01-{cfg.year}12.nc"
    ds = xarray.open_dataset(path)
    layer = getattr(ds, cfg.satellite_pm25.layer)

    # == compute zonal stats of layer

    # obtain affine transform/boundaries
    dims = layer.dims
    assert len(dims) == 2, "netcdf coordinates must be 2d"
    lon = layer[cfg.satellite_pm25.longitude_layer].values
    lat = layer[cfg.satellite_pm25.latitude_layer].values
    transform = rasterio.transform.from_origin(
        lon[0], lat[-1], lon[1] - lon[0], lat[1] - lat[0]
    )

    # == load shapefile
    shapefile_years_list = list(cfg.shapefiles[cfg.shapefile_polygon_name].keys())
    #use previously available shapefile
    shapefile_year = available_shapefile_year(cfg.year, shapefile_years_list)

    shape_path = f'data/input/shapefiles/shapefile_{cfg.shapefile_polygon_name}_{shapefile_year}/shapefile.shp'
    polygon = gpd.read_file(shape_path)

    # compute zonal stats
    stats = rasterstats.zonal_stats(
        polygon,
        layer.values[::-1],
        stats="mean",
        affine=transform,
        all_touched=True,
        #geojson_out=True,
        nodata=np.nan #if cfg.job.nodata == "nan" else cfg.job.nodata,
    )
    #gdf = gpd.GeoDataFrame.from_features(stats)
    df = pd.DataFrame(stats, index=polygon[cfg.shapefiles[cfg.shapefile_polygon_name][shapefile_year].idvar])

    # == format dataframe
    df = df.rename(columns={"mean": "pm25"}) #df = df.rename(columns={"mean": cfg.satellite_pm25.layer})
    df.index.name = shape
    df["year"] = cfg.year

    # == save output file
    output_file = f"data/output/satellite_pm25_raster2polygon/satellite_pm25_{shape}_{cfg.year}.parquet"
    df.to_parquet(output_file)

if __name__ == "__main__":
    main()