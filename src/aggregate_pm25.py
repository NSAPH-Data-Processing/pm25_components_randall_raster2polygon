import xarray
import rasterio
import rasterstats
import pandas as pd
import geopandas as gpd
import numpy as np
import hydra
import logging  

# configure logger to print at info level
logging.basicConfig(level=logging.INFO)
LOGGER = logging.getLogger(__name__)

def available_shapefile_year(year, shapefile_years_list: list):
    """
    Given a list of shapefile years,
    return the latest year in the shapefile_years_list that is less than or equal to the given year
    """
    for shapefile_year in sorted(shapefile_years_list):
        if year <= shapefile_year:
            return shapefile_year
 
    return max(shapefile_years_list)  # Returns the last element if year is greater than the last element


@hydra.main(config_path="../conf", config_name="config", version_base=None)
def main(cfg):
    # get aggregation defaults
    if cfg.temporal_freq == "annual":
        LOGGER.info(f"Running for: {cfg.temporal_freq} {cfg.polygon_name} {cfg.year}")
    elif cfg.temporal_freq == "monthly":
        LOGGER.info(f"Running for: {cfg.temporal_freq} {cfg.polygon_name} {cfg.year} {cfg.month}")

    # == load netcdf file
    if cfg.temporal_freq == "annual":
        filename = f"{cfg.satellite_pm25[cfg.temporal_freq].file_prefix}.{cfg.year}01-{cfg.year}12.nc"
    elif cfg.temporal_freq == "monthly":
        filename = f"{cfg.satellite_pm25[cfg.temporal_freq].file_prefix}.{cfg.year}{cfg.month}-{cfg.year}{cfg.month}.nc"
    path = f"data/input/satellite_pm25/{cfg.temporal_freq}/{filename}"
    
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
    shapefile_years_list = list(cfg.shapefiles[cfg.polygon_name].keys())
    #use previously available shapefile
    shapefile_year = available_shapefile_year(cfg.year, shapefile_years_list)

    shape_path = f'data/input/shapefiles/shapefile_{cfg.polygon_name}_{shapefile_year}/shapefile.shp'
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
    df = pd.DataFrame(stats, index=polygon[cfg.shapefiles[cfg.polygon_name][shapefile_year].idvar])

    # == format dataframe
    df = df.rename(columns={"mean": "pm25"}) #df = df.rename(columns={"mean": cfg.satellite_pm25.layer})
    df.index.name = cfg.polygon_name
    df["year"] = cfg.year

    # == save output file
    if cfg.temporal_freq == "annual":
        filename = f"satellite_pm25_{cfg.polygon_name}_{cfg.year}.parquet"
    elif cfg.temporal_freq == "monthly":
        filename = f"satellite_pm25_{cfg.polygon_name}_{cfg.year}_{cfg.month}.parquet"
    output_file = f"data/output/satellite_pm25_raster2polygon/{cfg.temporal_freq}/{filename}"

    df.to_parquet(output_file)

if __name__ == "__main__":
    main()
