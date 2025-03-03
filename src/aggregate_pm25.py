import xarray
import rasterio
import pandas as pd
import geopandas as gpd
import numpy as np
import hydra
import logging  
import matplotlib.pyplot as plt

from hydra.core.hydra_config import HydraConfig
from utils.faster_zonal_stats import polygon_to_raster_cells


# configure logger to print at info level
logging.basicConfig(level=logging.INFO)
LOGGER = logging.getLogger(__name__)


def available_shapefile_year(year, shapefile_years_list: list):
    """
    Given a list of shapefile years,
    return the latest year in the shapefile_years_list that is less than or equal to the given year
    """
    for shapefile_year in sorted(shapefile_years_list, reverse=True):
        if year >= shapefile_year:
            return shapefile_year
 
    return min(shapefile_years_list)  # Returns the last element if year is greater than the last element

@hydra.main(config_path="../conf", config_name="config", version_base=None)
def main(cfg):
    # get aggregation defaults
    LOGGER.info(f"Running for: {cfg.temporal_freq} {cfg.polygon_name} {cfg.year}")
    logging_dir = HydraConfig.get().runtime.output_dir

    # == load shapefile
    LOGGER.info("Loading shapefile.")
    shapefile_years_list = list(cfg.shapefiles[cfg.polygon_name].keys())
    #use previously available shapefile
    shapefile_year = available_shapefile_year(cfg.year, shapefile_years_list)

    shape_path = f'data/input/shapefiles/shapefile_{cfg.polygon_name}_{shapefile_year}/shapefile.shp'
    polygon = gpd.read_file(shape_path)
    polygon_ids = polygon[cfg.shapefiles[cfg.polygon_name][shapefile_year].idvar].values

    # == filenames to be aggregated
    if cfg.temporal_freq == "annual":
        filenames = [
            f"{cfg.satellite_pm25[cfg.temporal_freq].file_prefix}.{cfg.year}01-{cfg.year}12.nc"
        ]
    elif cfg.temporal_freq == "monthly": 
        # Note; will use the january file for obtaining the mapping from geometries to raster cells
        # the aggregation for the all the months will be done using the same mapping later
        filenames = []
        for m in range(1, 13):
            filenames.append(f"{cfg.satellite_pm25[cfg.temporal_freq].file_prefix}.{cfg.year}{m:02d}-{cfg.year}{m:02d}.nc")
    else:
        raise ValueError(f"temporal_freq {cfg.temporal_freq} not supported")
    
    # == compute mapping from vector geometries to raster cells

    # load the first file to obtain the affine transform/boundaries
    LOGGER.info("Mapping polygons to raster cells.")

    ds = xarray.open_dataset(f"data/input/pm25__washu__raw/{cfg.temporal_freq}/{filenames[0]}")
    layer = getattr(ds, cfg.satellite_pm25.layer)

    # obtain affine transform/boundaries
    dims = layer.dims
    assert len(dims) == 2, "netcdf coordinates must be 2d"
    lon = layer[cfg.satellite_pm25.longitude_layer].values
    lat = layer[cfg.satellite_pm25.latitude_layer].values
    transform = rasterio.transform.from_origin(
        lon[0], lat[-1], lon[1] - lon[0], lat[1] - lat[0]
    )

    # compute mapping
    poly2cells = polygon_to_raster_cells(
        polygon,
        layer.values[::-1],
        affine=transform,
        all_touched=True,
        nodata=np.nan,
        verbose=cfg.show_progress,
    )

    # == aggregate for all the files using the same mapping
    for i, filename in enumerate(filenames):
        LOGGER.info(f"Aggregating {filename}")

        if i > 0:
            # reload the file only if it is different from the first one
            ds = xarray.open_dataset(f"data/input/pm25__washu__raw/{cfg.temporal_freq}/{filename}")
            layer = getattr(ds, cfg.satellite_pm25.layer)

        # === obtain stats quickly using precomputed mapping
        stats = []
        for indices in poly2cells:
            if len(indices[0]) == 0:
                # no cells found for this polygon
                stats.append(np.nan)
                continue
            cells = layer.values[::-1][indices]
            stats.append(np.nanmean(cells))

        df = pd.DataFrame(
            {"pm25": stats, "year": cfg.year},
            index=pd.Index(polygon_ids, name=cfg.polygon_name)
        )

        # == save output file
        if cfg.temporal_freq == "annual":
            # ignore month since len(filenames) == 1
            output_filename = f"pm25__washu__{cfg.polygon_name}_{cfg.temporal_freq}__{cfg.year}.parquet"

        elif cfg.temporal_freq == "monthly":
            # use month in filename since len(filenames) = 12
            month = f"{i + 1:02d}"
            df["month"] = month
            output_filename = f"pm25__washu__{cfg.polygon_name}_{cfg.temporal_freq}__{cfg.year}_{month}.parquet"

        output_path = f"data/output/pm25__washu/{cfg.polygon_name}_{cfg.temporal_freq}/{output_filename}"
        df.to_parquet(output_path)

        # plot aggregation map using geopandas
        if cfg.plot_output:
            LOGGER.info("Plotting result...")
            gdf = gpd.GeoDataFrame(df, geometry=polygon.geometry.values, crs=polygon.crs)
            png_path = f"{logging_dir}/{output_filename}.png"
            gdf.plot(column="pm25", legend=True)
            plt.savefig(png_path)


if __name__ == "__main__":
    main()
