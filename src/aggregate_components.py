import xarray
import rasterio
import pandas as pd
import geopandas as gpd
import numpy as np
import hydra
import logging  
import pathlib
import os
import re
import matplotlib.pyplot as plt

from hydra.core.hydra_config import HydraConfig
from utils.faster_zonal_stats import polygon_to_raster_cells


# configure logger to print at info level
logging.basicConfig(level=logging.INFO)
LOGGER = logging.getLogger(__name__)

# mapping for month abbreviations to numbers
month_map = {
    "JAN": "01", "FEB": "02", "MAR": "03", "APR": "04",
    "MAY": "05", "JUN": "06", "JUL": "07", "AUG": "08",
    "SEP": "09", "OCT": "10", "NOV": "11", "DEC": "12"
}


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
    LOGGER.info(f"Running for: {cfg.temporal_freq} {cfg.polygon_name} {cfg.component}")
    logging_dir = HydraConfig.get().runtime.output_dir

    # == load shapefile
    LOGGER.info("Loading shapefile.")
    shapefile_years_list = list(cfg.shapefiles[cfg.polygon_name].keys())
    #use previously available shapefile
    shapefile_year = available_shapefile_year(cfg.year, shapefile_years_list)

    shape_path = f'data/input/shapefiles/shapefile_{cfg.polygon_name}_{shapefile_year}/shapefile.shp'
    polygon = gpd.read_file(shape_path)
    polygon_ids = polygon[cfg.shapefiles[cfg.polygon_name][shapefile_year].idvar].values

    # from omegaconf import OmegaConf
    # print(OmegaConf.to_yaml(cfg))
    # import sys; sys.exit(0)  # exit here to avoid running the rest of the code

    # == filenames to be aggregated
    filenames = pathlib.Path(f"data/input/satellite_components/{cfg.component}/{cfg.temporal_freq}/").glob("*.nc")
    filenames = [str(f) for f in filenames if f.is_file()]
    
    # == compute mapping from vector geometries to raster cells

    # load the first file to obtain the affine transform/boundaries
    LOGGER.info("Mapping polygons to raster cells.")

    ds = xarray.open_dataset(filenames[0])
    layer = getattr(ds, cfg.satellite_component.component[cfg.component].layer)

    # obtain affine transform/boundaries
    dims = layer.dims
    assert len(dims) == 2, "netcdf coordinates must be 2d"
    lon = layer[cfg.satellite_component.longitude_layer].values
    lat = layer[cfg.satellite_component.latitude_layer].values
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
        
        match = re.search(r"(?<!\d)(20\d{2})(?=\d{3})", filename)
        year = match.group(0) if match else None
        if not year:
            raise ValueError(f"Filename {filename} does not contain a valid year.")
        
        if cfg.temporal_freq == "monthly":
            match = re.search(r"(?<!\d)(JAN|FEB|MAR|APR|MAY|JUN|JUL|AUG|SEP|OCT|NOV|DEC)(?!\w)", filename)
            month_abbr = match.group(0) if match else None
            if not month_abbr:
                raise ValueError(f"Filename {filename} does not contain a valid month abbreviation.")
            month = month_map[month_abbr]
        else:
            month = None
        LOGGER.info(f"Aggregating {filename} as {cfg.temporal_freq} for year {year} month {month if cfg.temporal_freq == 'monthly' else 'N/A'}")

        if i > 0:
            # reload the file only if it is different from the first one
            ds = xarray.open_dataset(filenames[i])
            layer = getattr(ds, cfg.satellite_component.component[cfg.component].layer)

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
            {cfg.component: stats, "year": cfg.year},
            index=pd.Index(polygon_ids, name=cfg.polygon_name)
        )

        if cfg.temporal_freq == "monthly":
            # add month to the dataframe
            df["month"] = cfg.month
        else:
            # for yearly aggregation, we don't need to add month
            df["month"] = np.nan

        # == save output file
        if cfg.temporal_freq == "yearly":
            output_filename = f"data/output/satellite_components/{cfg.component}/yearly/{cfg.component}_{cfg.polygon_name}_{cfg.temporal_freq}_{year}.parquet"

        elif cfg.temporal_freq == "monthly":
            output_filename = f"data/output/satellite_components/{cfg.component}/monthly/{cfg.component}_{cfg.polygon_name}_{cfg.temporal_freq}_{year}-{month}.parquet"

        output_path = os.path.abspath(output_filename)
        # ensure directory exists
        os.makedirs(os.path.dirname(output_path), exist_ok=True)

        LOGGER.info(f"Saving output to {output_path}")
        # save to parquet
        if os.path.exists(output_path):
            LOGGER.warning(f"Output file {output_path} already exists. Skipping.")
        else:
            LOGGER.info(f"Output file {output_path} does not exist. Creating new file.")
            df.to_parquet(output_path)

        # plot aggregation map using geopandas
        if cfg.plot_output:
            LOGGER.info("Plotting result...")
            gdf = gpd.GeoDataFrame(df, geometry=polygon.geometry.values, crs=polygon.crs)
            png_path = output_path.replace(".parquet", ".png")
            gdf.plot(column=cfg.component, legend=True)
            plt.savefig(png_path)


if __name__ == "__main__":
    main()
