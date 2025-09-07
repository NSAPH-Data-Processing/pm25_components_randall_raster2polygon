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
    LOGGER.info(f"Running aggregation for: {cfg.component} {cfg.temporal_freq} {cfg.polygon_name} {cfg.year}")
    logging_dir = HydraConfig.get().runtime.output_dir

    # == load shapefile
    LOGGER.info("Loading shapefile.")
    shapefile_years_list = list(cfg.shapefiles[cfg.polygon_name].keys())
    shapefile_year = available_shapefile_year(cfg.year, shapefile_years_list)

    shape_path = f'data/input/shapefiles/shapefile_{cfg.polygon_name}_{shapefile_year}/shapefile.shp'
    polygon = gpd.read_file(shape_path)
    polygon_ids = polygon[cfg.shapefiles[cfg.polygon_name][shapefile_year].idvar].values

    # == filenames to be aggregated for this component
    component_path = pathlib.Path(f"data/input/pm25_components__randall/{cfg.temporal_freq}/{cfg.component}/")
    if not component_path.exists():
        LOGGER.error(f"Component path {component_path} does not exist.")
        return

    filenames = component_path.glob("*.nc")
    filenames = [str(f) for f in filenames if f.is_file()]

    if not filenames:
        LOGGER.error(f"No files found for component {cfg.component}.")
        return

    # == compute mapping from vector geometries to raster cells (only once per component)
    LOGGER.info(f"Mapping polygons to raster cells for {cfg.component}.")

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

    # Store component data for all files
    component_data = []

    # == aggregate for all the files using the same mapping
    for i, filename in enumerate(filenames):
        
        # Extract year from filename
        match = re.search(r"(20\d{2})(?:-\1-|\d{3}-\1\d{3})", filename)
        file_year = match.group(1) if match else None
        if not file_year:
            raise ValueError(f"Filename {filename} does not contain a valid year.")
        
        # Only process files for the requested year
        if int(file_year) != cfg.year:
            continue
            
        if cfg.temporal_freq == "monthly":
            match = re.search(r"(?<!\d)(JAN|FEB|MAR|APR|MAY|JUN|JUL|AUG|SEP|OCT|NOV|DEC)(?!\w)", filename)
            month_abbr = match.group(0) if match else None
            if not month_abbr:
                raise ValueError(f"Filename {filename} does not contain a valid month abbreviation.")
            month = month_map[month_abbr]
        else:
            month = None
            
        LOGGER.info(f"Aggregating {filename} as {cfg.temporal_freq} for year {file_year} month {month if cfg.temporal_freq == 'monthly' else 'N/A'}")

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

        df_data = {
            cfg.component: stats, 
            "year": int(file_year),
            cfg.polygon_name: polygon_ids
        }
        
        if cfg.temporal_freq == "monthly":
            df_data["month"] = int(month)
            
        component_data.append(pd.DataFrame(df_data))

    # concatenate all data (necessary for monthly files to combine all months)
    if component_data:
        final_df = pd.concat(component_data, ignore_index=True)
    else:
        LOGGER.error(f"No data processed for component {cfg.component}!")
        return

    # == save individual component output file
    output_dir = f"data/intermediate/pm25_components__randall/{cfg.temporal_freq}/{cfg.component}/"
    output_filename = f"{output_dir}{cfg.component}__{cfg.polygon_name}_{cfg.temporal_freq}_{cfg.year}.parquet"

    os.makedirs(output_dir, exist_ok=True)

    output_path = os.path.abspath(output_filename)
    LOGGER.info(f"Saving component output to {output_path}")
    LOGGER.info(f"Component dataset shape: {final_df.shape}")
    LOGGER.info(f"Columns: {list(final_df.columns)}")
    
    # save to parquet
    final_df.to_parquet(output_path, index=False)

    LOGGER.info(f"Successfully created component file: {output_path}")


if __name__ == "__main__":
    main()
