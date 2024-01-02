import xarray
import rasterio
import rasterstats
import pandas as pd
import geopandas as gpd
import numpy as np

ds = xarray.open_dataset("data/satellite_pm25/V5GL04.HybridPM25.NorthAmerica.202201-202212.nc")
layer = getattr(ds, 'GWRPM25')

# == compute zonal stats of layer

# obtain affine transform/boundaries
dims = layer.dims
assert len(dims) == 2, "netcdf coordinates must be 2d"
lon = layer['lon'].values
lat = layer['lat'].values
transform = rasterio.transform.from_origin(
    lon[0], lat[-1], lon[1] - lon[0], lat[1] - lat[0]
)

# == load shapefile from 
shape_path = 'data/shapefile_cb_county_2015/shapefile.shp'
polygon = gpd.read_file(shape_path)

# compute zonal stats
stats = rasterstats.zonal_stats(
    shape_path,
    layer.values[::-1],
    stats="mean",
    affine=transform,
    all_touched=True,
    #geojson_out=True,
    nodata=np.nan #if cfg.job.nodata == "nan" else cfg.job.nodata,
)
#gdf = gpd.GeoDataFrame.from_features(stats)
df = pd.DataFrame(stats, index=polygon.GEOID)

df.to_csv('data/satellite_pm25_raster2polygon/county_pm25.csv')
