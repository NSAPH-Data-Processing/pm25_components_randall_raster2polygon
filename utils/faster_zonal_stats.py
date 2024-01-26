# version of zonal_stats based on rasterio (https://github.com/perrygeo/python-rasterstats/blob/master/src/rasterstats/main.py)
# but with some modifications to make reuse the same rasterization of polygons for various rasters.
# maybe a PR to rasterstats is in order?

# Copyright (c) 2013 Matthew Perry
# All rights reserved.

import warnings
import numpy as np
from tqdm import tqdm
from affine import Affine
from shapely.geometry import shape


from rasterstats.io import Raster, read_features, bounds_window
from rasterstats.utils import boxify_points, rasterize_geom


def polygon_to_raster_cells(
    vectors,
    raster,
    layer=0,
    band=1,
    nodata=None,
    affine=None,
    all_touched=False,
    boundless=True,
    verbose=False,
    **kwargs,
):
    """Returns an index map for each vector geometry to indices in the raster source.

    Parameters
    ----------
    vectors: path to an vector source or geo-like python objects

    raster: ndarray or path to a GDAL raster source
        If ndarray is passed, the ``affine`` kwarg is required.

    layer: int or string, optional
        If `vectors` is a path to a fiona source,
        specify the vector layer to use either by name or number.
        defaults to 0

    band: int, optional
        If `raster` is a GDAL source, the band number to use (counting from 1).
        defaults to 1.

    nodata: float, optional
        If `raster` is a GDAL source, this value overrides any NODATA value
        specified in the file's metadata.
        If `None`, the file's metadata's NODATA value (if any) will be used.
        defaults to `None`.

    affine: Affine instance
        required only for ndarrays, otherwise it is read from src

    all_touched: bool, optional
        Whether to include every raster cell touched by a geometry, or only
        those having a center point within the polygon.
        defaults to `False`

    prefix: string
        add a prefix to the keys (default: None)

    Returns
    -------
    dict
        A dictionary mapping vector the ids of geometries to locations (indices) in the raster source.
    """

    # Handle 1.0 deprecations
    transform = kwargs.get("transform")
    if transform:
        warnings.warn(
            "GDAL-style transforms will disappear in 1.0. "
            "Use affine=Affine.from_gdal(*transform) instead",
            DeprecationWarning,
        )
        if not affine:
            affine = Affine.from_gdal(*transform)

    cp = kwargs.get("copy_properties")
    if cp:
        warnings.warn(
            "Use `geojson_out` to preserve feature properties", DeprecationWarning
        )

    band_num = kwargs.get("band_num")
    if band_num:
        warnings.warn("Use `band` to specify band number", DeprecationWarning)
        band = band_num

    cell_map = []

    rast = Raster(raster, affine, nodata, band)

    with Raster(raster, affine, nodata, band) as rast:
        features_iter = read_features(vectors, layer)


        for feat in tqdm(features_iter, disable=(not verbose)):
            geom = shape(feat["geometry"])

            if "Point" in geom.geom_type:
                geom = boxify_points(geom, rast)

            geom_bounds = tuple(geom.bounds)
            fsrc = rast.read(bounds=geom_bounds, boundless=boundless)

            # rasterized geometry
            rv_array = rasterize_geom(geom, like=fsrc, all_touched=all_touched)

            # nodata mask
            isnodata = fsrc.array == fsrc.nodata

            # add nan mask (if necessary)
            has_nan = np.issubdtype(fsrc.array.dtype, np.floating) and np.isnan(
                fsrc.array.min()
            )
            if has_nan:
                isnodata = isnodata | np.isnan(fsrc.array)

            indices = np.nonzero(rv_array & ~isnodata)

            # add row and col start
            (row_start, _), (col_start, _) = bounds_window(geom.bounds, affine)
            indices = (indices[0] + row_start, indices[1] + col_start)
            cell_map.append(indices)

        return cell_map
