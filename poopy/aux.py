import json
from typing import Tuple

import numpy as np
from osgeo import gdal
from geojson import Feature, FeatureCollection, LineString
from landlab import RasterModelGrid


def geographic_coords_to_model_xy(
    xy_coords: Tuple[float, float], ds: gdal.Dataset
) -> Tuple[float, float]:
    """Converts geographical coordinates (from lower left) into model grid
    x, y indices (i.e., # cells from from upper left)"""
    trfm = ds.GetGeoTransform()
    xy_of_upper_left = (
        trfm[0],
        trfm[3],
    )
    x = (xy_coords[0] - xy_of_upper_left[0]) / trfm[1]
    y = (xy_coords[1] - xy_of_upper_left[1]) / trfm[5]
    return x, y


def model_xy_to_geographic_coords(
    xy_coords: Tuple[float, float], ds: gdal.Dataset
) -> Tuple[float, float]:
    """Converts model grid x, y indices (i.e., # cells from upper left) into
    geographical coordinates (from lower left)"""
    trfm = ds.GetGeoTransform()
    xy_of_upper_left = (
        trfm[0],
        trfm[3],
    )
    x = xy_coords[0] * trfm[1] + xy_of_upper_left[0]
    y = xy_coords[1] * trfm[5] + xy_of_upper_left[1]
    return x, y

def ids_to_xyz(
    node_ids: np.ndarray, grid: RasterModelGrid, field: str
) -> Tuple[np.ndarray, np.ndarray, np.ndarray]:
    """Converts list of node ids into arrays of model x, y coordinates
    and values of a given field"""

    model_ys, model_xs = np.unravel_index(node_ids, grid.shape)
    xs, ys = model_xy_to_geographic_coords((model_xs, model_ys), grid)
    vals = grid.at_node[field][node_ids]
    return (xs, ys, vals)


def save_json(object, filename: str) -> None:
    """Saves a (geo)json object to file"""
    f = open(filename, "w")
    json.dump(object, f)
