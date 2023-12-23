import json
from typing import Tuple

import numpy as np
from osgeo import gdal
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

def save_json(object, filename: str) -> None:
    """Saves a (geo)json object to file"""
    f = open(filename, "w")
    json.dump(object, f)
