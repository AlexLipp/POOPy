from landlab import RasterModelGrid
from typing import Tuple
import numpy as np
import json
from geojson import FeatureCollection, Feature, LineString
from osgeo import osr


def geographic_coords_to_model_xy(
    xy_coords: Tuple[float, float], grid: RasterModelGrid
) -> Tuple[float, float]:
    """Converts geographical coordinates (from lower left) into RasterModelGrid
    coordinates (from upper left)"""
    xy_of_upper_left = (
        grid.xy_of_lower_left[0],
        grid.xy_of_lower_left[1] + grid.dy * grid.shape[0],
    )
    x = (xy_coords[0] - xy_of_upper_left[0]) / grid.dx
    y = (xy_of_upper_left[1] - xy_coords[1]) / grid.dy
    return x, y


def model_xy_to_geographic_coords(
    model_xy_coords: Tuple[float, float], grid: RasterModelGrid
) -> Tuple[float, float]:
    """Converts RasterModelGrid coordinates (from upper left) to geographical coordinates
    (from lower left)"""
    xy_of_upper_left = (
        grid.xy_of_lower_left[0],
        grid.xy_of_lower_left[1] + grid.dy * grid.shape[0],
    )
    x = xy_of_upper_left[0] + model_xy_coords[0] * grid.dx
    y = xy_of_upper_left[1] - model_xy_coords[1] * grid.dy
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


def BNG_to_WGS84_points(
    eastings: np.ndarray, northings: np.ndarray
) -> Tuple[np.ndarray, np.ndarray]:
    """Converts coorindates on British National Grid into Long, Lat on WGS84"""

    OSR_WGS84_REF = osr.SpatialReference()
    OSR_WGS84_REF.ImportFromEPSG(4326)

    OSR_BNG_REF = osr.SpatialReference()
    OSR_BNG_REF.ImportFromEPSG(27700)

    OSR_BNG_to_WGS84 = osr.CoordinateTransformation(OSR_BNG_REF, OSR_WGS84_REF)
    lat_long_tuple_list = OSR_BNG_to_WGS84.TransformPoints(
        np.vstack([eastings, northings]).T
    )
    lat_long_array = np.array(list(map(np.array, lat_long_tuple_list)))
    return (lat_long_array[:, 1], lat_long_array[:, 0])


def profiler_data_struct_to_geojson(
    profiler_data_struct, grid: RasterModelGrid, field: str
) -> FeatureCollection:
    """Turns output from ChannelProfiler into a geojson FeatureCollection
    of LineStrings with property corresponding to chosen field"""
    features = []
    for _, segments in profiler_data_struct.items():
        for _, segment in segments.items():
            xs, ys, vals = ids_to_xyz(segment["ids"], grid, field)
            longs, lats = BNG_to_WGS84_points(xs, ys)
            features += [xyz_to_linestring(longs, lats, field, vals[-1])]

    return FeatureCollection(features)


def xyz_to_linestring(xs: np.ndarray, ys: np.ndarray, label: str, value: float):
    """Turns a list of x,y coordinates and a given label, value pair into
    a geojson LineString feature"""
    geom = LineString(coordinates=tuple(zip(xs, ys)))
    prop = {label: value}
    return Feature(geometry=geom, properties=prop)


def save_json(object, filename: str) -> None:
    """Saves a (geo)json object to file"""
    f = open(filename, "w")
    json.dump(object, f)
