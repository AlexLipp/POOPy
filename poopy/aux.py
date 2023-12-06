from landlab import RasterModelGrid
from typing import Tuple


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
