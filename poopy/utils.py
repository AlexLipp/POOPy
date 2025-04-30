"""Utility functions."""

from osgeo import osr


def latlong_to_osgb(lat, lon):
    """
    Convert latitude and longitude to OSGB36 coordinates.

    Args:
        lat: Latitude in WGS84.
        lon: Longitude in WGS84.

    Returns:
        x: OSGB36 easting.
        y: OSGB36 northing.

    """
    # Define the WGS84 spatial reference system
    wgs84 = osr.SpatialReference()
    wgs84.ImportFromEPSG(4326)  # WGS84

    # Define the OSGB36 spatial reference system
    osgb36 = osr.SpatialReference()
    osgb36.ImportFromEPSG(27700)  # OSGB36

    # Create a coordinate transformation
    transform = osr.CoordinateTransformation(wgs84, osgb36)

    # Transform the coordinates
    x, y, _ = transform.TransformPoint(lat, lon)
    return x, y
