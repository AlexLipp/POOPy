"""
Module for accumulating flow on a D8 flow grid. This class can be used to calculate drainage area and discharge,
and to accumulate any other tracer across a drainage network. 

Builds a network of nodes from a D8 flow grid. Uses a queue-based algorithm to traverse the network in topological order, 
modified from Braun & Willet (2013) DOI: 10.1016/j.geomorph.2012.10.008. This is faster than the recursive algorithm used in 
original Landlab implementation as we use an iterative build_ordered_list algorithm (much faster). Most of the code is written 
in Cython for speed. The approach is linear w.r.t. the number of nodes in the network. Class is designed to be used with 
geospatial rasters, but can also be used with a numpy array of D8 flow directions with some loss of functionality. 
"""

import warnings
from typing import Tuple, List, Union

from geojson import MultiLineString
import json
import numpy as np
from osgeo import gdal

import cfuncs as cf


def read_geo_file(filename: str) -> Tuple[np.ndarray, gdal.Dataset]:
    """Reads a geospatial file"""
    ds = gdal.Open(filename)
    band = ds.GetRasterBand(1)
    arr = band.ReadAsArray()
    return arr, ds


def write_geotiff(filename: str, arr: np.ndarray, ds: gdal.Dataset):
    """Writes a numpy array to a geotiff"""
    if arr.dtype == np.float32:
        arr_type = gdal.GDT_Float32
    else:
        arr_type = gdal.GDT_Int32

    driver = gdal.GetDriverByName("GTiff")
    out_ds = driver.Create(filename, arr.shape[1], arr.shape[0], 1, arr_type)
    out_ds.SetProjection(ds.GetProjection())
    out_ds.SetGeoTransform(ds.GetGeoTransform())
    band = out_ds.GetRasterBand(1)
    band.WriteArray(arr)
    band.FlushCache()
    band.ComputeStatistics(False)


def write_geojson(filename: str, geojson: dict):
    """Writes a GeoJSON object to a file"""
    with open(filename, "w") as f:
        json.dump(geojson, f)


class D8Accumulator:
    """Class to accumulate flow on a D8 flow grid. This class can be used to calculate drainage area and discharge,
    and to accumulate any other tracer across a drainage network. The class assumes that all boundary
    nodes are sinks (i.e., no flow leaves the grid). This class can be used with any geospatial file that GDAL can read.
    It can also be used with a numpy array of D8 flow directions.

    Parameters
    ----------
    filename : str
        Path to the D8 flow grid geospatial file (e.g., .tif, .asc, etc.)
        This can be to any file that GDAL can read. Expects a single band raster (ignores other bands).
        This raster should be a 2D array of D8 flow directions according to ESRI convention:

            Sink [no flow]= 0
            Right = 1
            Lower right = 2
            Bottom = 4
            Lower left = 8
            Left = 16
            Upper left = 32
            Top = 64
            Upper right = 128

    Attributes
    ----------
    receivers : np.ndarray
        Array of receiver nodes (i.e., the ID of the node that receives the flow from the i'th node)
    baselevel_nodes : np.ndarray
        Array of baselevel nodes (i.e., nodes that do not donate flow to any other nodes)
    order : np.ndarray
        Array of nodes in order of upstream to downstream (breadth-first)
    arr : np.ndarray
        Array of D8 flow directions
    ds : gdal.Dataset
        GDAL Dataset object of the D8 flow grid. If the array is manually set, this will be None
    extent : List[float]
        Extent of the array in the accumulator as [xmin, xmax, ymin, ymax]. Can be used for plotting.

    Methods
    -------
    accumulate(weights : np.ndarray = None)
        Accumulate flow on the grid using the D8 flow directions
    get_channel_segments(field : np.ndarray, threshold : float)
        Get the profile segments of river channels where 'field' is greater than 'threshold'. Used for, e.g., plotting
        the location of a river channel as a line-string.
    get_profile(start_node : int)
        Extract the downstream profile *from* a given node.
    node_to_coord(node : int)
        Converts a node index to a coordinate pair
    coord_to_node(x : float, y : float)
        Converts a coordinate pair to a node index
    """

    def __init__(self, filename: str):
        """
        Parameters
        ----------
        filename : str
            Path to the D8 flow grid
        """
        # Check that filename is a string
        if not isinstance(filename, str):
            raise TypeError("Filename must be a string")
        self._arr, self._ds = read_geo_file(filename)
        self._arr = self._arr.astype(int)
        self._receivers = cf.d8_to_receivers(self.arr)
        self._baselevel_nodes = np.where(
            self.receivers == np.arange(len(self.receivers))
        )[0]
        self._order = cf.build_ordered_list_iterative(
            self.receivers, self.baselevel_nodes
        )

    def accumulate(self, weights: np.ndarray = None) -> np.ndarray:
        """Accumulate flow on the grid using the D8 flow directions

        Parameters
        ----------
        weights : np.ndarray [ndim = 2], optional
            Array of weights for each node, defaults to giving each node a weight of 1, resulting in a map of the number of upstream nodes.
            If the area of each node is known, this can be used to calculate drainage area. If run-off at each node is known,
            this can be used to calculate discharge.

        Returns
        -------
        np.ndarray [ndim = 2]
            Array of accumulated weights (or number of upstream nodes if no weights are passed)
        """
        if weights is None:
            # If no weights are passed, assume all nodes have equal weight of 1.
            # Output is array of # upstream nodes
            weights = np.ones(len(self.receivers))
        else:
            if weights.shape != self.arr.shape:
                raise ValueError("Weights must be have same shape as D8 array")
            weights = weights.flatten()

        return cf.accumulate_flow(self.receivers, self.order, weights=weights).reshape(
            self._arr.shape
        )

    def get_channel_segments(
        self, field: np.ndarray, threshold: float
    ) -> Union[List[List[int]], MultiLineString]:
        """Get the profile segments of river channels where 'field' is greater than 'threshold'. i.e.,
        if 'field' is drainage area, this will return the profile segments of channels with drainage area greater than 'threshold'.
        Generated by topologically traversing the network using a depth-first search algorithm from baselevel nodes, only
        continuing to traverse a node if the value of 'field' is greater than 'threshold'. If the D8 flow grid is a geospatial
        raster, this will return a GeoJSON MultiLineString object of the profile segments. If the D8 flow grid is a numpy array,
        this will return a list of segments of node IDs.

        Parameters
        ----------
        field
            Array of values to get profile segments according to
        threshold
            Threshold value for the profile segments

        Returns
        -------
            - GeoJSON MultiLineString object of the profile segments if the D8 flow grid is a geospatial raster.
            - List of segments of node IDs if the D8 flow grid is a numpy array (and no GDAL Dataset object exists)
        """
        # Nodes where field is greater than threshold
        gteq_thresh = (field > threshold).flatten()
        # Nodes that are baselevel
        is_baselevel = np.asarray(self.receivers) == np.arange(len(self.receivers))
        # Starting nodes are where field is greater than threshold and are also baselevel
        starting_nodes = np.where(np.logical_and(gteq_thresh, is_baselevel))[0]

        n_donors = cf.count_donors(self._receivers)
        delta = cf.ndonors_to_delta(n_donors)
        donors = cf.make_donor_array(self._receivers, delta)
        # Get the profile segments of node IDs
        segments = cf.get_channel_segments(
            starting_nodes, delta, donors, field.flatten(), threshold
        )
        # Convert to x,y indices
        if self.ds is None:
            warnings.warn(
                "\nNo GDAL Dataset object exists. Cannot convert to x,y indices.\nReturning node ID segments"
            )
            return segments
        else:
            geotransform = self.ds.GetGeoTransform()
            ULx = geotransform[0]
            ULy = geotransform[3]
            dx = geotransform[1]
            dy = geotransform[5]
            ncols = self.arr.shape[1]
            coord_segs = cf.id_segments_to_coords_segments(
                segments, ncols, dx, dy, ULx, ULy
            )
            return MultiLineString(coord_segs)

    def get_profile(self, start_node: int) -> Tuple[np.ndarray[int], np.ndarray[float]]:
        """Extract the downstream profile *from* a given node. Returns the profile as a list
        of node IDs in order upstream to downstream. Also returns the distance along the profile
        *counting upstream from the mouth* in the same units as the geospatial file. i.e.,
        a distance of 0 is the mouth of the river, and a distance of 100 is 100 units upstream from the mouth.

        Parameters
        ----------
        start_node : int
            Node ID to start the profile from. Must be a valid node ID.

        Returns
        -------
        Tuple[np.ndarray[int], np.nadrray[float]]
            Tuple of the profile as an array of node IDs and the distance along the profile from the start node.

        Raises
        ------
        ValueError
            If start_node is not a valid node ID
        """
        if start_node < 0 or start_node >= self.arr.size:
            raise ValueError("start_node must be a valid node index")

        dx = self.ds.GetGeoTransform()[1]
        dy = self.ds.GetGeoTransform()[5] * -1
        profile, distance = cf.get_profile(
            start_node, dx, dy, self._receivers, self.arr.flatten()
        )
        # Check length of outputs
        if len(profile) == 0:
            warnings.warn("\nProfile is empty. Returning empty arrays")
            return np.asarray([]), np.asarray([])
        else:
            dstream_dist = np.asarray(distance)
            return np.asarray(profile), np.amax(dstream_dist) - dstream_dist

    def node_to_coord(self, node: int) -> Tuple[float, float]:
        """Converts a node index to a coordinate pair for the centre of the pixel"""
        nrows, ncols = self.arr.shape
        if node > ncols * nrows or node < 0:
            raise ValueError("Node is out of bounds")
        x_ind = node % ncols
        y_ind = node // ncols
        ulx, dx, _, uly, _, dy = self.ds.GetGeoTransform()

        # This gives the coords for the upper left corner of the pixel
        x_coord = ulx + dx * x_ind
        y_coord = uly + dy * y_ind
        # Add dx/2 and dy/2 to get to the center of the pixel from the upper left corner
        x_coord += dx / 2
        y_coord += dy / 2  # recall that dy is negative
        return x_coord, y_coord

    def coord_to_node(self, x: float, y: float) -> int:
        """Converts a coordinate pair to a node index. Returns the node index of the pixel that contains the coordinate"""
        nrows, ncols = self.arr.shape
        ulx, dx, _, uly, _, dy = self.ds.GetGeoTransform()
        x_ind = int((x - ulx) / dx)
        # Casting to int rounds towards zero ('floor' for positive numbers; e.g, int(3.9) = 3)
        y_ind = int((y - uly) / dy)
        out = y_ind * ncols + x_ind
        if out > ncols * nrows or out < 0:
            raise ValueError("Coordinate is out of bounds")
        return out

    @property
    def receivers(self) -> np.ndarray:
        """Array of receiver nodes (i.e., the ID of the node that receives the flow from the i'th node)"""
        return np.asarray(self._receivers)

    @property
    def baselevel_nodes(self) -> np.ndarray:
        """Array of baselevel nodes (i.e., nodes that do not donate flow to any other nodes)"""
        return self._baselevel_nodes

    @property
    def order(self) -> np.ndarray:
        """Array of nodes in order of upstream to downstream"""
        return np.asarray(self._order)

    @property
    def arr(self):
        """Array of D8 flow directions"""
        return self._arr

    @property
    def extent(self) -> List[float]:
        """
        Get the extent of the array in the accumulator. Can be used for plotting.
        """
        trsfm = self.ds.GetGeoTransform()
        minx = trsfm[0]
        maxy = trsfm[3]
        maxx = minx + trsfm[1] * self.arr.shape[1]
        miny = maxy + trsfm[5] * self.arr.shape[0]
        return [minx, maxx, miny, maxy]

    @property
    def ds(self):
        """GDAL Dataset object of the D8 flow grid"""
        if self._ds is None:
            warnings.warn("\nNo GDAL Dataset object exists.")
        return self._ds

    @arr.setter
    def arr(self, arr):
        warnings.warn("\nManually setting the array. Geospatial information lost")
        if len(arr.shape) != 2:
            raise ValueError("D8 Array must be 2D")
        self._arr = arr
        self._ds = None
        self._receivers = cf.d8_to_receivers(self.arr)
        self._baselevel_nodes = np.where(
            self.receivers == np.arange(len(self.receivers))
        )[0]
        self._order = cf.build_ordered_list_iterative(
            self.receivers, self.baselevel_nodes
        )

    @classmethod
    def from_array(cls, arr: np.ndarray):
        """
        Creates a D8Accumulator from a numpy array

        Parameters
        ----------
        arr : np.ndarray
            2D array of D8 flow directions
        """
        if len(arr.shape) != 2:
            raise ValueError("D8 Array must be 2D")
        # Create an instance of the class
        instance = cls.__new__(cls)

        # Initialize attributes
        instance._arr = arr.astype(int)
        instance._ds = None
        instance._receivers = cf.d8_to_receivers(instance.arr)
        instance._baselevel_nodes = np.where(
            instance.receivers == np.arange(len(instance.receivers))
        )[0]
        instance._order = cf.build_ordered_list_iterative(
            instance.receivers, instance.baselevel_nodes
        )
        return instance
