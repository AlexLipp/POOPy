"""
Module for accumulating flow on a D8 flow grid. This class can be used to calculate drainage area and discharge,
and to accumulate any other tracer across a drainage network. 

Builds a network of nodes from a D8 flow grid. Uses a stack-based algorithm to traverse the network in topological order, 
modified from Braun & Willet (2013) DOI: 10.1016/j.geomorph.2012.10.008. This is faster than the recursive algorithm used in 
original Landlab implementation as we use an iterative build_stack algorithm (much faster). Most of the code is written 
in Cython for speed. The approach is linear w.r.t. the number of nodes in the network. Class is designed to be used with 
geospatial rasters, but can also be used with a numpy array of D8 flow directions. 

## Installation 

To install run: 
python setup.py build_ext --inplace

## Example of use 

>>> from d8_accumulator import D8Accumulator, write_geotiff
>>> import numpy as np 
>>> accumuator = D8Accumulator("d8.tif")
>>> # Create an array of cell areas
>>> cell_area = np.ones(len(accumulator.receivers)) * 100 # 100 m^2 cell area
>>> # Calculate drainage area in m^2
>>> drainage_area = accumulator.accumulate(weights=cell_area)
>>> # Calculate number of upstream nodes
>>> number_nodes = accumulator.accumulate()
>>> # Write the results to a geotiff
>>> write_geotiff("drainage_area.tif", drainage_area, accumulator.ds)

"""

import warnings
from typing import Tuple

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
    stack : np.ndarray
        Array of nodes in order of upstream to downstream (breadth-first)
    arr : np.ndarray
        Array of D8 flow directions
    ds : gdal.Dataset
        GDAL Dataset object of the D8 flow grid. If the array is manually set, this will be None

    Methods
    -------
    accumulate(weights : np.ndarray = None)
        Accumulate flow on the grid using the D8 flow directions


    Examples
    --------
    >>> accumuator = D8Accumulator("d8.tif")
    >>> # Create an array of cell areas
    >>> cell_area = np.ones(len(accumulator.receivers)) * 100 # 100 m^2 cell area
    >>> # Calculate drainage area in m^2
    >>> drainage_area = accumulator.accumulate(weights=cell_area)
    >>> # Calculate number of upstream nodes
    >>> number_nodes = accumulator.accumulate()
    >>> # Write the results to a geotiff
    >>> write_geotiff("drainage_area.tif", drainage_area, accumulator.ds)
    """

    def __init__(self, filename: str):
        """
        Parameters
        ----------
        filename : str
            Path to the D8 flow grid
        """
        self._arr, self._ds = read_geo_file(filename)
        self._arr = self._arr.astype(int)
        self._receivers = cf.d8_to_receivers(self.arr)
        self._baselevel_nodes = np.where(
            self.receivers == np.arange(len(self.receivers))
        )[0]
        self._stack = cf.build_stack_iterative(self.receivers, self.baselevel_nodes)

    def accumulate(self, weights: np.ndarray = None) -> np.ndarray:
        """Accumulate flow on the grid using the D8 flow directions

        Parameters
        ----------
        weights : np.ndarray, optional
            Array of weights for each node, defaults to giving each node a weight of 1, resulting in a map of the number of upstream nodes.
            If the area of each node is known, this can be used to calculate drainage area. If run-off at each node is known,
            this can be used to calculate discharge.
        """

        if weights is None:
            # If no weights are passed, assume all nodes have equal weight of 1.
            # Output is a 1D array of # upstream nodes
            weights = np.ones(len(self.receivers))

        # Check that weights are the right length
        if len(weights) != len(self.receivers):
            raise ValueError("Weights must be the same length as the number of nodes")
        return cf.accumulate_flow(self.receivers, self.stack, weights=weights).reshape(
            self._arr.shape
        )

    @property
    def receivers(self) -> np.ndarray:
        """Array of receiver nodes (i.e., the ID of the node that receives the flow from the i'th node)"""
        return np.asarray(self._receivers)

    @property
    def baselevel_nodes(self) -> np.ndarray:
        """Array of baselevel nodes (i.e., nodes that do not donate flow to any other nodes)"""
        return self._baselevel_nodes

    @property
    def stack(self) -> np.ndarray:
        """Array of nodes in order of upstream to downstream"""
        return np.asarray(self._stack)

    @property
    def arr(self):
        """Array of D8 flow directions"""
        return self._arr

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
        self._stack = cf.build_stack_iterative(self.receivers, self.baselevel_nodes)

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
        instance._stack = cf.build_stack_iterative(
            instance.receivers, instance.baselevel_nodes
        )
        return instance
