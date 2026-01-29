# ruff: noqa: D100
import numpy
from Cython.Build import cythonize
from setuptools import find_packages, setup
from setuptools.extension import Extension

extensions = [
    Extension(
        "cfuncs",
        ["poopy/cfuncs.pyx"],
        language="c++",  # Use C++ compiler
    )
]

setup(
    name="poopy",
    version="0.5.6",
    ext_modules=cythonize(extensions),
    include_dirs=[numpy.get_include()],
    packages=find_packages(),
    author="Alex Lipp",
    author_email="a.lipp@ucl.ac.uk",
    description="An Object Oriented Python package for working with English Water Companies Event Duration Monitoring live data.",
    install_requires=[
        "Cython",
        "matplotlib",
        "numpy",
        "pandas",
        "pooch",
        "pytest",
        "geojson",
        "geopandas",
        "gdal",  # osgeo package is usually installed via the GDAL package
        "requests",
        "shapely",
    ],
)
