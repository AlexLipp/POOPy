from setuptools import setup, find_packages
from setuptools.extension import Extension
from Cython.Build import cythonize
import numpy

extensions = [
    Extension(
        "cfuncs",
        ["poopy/cfuncs.pyx"],
        language="c++",  # Use C++ compiler
    )
]

setup(
    name="poopy",
    version="0.2",
    ext_modules=cythonize(extensions),
    include_dirs=[numpy.get_include()],
    packages=find_packages(),
    author="Alex Lipp",
    author_email="alexander.lipp@merton.ox.ac.uk",
    description="An Object Oriented Python package for working with English Water Companies Event Duration Monitoring live data.",
    install_requires=[
        "Cython",
        "matplotlib",
        "numpy",
        "pandas",
        "pooch",
        "geojson",
        "gdal",  # osgeo package is usually installed via the GDAL package
        "requests",
    ],
)
