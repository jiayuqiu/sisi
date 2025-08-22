# setup_geo_cython.py
from setuptools import setup, Extension
from Cython.Build import cythonize
import numpy

setup(
    ext_modules=cythonize([
        Extension(
            "geo_cython",
            ["geo_cython.pyx"],
            include_dirs=[numpy.get_include()],
        )
    ]),
)