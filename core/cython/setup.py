from setuptools import setup
from Cython.Build import cythonize
import numpy

setup(
    name="sisi_geo",
    version="0.0.1",
    ext_modules = cythonize("sisi_geo.pyx", compiler_directives={'language_level': "3"}),
    include_dirs=[numpy.get_include()],
    setup_requires=["Cython", "numpy"]
)