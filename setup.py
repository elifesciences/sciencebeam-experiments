import os

from setuptools import (
  find_packages,
  setup
)

from Cython.Build import cythonize

with open(os.path.join('requirements.txt'), 'r') as f:
  REQUIRED_PACKAGES = f.readlines()

packages = find_packages()

setup(
  name='sciencebeam_lab',
  version='0.0.1',
  install_requires=REQUIRED_PACKAGES,
  packages=packages,
  include_package_data=True,
  description='ScienceBeam Lab',
  ext_modules=cythonize("sciencebeam_lab/alignment/align_fast_utils.pyx")
)
