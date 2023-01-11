#! /usr/bin/env python

# ideas stolen from https://github.com/jgehrcke/python-cmdline-bootstrap

import re

from setuptools import setup, find_packages

version = re.search(
    '^__version__\s*=\s*\'(.*)\'',
    open('slurm_accounting/version.py').read(),
    re.M
    ).group(1)


with open("README.md", "rb") as f:
    long_descr = f.read().decode("utf-8")

setup(name='slurm-accounting',
      version=version,
      description='Slurm accounting helper',
      long_description=long_descr,
      author='Pierre Gay',
      author_email='pierre.gay@u-bordeaux.fr',
      url='https://github.com/mesocentre-mcia/slurm-accounting',
      packages=find_packages('.', exclude=['*.tests']),
      entry_points = {
          "console_scripts": [
              'saccounting = slurm_accounting.saccounting:main',
              'sreporting = slurm_accounting.sreport:main',
              'periodic_reports = slurm_accounting.periodic_reports:main',
          ]
        },
      keywords=['slurm'],
      classifiers=[
        'License :: OSI Approved :: BSD License',
        'Development Status :: 3 - Alpha',
        'Programming Language :: Python :: 3.7',
        'Operating System :: POSIX :: Linux',
        'Topic :: Utilities',
      ],
      install_requires=[
#      'six>=1.10.0',
      ]
     )
