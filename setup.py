#!/usr/bin/env python
import os
import sys
from setuptools import setup, find_packages
from version import version
rootdir = os.path.abspath(os.path.dirname(__file__))
name = "solrpage"
long_description = open(os.path.join(rootdir, 'README.md')).read().strip()
VERSION = open(os.path.join(rootdir, 'VERSION')).read().strip()
setup(name='python-' + name,
      version=VERSION,
      description='solrpage',
      long_description=long_description,
      url='',
      author='gardsted',
      author_email='gardsted@gmail.com',
      license='MPL',
      classifiers=[
          'Development Status :: 1 - Planning',
          'Environment :: Console',
          'License :: OSI Approved :: Mozilla Public License',
          'Natural Language :: English',
          'Operating System :: OS Independent',
          'Programming Language :: Python :: 3',
          'Programming Language :: Python :: 3.6',
      ],
      keywords='mail',
      packages=["solrpage"]
)
