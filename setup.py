#! /usr/bin/env python

import os
import sys

from setuptools import setup

PACKAGE = "bbconf"

# Additional keyword arguments for setup().
extra = {}

# Ordinary dependencies
DEPENDENCIES = []
with open("requirements/requirements-all.txt", "r") as reqs_file:
    for line in reqs_file:
        if not line.strip():
            continue
        DEPENDENCIES.append(line)

extra["install_requires"] = DEPENDENCIES

with open("{}/_version.py".format(PACKAGE), "r") as versionfile:
    version = versionfile.readline().split()[-1].strip("\"'\n")

# Handle the pypi README formatting.
try:
    import pypandoc

    long_description = pypandoc.convert_file("README.md", "rst")
except (IOError, ImportError, OSError):
    long_description = open("README.md").read()

setup(
    name=PACKAGE,
    packages=[PACKAGE],
    version=version,
    description="Configuration package for bedbase project",
    long_description=long_description,
    long_description_content_type="text/markdown",
    classifiers=[
        "Development Status :: 4 - Beta",
        "License :: OSI Approved :: BSD License",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Programming Language :: Python :: 3.12",
        "Programming Language :: Python :: 3.13",
        "Topic :: Scientific/Engineering :: Bio-Informatics",
    ],
    keywords="",
    url="https://databio.org",
    author="Michal Stolarczyk, Oleksandr Khoroshevskyi",
    author_email="khorosh@virginia.edu",
    license="BSD2",
    package_data={PACKAGE: [os.path.join(PACKAGE, "*")]},
    include_package_data=True,
    test_suite="tests",
    tests_require=(["pytest"]),
    setup_requires=(
        ["pytest-runner"] if {"test", "pytest", "ptr"} & set(sys.argv) else []
    ),
    **extra,
)
