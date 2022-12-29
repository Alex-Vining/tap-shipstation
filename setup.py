#!/usr/bin/env python
from setuptools import setup

setup(
    name="tap-shipstation",
    version="0.1.1",
    description="Singer.io tap for extracting data from the ShipStation API",
    author="Josh Temple",
    url="https://milkbarstore.com",
    classifiers=["Programming Language :: Python :: 3 :: Only"],
    py_modules=["tap_shipstation"],
    install_requires=["singer-python>=5.0.12", "requests", "pendulum", "jsonref"],
    entry_points="""
    [console_scripts]
    tap-shipstation=tap_shipstation:main
    """,
    packages=["tap_shipstation"],
    package_data={"schemas": ["tap_shipstation/schemas/*.json"]},
    include_package_data=True,
)
