import os
from setuptools import setup, find_packages

# The directory containing this file
HERE = os.path.dirname(os.path.abspath(__file__))

# The text of the README file
with open(os.path.join(HERE, "README.md"), "r") as f:
    README = f.read()

# This call to setup() does all the work
setup(
    name="spark-etl",
    version="0.0.81",
    description="Generic ETL Pipeline Framework for Apache Spark",
    long_description=README,
    long_description_content_type="text/markdown",
    url="https://github.com/stonezhong/spark_etl",
    author="Stone Zhong",
    author_email="stonezhong@hotmail.com",
    license="MIT",
    classifiers=[
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python :: 3",
    ],
    package_dir = {'': 'src'},
    packages=find_packages(where='src'),
    install_requires=["requests", "Jinja2", "termcolor"],
)