# setup.py
from setuptools import setup, find_packages

setup(
    name="KinderUDP",
    version="0.1.0",
    packages=find_packages(),  # Automatically finds all sub-packages
    install_requires=[
        "sqlalchemy",
        "pandas",
        "tqdm",
        "pyodbc"
    ],
)
