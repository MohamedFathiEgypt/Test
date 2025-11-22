from setuptools import setup, find_packages

setup(
    name="pandas-ta",
    version="0.4.0",
    packages=find_packages(),
    install_requires=[
        "pandas>=1.0.0",
        "numpy>=1.18.0"
    ]
)
