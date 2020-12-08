from setuptools import setup, find_packages
import metadata_generator

setup(
    name='metadata_generator',
    version='1.0.1',
    description="""
    Python library with helper functions intended for
    the metadata generation of the Spark datasets.
    """,
    packages=find_packages(),
    url='https://',
    author='OPI',
    install_requires=[
        'pandas==0.23.4',
    ],
    author_email='j.carvajal@opianalytics.com',
    license='MIT',
    zip_safe=False
)