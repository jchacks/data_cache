from os import path

from setuptools import setup

this_directory = path.abspath(path.dirname(__file__))
with open(path.join(this_directory, 'README.md'), encoding='utf-8') as f:
    long_description = f.read()

setup(
    name='data_cache',
    version='0.1',
    description='Data caching server and client',
    long_description=long_description,
    long_description_content_type='text/markdown',
    url='https://github.com/jchacks/data_cache',
    author='jchacks',
    packages=['data_cache'],
    install_requires=[
        'redis',
        'pyarrow'
    ],
    include_package_data=True,
    zip_safe=False
)
