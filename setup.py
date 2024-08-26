from setuptools import setup, find_packages

setup(
    name='TAconnect',
    version='0.1.0',
    description='A Python library for Redshift and Snowflake connections',
    author='Sai Aditya',
    author_email='sai.aditya@tigeranalytics.com',
    url='https://github.com/saiTA21/TAconnect',
    packages=find_packages(),
    install_requires=[
        'psycopg2',
        'snowflake-connector-python',
    ],
)
