from distutils.core import setup

from setuptools import find_packages

setup(
    name='webike_dbimport',
    version='0.5.0',
    url='https://github.com/iss4e/webike-dbimport.git',
    license='MIT',
    classifiers=[
        'Development Status :: 3 - Alpha',
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python :: 3.5',
    ],
    author='Information Systems and Science for Energy',
    author_email='webike-dev@lists.uwaterloo.ca',
    description='WeBike data import into database',
    packages=find_packages(),
    install_requires=[
        'iss4e_toolchain>=0.1.0'
    ]
)
