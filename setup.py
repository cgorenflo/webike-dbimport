from setuptools import setup

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
    packages=['iss4e.webike.db'],
    scripts=['scripts/import_data'],
    install_requires=[
        'iss4e_toolchain>=0.1.0'
    ]
)
