# -*- coding: UTF-8 -*-
# (c)2013 Mik Kocikowski, MIT License (http://opensource.org/licenses/MIT)
# https://github.com/mkocikowski/esbench


from setuptools import setup

ld = """Elasticsearch data handling tools.
"""

setup(
    name = 'estools',
    version = '0.0.6',
    author = 'Mik Kocikowski',
    author_email = 'mkocikowski@gmail.com',
    url = 'https://github.com/mkocikowski/estools',
    description = 'Elasticsearch data tools',
    long_description = ld,
    install_requires = [
        'requests >= 2.1.0',
#         'pyrax >= 1.6.2',
    ],
    packages = [
        'estools',
        'estools.test',
        'estools.load',
        'estools.dump',
        'estools.rax',
        'estools.common',
    ],
    package_data = {
        '': ['README.md'],
    },
    entry_points = {
        'console_scripts': [
            'esload = estools.load.client:main',
            'esdump = estools.dump.client:main',
        ]
    },
    classifiers = [
        "Development Status :: 3 - Alpha",
        "Environment :: Console",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: MIT License",
        "Natural Language :: English",
        "Operating System :: POSIX",
        "Topic :: Internet :: WWW/HTTP :: Indexing/Search",
        "Topic :: Utilities",
    ],
    license = 'MIT',
    test_suite = "estools.test.units.suite",
)
