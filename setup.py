#!/usr/bin/env python
"""
Postfix stats collector (StatsD integration)
"""
from setuptools import setup, find_packages

setup(
    name='postfix-stats-collector',
    version='0.0.0',
    url='https://github.com/ministryofjustice/postfix-stats-collector',
    license='MIT',
    author='',
    author_email='',
    description='',
    long_description=__doc__,
    platforms='any',
    packages=find_packages(),
    install_requires=[
        'schedule',
        'statsd'
    ],
    classifiers=[
    ],
    entry_points={
        'console_scripts': [
            'postfix-stats-qshape=postfix_stats_collector.qshape:main',
            'postfix-stats-logparser=postfix_stats_collector.logparser:main',
        ],
    }
)
