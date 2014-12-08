#!/usr/bin/env python
# -*- coding: utf-8 -*-


try:
    from setuptools import setup
except ImportError:
    from distutils.core import setup


readme = open('README.rst').read()
history = open('HISTORY.rst').read().replace('.. :changelog:', '')

requirements = [
    # TODO: put package requirements here
]

test_requirements = [
    # TODO: put package test requirements here
]

setup(
    name='tmsoup',
    version='0.1.0',
    description='Extensions and enhancements to TMSU.',
    long_description=readme + '\n\n' + history,
    author='David Gowers',
    author_email='finticemo@gmail.com',
    url='https://github.com/0ion9/tmsoup',
    packages=[
        'tmsoup',
    ],
    package_dir={'tmsoup':
                 'tmsoup'},
    include_package_data=True,
    install_requires=requirements,
    license="LGPLv3",
    zip_safe=False,
    keywords='tmsoup',
    classifiers=[
        'Development Status :: 2 - Pre-Alpha',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: BSD License',
        'Natural Language :: English',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.3',
        'Programming Language :: Python :: 3.4',
    ],
    test_suite='tests',
    tests_require=test_requirements
)
