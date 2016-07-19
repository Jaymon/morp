#!/usr/bin/env python
# http://docs.python.org/distutils/setupscript.html
# http://docs.python.org/2/distutils/examples.html

import sys
from setuptools import setup
import ast
import os

name = 'morp'
def get_version(name):
    with open('{}{}__init__.py'.format(name, os.sep), 'rU') as f:
        for node in (n for n in ast.parse(f.read()).body if isinstance(n, ast.Assign)):
            node_name = node.targets[0]
            if isinstance(node_name, ast.Name) and node_name.id.startswith('__version__'):
                return node.value.s
    raise RuntimeError('Unable to find version number')
version = get_version(name)

setup(
    name=name,
    version=version,
    description='Send and receive messages without thinking about it',
    author='Jay Marcyes',
    author_email='jay@marcyes.com',
    url='http://github.com/firstopinion/{}'.format(name),
    packages=[name, '{}.interface'.format(name)],
    license="MIT",
    install_requires=['dsnparse', 'boto3', 'pycrypto'],
    classifiers=[ # https://pypi.python.org/pypi?:action=list_classifiers
        'Development Status :: 4 - Beta',
        'Environment :: Web Environment',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: MIT License',
        'Operating System :: OS Independent',
        'Topic :: Database',
        'Topic :: Software Development :: Libraries',
        'Topic :: Utilities',
        'Programming Language :: Python :: 2.7',
    ],
    #test_suite = "{}_test".format(name),
)
