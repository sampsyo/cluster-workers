from setuptools import setup
import os

def _read(fn):
    path = os.path.join(os.path.dirname(__file__), fn)
    data = open(path).read()
    return data

setup(name='cluster-workers',
      version='0.1.0',
      description='a client/master/worker system for distributing '
                  'jobs in a cluster',
      author='Adrian Sampson',
      author_email='adrian@radbox.org',
      url='https://github.com/sampsyo/cluster-workers',
      license='MIT',
      platforms='ALL',
      long_description=_read('README.rst'),

      packages=['cw'],
      install_requires=['bluelet'],

      classifiers=[
          'Topic :: System :: Networking',
          'Intended Audience :: Developers',
          'Programming Language :: Python :: 2',
          'Programming Language :: Python :: 3',
      ],
)
