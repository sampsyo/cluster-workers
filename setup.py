from setuptools import setup

setup(name='cluster-workers',
      version='0.1.0',
      description='a client/master/worker system for distributing '
                  'jobs in a cluster',
      author='Adrian Sampson',
      author_email='adrian@radbox.org',
      url='https://github.com/sampsyo/cluster-workers',
      license='MIT',
      platforms='ALL',
      long_description='',

      packages=['cw'],
      install_requires=['bluelet'],

      classifiers=[
          'Topic :: System :: Networking',
          'Intended Audience :: Developers',
          'Programming Language :: Python :: 2',
          'Programming Language :: Python :: 3',
      ],
)
