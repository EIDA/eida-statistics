from io import open

from setuptools import find_packages, setup

with open('aggregator/__init__.py', 'r') as f:
    for line in f:
        if line.startswith('__version__'):
            version = line.strip().split('=')[1].strip(' \'"')
            break
    else:
        version = '0.0.1'

with open('README.md', 'r', encoding='utf-8') as f:
    readme = f.read()

REQUIRES = [
    'certifi',
    'click>=7.0.0',
    'mmh3>=2.5.0',
    'numpy>=1.18.0',
    'python-hll>=0.1.3',
    'requests>=2.0.0',
    'urllib3>=1.20.0',
    'python-magic'
]

description = """
EIDA statistics aggregator builds monthly statistics out of seiscomp's logfile. It is also able to post the result to the central statistics system.
"""

kwargs = {
    'name': 'eida-statistics-aggregator',
    'version': version,
    'description': 'EIDA nodes statistics aggregator',
    'python_requires': '>=3.6',
    'long_description': readme,
    'author': 'Jonathan Schaeffer',
    'author_email': 'jonathan.schaeffer@univ-grenoble-alpes.fr',
    'maintainer': 'Jonathan Schaeffer',
    'maintainer_email': 'jonathan.schaeffer@univ-grenoble-alpes.fr',
    'url': 'https://github.com/eida/eida-statistics/aggregator',
    'license': 'GPLv3',
    'classifiers': [
        'Development Status :: 4 - Beta',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: GNU General Public License v3 (GPLv3)',
        'Natural Language :: English',
        'Operating System :: OS Independent',
        'Programming Language :: Python :: 3',
    ],
    'install_requires': REQUIRES,
    'tests_require': ['coverage', 'pytest'],
    'packages': find_packages(exclude=('tests', 'tests.*')),
    'include_package_data': True,
    'entry_points': '''
        [console_scripts]
        eida_stats_aggregator=aggregator.aggregator:cli
    ''',

}

setup(**kwargs)
