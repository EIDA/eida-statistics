from io import open

from setuptools import find_packages, setup

with open('eida_statsman/__init__.py', 'r') as f:
    for line in f:
        if line.startswith('__version__'):
            version = line.strip().split('=')[1].strip(' \'"')
            break
    else:
        version = '0.0.1'

with open('README.md', 'r', encoding='utf-8') as f:
    readme = f.read()

REQUIRES = [
    'click>=7.0.0',
    'psycopg2>=2.8.0',
    'sqlalchemy==1.3'
]

description = """
EIDA statistics manager builds monthly statistics out of seiscomp's logfile. It is also able to post the result to the central statistics system.
"""

kwargs = {
    'name': 'eida-statistics-manager',
    'version': version,
    'description': 'EIDA nodes statistics manager',
    'python_requires': '>=3.6',
    'long_description': readme,
    'author': 'Jonathan Schaeffer',
    'author_email': 'jonathan.schaeffer@univ-grenoble-alpes.fr',
    'maintainer': 'Jonathan Schaeffer',
    'maintainer_email': 'jonathan.schaeffer@univ-grenoble-alpes.fr',
    'url': 'https://github.com/eida/eida-statistics/manager',
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
    'packages': find_packages(exclude=('tests', 'tests.*')),
    'include_package_data': True,
    'entry_points': '''
        [console_scripts]
        eida_statsman=eida_statsman.eida_statsman:cli
    ''',
}

setup(**kwargs)
