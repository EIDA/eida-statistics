from setuptools import setup

requires = [
    'pyramid',
    'waitress',
]

setup(
    name='webservice',
    install_requires=requires,
    entry_points={
        'paste.app_factory': [
            'main = webservice:main'
        ],
    },
)
