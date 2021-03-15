# EIDA Stats Manager

Command line tool to manage the central database

## Installation

### With Pypi

    pip install eida_statsman
    
### Alternatively, from source with pipenv

    pipenv shell
    pip install .
    

## Usage

Provided that you have access to the eidastats database :

Exemple :

    DBURI=postgres://eidastats@pghost/eidastats
    eida_statsman tokens list

