# EIDA Statistics aggregation

This project provides unified statistics about EIDA nodes usage.

This program is part of the EIDA logging system. The aggregator groups a set of dataselect usage information in order to build a logging statistic ready to be shipped to the central system.
    
## Aggregating data

Each EIDA node prepares an aggregation of their logging file using the same script.

This aggregation result is sent to a central database through a webservice provided by a central node

### Install and execute

This program is intended for python3.6 and more.

From [Pypi](https://pypi.org/project/eida-statistics-aggregator/)

    pip install eida-statistics-aggregator
    eida_stats_aggregator --help
    

Alternatively, if you want to install with `pipenv` , run 

    pipenv install
    pipenv shell
    pip install -e .
    eida_stats_aggregator --help
    
For now, the log file from seiscomp is expected to be a list of JSON entries compressed with BZIP2.

### Exemples

Aggregate one bz2 seiscomp logfile:

    eida_stats_aggregator --output-directory aggregates fdsnws-requests.log.2020-11-02.bz2
    
Also available with stdin:

    cat fdsnws-requests.log.2020-11-02.bz2 |  eida_stats_aggregator --output-directory
    
You can also agregate several logfiles:

     eida_stats_aggregator --output-directory aggregates fdsnws-requests.log.2020-11-02.bz2  fdsnws-requests.log.2020-11-03.bz2
     
### Registering the aggregation to the central statistics service

In order to register, you first need a token. Please ask for one by submitting an issue in https://github.com/eida/etc/issues/

When you have a valid token, you can send all your aggregation files with curl :

    gunzip -c aggregationfile.json.gz | curl  --header "Authentication: Bearer MYSECRETTOKEN"  --header "Content-Type: application/json" -d "@-"  https://ws.resif.fr/eidaws/statistics/1/dataselectstats
    
The aggregation script can do this for you on the fly :

    eida_stats_aggregator -o aggregates fdsn-requests.log.2020-11-02.bz2 --token MYSECRETTOKEN --send-to https://ws.resif.fr/eidaws/statistics/1/dataselectstats
    

### Test

From the projet's root directory run 

    pipenv install
    pipenv shell
    python -m pytest tests/test_aggregator.py -s 
    
### Aggregation problems

#### The Count distinct problem

Some information requested by EIDA need to count distint occurences of information (an IP, a country). A naive approach counting distinct occurences on each day and each node can't be used to count the distinct occurences at a global scale nor for another timewindow.

Enters HyperLogLog, an algorithm allowing to estimate occurences for different timeframe. hll is implemented in Python and PostgreSQL this is why this project uses both technologies.

#### Anonimization

We want to anonimize every data that can link to a person. This is why IP adresses are hashed using the same algorithm on each datacenter, in order to have consistant statistics.

## Ingesting data

A webservice receiving POST request and ingesting the result in a database

## Creating reports

This code create automatic reports from the database


