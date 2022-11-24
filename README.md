# EIDA central statistics repository

There are 3 parts of this project :

## Aggregator

The aggregator is meant to be deployed on each EIDA node. It is responsible for computing logging statistics with the dataselect usage events as provided by seiscomp.

## Centralized statistics web service

This web service is able to receive logging statistics as provided by the aggregator. It stores the logging statistics in a database.

The web service also provide an interface to consult the statistics.

## Centralized service management tool

A command line interface to manage the central database system : listing nodes, adding nodes, updating a token.

## Test environment

Starting the webserver and the database:

   docker-compose up
   tests/load_database.sh



