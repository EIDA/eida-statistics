
# My Project

[![Coverage Status](coverage.svg)](./coverage.svg)

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


# Deployment

## Database

A working postgresql instance is needed, with the hll extension installed. This extension is usually available in the PGDG repository (see for [yum](https://yum.postgresql.org/) or [APT](https://wiki.postgresql.org/wiki/Apt)).
Otherwise, look at the installation instructions of the [postgres-hll project](https://github.com/citusdata/postgresql-hll)

Create a new database and enable the HLL extension :

``` sql
CREATE ROLE fedstats WITH LOGIN PASSWOD 'xxxxxxxxx';
CREATE DATABASE fedstats OWNER fedstats;
\c fedstats
CREATE EXTENTION hll;
```

Now you need to deploy the database schema. For this, read the [instructions](backend_database/README.md).

## Database management tool

`eida_statsman` from this project will help you manage the nodes and the tokens for statistics ingestion.

To install it, look at it's [documentation](eida_statsman/README.md)

## Webservice deployment

The recommended way of deploying the webservice is to use the docker image distributed in this project `ghcr.io/eida/eida-statistics:main`.

To configure and run an instance of the webservice, see [it's documentation](webservice/README.md).

## Aggregate and submit statistics

Now, use the aggregator program to build statistics and send them to the webservice instance. Look at the [documentation](aggregator/README.md)

