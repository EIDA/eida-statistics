# EIDA statistics webservice

The webserice provides an endpoint to send and consult the statistics.

## Run for developement

Database backend: 

    cd ../backend_database
    docker build -f Dockerfile.pg-hll -t pg-hll .
    docker run -d -e POSTGRES_PASSWORD=password -p 5432:5432 pg-hll -c fsync=no
    cd ../webservice
    pip instann --user pipenv
    pipenv install
    pipenv install yoyo
    pipenv shell
    yoyo apply --database postgres://postgre:password@localhost/postgres migrations
    
Webservice frontend :
    
    FLASK_ENV=development FLASK_APP=app.py DBURI=postgresql://postgres:password@localhost:5432/postgres flask run
    
## API validation with behaviour tests

    pip install behave
    BASEURL=http://ws-staging.resif.fr/eidaws/statistics/1 behave behave_tests
    
The BASEURL environment can be set to the webservice instance to test.


## Database requirements

Here are the list of permissions required in order for the webservice to run with the role `wseidastats`

``` sql
create role wseidastats with password 'xxxxxxxx';
grant connect on database eidastats to wseidastats;
grant SELECT,UPDATE on SEQUENCE payloads_id_seq TO wseidastats ;
grant SELECT on nodes to wseidastats ;
grant SELECT,INSERT,UPDATE on dataselect_stats to wseidastats ;
grant SELECT,INSERT on payloads to wseidastats ;
grant SELECT on tokens to wseidastats ;
```

## Test with some data to ingest

You need a valid token to post some data.

    cat aggregated-data.json | curl  --header "Authentication: Bearer ${TOKEN}"  --header "Content-Type: application/json" -d "@-" https://ws.resif.fr/eidaws/statistics/1/dataselect

## Getting statistics

The interface is fully documented in [openapi](openapi.yaml).
