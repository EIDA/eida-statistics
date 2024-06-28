# EIDA statistics webservice

The webserice provides an endpoint to send and consult the statistics.

## Install the webservice

For the database backend look at [database README](../backend_database/README.md).

### Webservice frontend:

 - For development purposes:
   ```
   pipenv install requirements.txt
   DBURI=postgresql://postgres:password@localhost:5432/postgres pserve development.ini
   ```

 - Using Docker:
   ```
   docker build -f Dockerfile -t eidastats .
   DBURI=postgresql://postgres:password@localhost:5432/postgres docker run -p 6543:6543 eidastats
   ```
   **Note:** Make sure to use the correct environment variables for the database in your system.

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

The interface is fully documented in [openapi](ws_eidastats/openapi.yaml).
