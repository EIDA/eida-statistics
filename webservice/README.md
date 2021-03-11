# EIDA statistics webservice

## Run for developement

Database backend: 

    cd backend_database
    buildah build -t pg_hll .
    podman run -d -e POSTGRES_PASSWORD=password -p 5432:5432 postgres-hll -c fsync=no
    yoyo apply --database postgres://postgre:password@localhost/postgres migrations
    
Webservice frontend :
    
    FLASK_ENV=development FLASK_APP=app.py DBURI=postgresql://postgres:password@localhost:5432/postgres flask run

## Database requirements

Here are the list of permissions required in order for the webservice to run with the role `wseidastats`

``` sql
grant SELECT,UPDATE on SEQUENCE payloads_id_seq TO wseidastats ;
grant SELECT on nodes to wseidastats ;
grant SELECT,INSERT,UPDATE on dataselect_stats to wseidastats ;
grant SELECT,INSERT on payloads to wseidastats ;
grant SELECT on tokens to wseidastats ;
```
