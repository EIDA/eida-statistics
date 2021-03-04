# EIDA statistics webservice

## Run for developement

Database backend: 

    cd backend_database
    buildah build -t pg_hll .
    podman run -d -e POSTGRES_PASSWORD=password -p 5432:5432 postgres-hll -c fsync=no
    psql -h localhost -U postgres -d postgres < eidastats_schema.sql
    
Webservice frontend :
    
    FLASK_ENV=development FLASK_APP=app.py DBURI=postgresql://postgres:password@localhost:5432/postgres flask run
