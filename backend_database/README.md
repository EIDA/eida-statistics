## Containerized postgresql

The file `Dockerfile.pg-hll` builds a postgresql 13 container woth HyperLogLog extension.

    docker build -f Dockerfile.pg-hll -t pg-hll .
    
Then run the container like this :

    docker run -e POSTGRES_PASSWORD=password -p 5432:5432 pg-hll 

## Database schema

The file `eidastats_schema.sql` contains the database schema.

### table nodes

Columns definition :

  - `id`: uniquely identify an entry in this table
  - `name`: the usual acronym of the node
  - `contact`: an email contact
  
### table tokens

Describe all the tokens :

  - `id`: uniquely identify a token
  - `node_id`: refernce of the `node(id)` column
  - `value`: the value of the token
  - `valid_from` : timestamp when the token starts to be valid
  - `valid_until`: when the token expires
  - `created_at`: timestamp when the token has been created

### table dataselect_stats

Columns definition :

  - `node_id`: reference of the `node(id)` column
  - `date`: the date of the statistics. It's the first day of a month, as the stats are aggregated monthly.
  - `network`: extended seismic network code (eg Z32015)
  - `station`, `location`, `channel`: usual seismological stream identification 
  - `bytes`: quantity of data delivered
  - `nb_reqs`: number of total requests
  - `nb_successful_reqs`: number of successful requests (http rc 200)
  - `nb_failedrequests`: number of failed requests TODO: clarify this definition
  - `clients`: unique IPs as a HyperLogLog hash
  - `created_at`: timestamp when the statistic has been submitted
  - `updated_at`: timestamp when the statistic has been updated
  
