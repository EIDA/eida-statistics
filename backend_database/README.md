## Containerized postgresql

The file `Dockerfile.pg-hll` builds a postgresql 13 container with HyperLogLog extension.

    docker build -f Dockerfile.pg-hll -t pg-hll .
    
Then run the container like this :

    docker run -e POSTGRES_PASSWORD=password -p 5432:5432 pg-hll 

## Database schema 

### Migrations

Migrations are managed with `yoyo`

    pip install yoyo-migration psycopg2
    yoyo apply --database postgresql://user:password@host:port/database migrations

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

### table payloads

This table keeps track of all the payloads received. In order to prevent the server to ingest twice the same statistics
  
  - `node_id`: reference of the `node(id)` column
  - `hash`: mmh3 hash of the received payload
  - `version`: the version of the aggregation schema
  - `generated_at`: the date when the aggregation has been computed
  - `first_stat_at`: the first date for the statistic (not the month, but the real event)
  - `last_stat_at`: the last date for the statistic (not the month, but the real event)
  - `created_at`: timestamp when the payload has been received

Uniqueness is defined on `node_id` + `hash`

### table dataselect_stats

Columns definition :

  - `node_id`: reference of the `node(id)` column
  - `date`: the date of the statistics. It's the first day of a month, as the stats are aggregated monthly.
  - `network`: extended seismic network code (eg Z32015)
  - `station`, `location`, `channel`: usual seismological stream identification 
  - `country`: iso code for the country
  - `bytes`: quantity of data delivered
  - `nb_reqs`: number of total requests
  - `nb_successful_reqs`: number of successful requests (http rc 200)
  - `nb_failedrequests`: number of failed requests TODO: clarify this definition
  - `clients`: unique IPs as a HyperLogLog hash
  - `created_at`: timestamp when the statistic has been submitted
  - `updated_at`: timestamp when the statistic has been updated
  
Uniqueness is defined on `node_id` + `date` + `network` + `station` + `location` + `channel` + `country`

### view coverage

This view is used as a helper to consult statistics coverage for each node. 

``` sql
CREATE OR REPLACE VIEW coverage AS 
    SELECT a.name AS node, unnest(b.stats) AS stat_day FROM
        (SELECT nodes.id,nodes.name FROM nodes) a 
        LEFT JOIN 
        (SELECT node_id, array_agg(splitup) as stats FROM 
            (select node_id, unnest(coverage) splitup FROM payloads) splitup 
            group by 1) b 
        ON a.id=b.node_id;
```

For instance, in order to get the monthly coverage for a given node :

``` sql
    select node, a.month, round(count(*)/(SELECT extract(days FROM a.month + interval '1 month - 1 day'))*100) as percent from
    (select node, date_trunc('month',stat_day) as month, stat_day from coverage where node='RESIF' and
  date_part('year', stat_day)=2021) as a group by 1,2 order by 1;

```
