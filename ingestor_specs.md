# Eida statistics webservice specifications

## API specification

The API has been described in [openapi3 specification format](./ingestor_openapi3.yaml).


### PUSH statistics
Pushing a new statistic or updating it is done with a bearer token wich is used to map the statistic to the correct EIDA node.

The body of the request is a list of statistics in JSON format (should we compress it ?).

#### Authentication considerations

Each node manager should push statistics for their own node. Statistics push should be as straightforward as possible.

The webservice should accept the data with a valid token. The management system should be able to generate a token for a node. Seen section management for the specification.

#### Technical constraints

The statistics system uses HyperLogLog algorithm which is implemented in postgreSQL database and for few languages (as far as I know) which are [https://github.com/svpcom/hyperloglog](python) and [https://github.com/hll/java-hll](java) and maybe [https://github.com/DylanMeeus/MediumCode/blob/master/hyperloglog/main.go](go).

Python implementation has been successfully tested for the aggregator.

### Get

TODO : describe an API to respond to all the EMB questions

## Management

There should be some management tasks available for the service operator in order to manage the registered nodes and authorized persons.

Each node can be registered in the databse with those attibutes :

  - name : the node name
  - contact : an email address

The system should be able to generate tokens with date validity for each node. The tokens are stored in the database :

  - token : the token as a string
  - node_id : reference to the node
  - generated_at : generation date of the token
  - expires_at : expiration date
  
A management tool should be provided as a command line interface to manage the tokens and the nodes.

    eida_statsman <subcommand> <action> <options>

Subcommands are :

  - node
  - token
  
Actions are :

  - list
  - add
  - del
  - update
  
The management tool will edit the postgreSQL statistics database in order to manage nodes and tokens. The database URI `postgresql://dbuser:dbpass@dbhost:dbport/dbname` should be provided as an environment variable (`DBURI` for instance)
The operator is responsible for generating the tokens and transmit them to the contact persons.

The contact persons have to notify the operator if the token has to be changed.


  
### eida_stasman node

   - list: list registered nodes
   - add: add a new node. Need mandatory options :
     - --name RESIF
     - --contact resif-dc@univ-grenoble-alpes.fr
     - there can't be 2 nodes with the same name
   - del: delete a node. Needs manatory options :
     - --name RESIF
     - will ask for confirmation by re-typing the node name
   - update: updates information about a node
     - --name RESIF --contact newcontact@resif.fr
     - will ask for confirmation by re-typing the node name
     
### eida_statsman token

  - list: lists all registerd active tokens. Optional arguments allow selection of nodes :
    - --expired : also lists expired tokens
    - --node RESIF : list tokens for the specified node
  - add: generates a new token for a node.
    - --node RESIF (mandatory)
    - --expires_at : specify a date with format YYYY-MM-DD. If not specified, never expires.
  - del: delete a token
    - --token-id specify the token ID to delete.
     - will ask for confirmation by re-typing the node name
  - update: do not implement 


