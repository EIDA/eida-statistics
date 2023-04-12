Feature: dataselect/public

  Scenario: Run the example request from openAPI spec
    Given the request parameter start set to 2022-01
    Given the request parameter end set to 2022-01
    Given the request parameter node set to RESIF,NOA
    Given the request parameter country set to FR,GR
    Given the request parameter level set to node
    Given the request parameter format set to json
    When doing a GET request to featured endpoint
    Then request result is 200
    And response is a valid JSON
    And there is one result per node

  Scenario: Try to get statistic for a specific network
    Given the request parameter start set to 2022-01
    Given the request parameter end set to 2022-01
    Given the request parameter network set to Z32015
    Given the request parameter level set to network
    Given the request parameter format set to json
    When doing a GET request to featured endpoint
    Then request result is 200
    And response is a valid JSON

  Scenario: Getting statistic for multiple network is wrong
    Given the request parameter start set to 2022-01
    Given the request parameter end set to 2022-01
    Given the request parameter network set to Z32015,FR
    Given the request parameter level set to network
    Given the request parameter format set to json
    When doing a GET request to featured endpoint
    Then request result is 400

  Scenario: Getting statistic for one restricted network is forbidden
    Given the request parameter start set to 2022-01
    Given the request parameter end set to 2022-01
    Given the request parameter network set to 2D2021
    Given the request parameter level set to network
    Given the request parameter format set to json
    When doing a GET request to featured endpoint
    Then request result is 401

  Scenario: Making a request with wrong parameters response in consistent format (JSON)
    Given the request parameter start set to 2022-01
    Given the request parameter end set to 2022-01
    Given the request parameter network set to Z32015,FR
    Given the request parameter wrong set to nothing
    Given the request parameter format set to json
    When doing a GET request to featured endpoint
    Then request result is 400
    And response is a valid JSON

  Scenario: Making a request with wrong parameters response in consistent format (CSV)
    Given the request parameter start set to 2022-01
    Given the request parameter end set to 2022-01
    Given the request parameter network set to Z32015,FR
    Given the request parameter wrong set to nothing
    Given the request parameter format set to csv
    When doing a GET request to featured endpoint
    Then request result is 400
    Then request result is in csv format
