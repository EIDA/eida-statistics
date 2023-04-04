Feature: dataselect/public

  Scenario: Run the example request from openAPI spec
    Given the request parameter start set to 2022-01
    Given the request parameter end set to 2022-01
    Given the request parameter node set to RESIF,NOA
    Given the request parameter country set to FR,GR
    Given the request parameter level set to node
    Given the request parameter format set to json
    When doing a GET request to endpoint public
    Then request result is 200
    And response is a valid JSON
    And there is one result per node
