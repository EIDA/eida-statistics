Feature: dataselect/restricted

Scenario: Get restricted data with a valid token
    Given the request parameter start set to 2022-01
    Given the request parameter end set to 2022-01
    Given the request parameter network set to 9H2016
    Given the request parameter level set to station
    Given the request parameter format set to json
    Given eidatoken.pgp as a valid token
    When doing a POST request to featured endpoint
    Then request result is 200
    And response is a valid JSON
