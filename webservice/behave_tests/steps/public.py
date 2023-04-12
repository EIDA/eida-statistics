#!/usr/bin/env python3
from behave import when, then, given
import requests

@given(u'the request parameter {param} set to {value}')
def set_parameters(context, param, value):
    context.request_parameters[param] = value

@given(u'{token} as a valid token')
def set_token(context, token):
    context.token = token

@when(u'doing a {method} request to featured endpoint')
def do_request(context, method):
    url_params = ""
    for k,v in context.request_parameters.items():
        url_params += f"{k}={v}&"
    url_params = url_params[:-1]
    if method.upper() == 'GET':
        context.request_result = requests.get(f"{context.baseurl}?{url_params}")
    if method.upper() == 'POST':
        print("POST request")
        print(context.token)
        context.request_result = requests.post(f"{context.baseurl}?{url_params}", data=open(context.token, 'rb'))


@then(u'request result is {rc}')
def request_result(context, rc):
    print(context.baseurl)
    print(context.request_result.status_code)
    assert context.request_result.status_code == int(rc)


@then(u'response is a valid JSON')
def request_result_is_json(context):
    print(context.request_result.text )
    try:
        context.request_result.json()
    except:
        raise ValueError(f"{context.request_result.text} is not a json format")


@then(u'there is one result per node')
def step_impl(context):
    response = context.request_result.json()
    assert len(response['results']) == len(context.request_parameters['node'].split(','))
