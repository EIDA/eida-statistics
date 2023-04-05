#!/usr/bin/env python3
import os

def before_feature(context, feature):
    context.baseurl = os.getenv('BASEURL', 'http://ws.resif.fr/eidaws/statistics/1')+"/"+feature.name

def before_scenario(context, scenario):
    context.request_parameters = {}
    context.request_result = ""
