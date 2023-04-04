#!/usr/bin/env python3
import os

def before_feature(context, feature):
    baseurl = os.getenv('BASEURL', 'http://ws.resif.fr/eidaws/statistics/1')
    context.baseurl = f"{baseurl}/{feature.name}"

def before_scenario(context, scenario):
    context.request_parameters = {}
    context.request_result = ""
