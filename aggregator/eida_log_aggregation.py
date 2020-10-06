#!/usr/bin/env python3

"""
Aggregate and submit metadata
"""

from datetime import datetime, timedelta
import logging
import json
import bz2
import mmh3
import click
from python_hll.hll import HLL


class EidaStatistic:
    """
    One statistic object.
    """
    date = datetime.now()
    network = ""
    station = ""
    location = ""
    channel = ""
    country = ""
    size = 0
    nb_requests = 0
    nb_successful_requests = 0
    nb_unsuccessful_requests = 0
    unique_clients = HLL(13, 5)

    def __init__(self, date=datetime.now(), network="", station="", location="--", channel="", country=""):
        """
        Class initialisation
        """
        self.date = date
        self.network = network
        self.station = station
        self.location = location
        self.channel = channel
        self.country = country
        self.size = 0
        self.nb_requests = 1
        self.unique_clients = HLL(13, 5)

    def key(self):
        """
        Generate a unique key for this object in order to ease
        """
        return f"{self.date}_{self.network}_{self.station}_{self.location}_{self.channel}_{self.country}"

    def aggregate(self, eidastat):
        """
        Aggregate a statistic to this object.
        This function alters the called object by aggregating another statistic object into it:
        - incrementing counts,
        - summing sizes
        - aggregating HLL objects
        """
        # Check if the two keys are the same
        if self.key() == eidastat.key():
            self.size += eidastat.size
            self.nb_requests += eidastat.nb_requests
            self.nb_successful_requests += eidastat.nb_successful_requests
            self.nb_unsuccessful_requests += eidastat.nb_unsuccessful_requests
            self.unique_clients.union(eidastat.unique_clients)
        else:
            logging.warning("Key %s to aggregate differs from called object's key %s", eidastat.key(), self.key())

    def info(self):
        string = f"{self.date} {self.network} {self.station} {self.location} {self.channel} from {self.country} {self.size}b {self.nb_successful_requests} successful requests from {self.unique_clients.cardinality()} unique clients"
        logging.debug(string)
        return string

def parse_file(filename):
    """
    Parse the file provided in order to aggregate the data.
    Returns a list of EidaStatistic
    Exemple of a line:
    {"clientID": "IRISDMC DataCenterMeasure/2019.136 Perl/5.018004 libwww-perl/6.13", "finished": "2020-09-18T00:00:00.758768Z", "userLocation": {"country": "US"}, "created": "2020-09-18T00:00:00.612126Z", "bytes":
98304, "service": "fdsnws-dataselect", "userEmail": null, "trace": [{"cha": "BHZ", "sta": "EIL", "start": "1997-08-09T00:00:00.0000Z", "net": "GE", "restricted": false, "loc": "", "bytes": 98304, "status": "OK", "end": "1997-08-09T01:00:00.0000Z"}], "status": "OK", "userID": 1497164453}
{"clientID": "ObsPy/1.2.2 (Windows-10-10.0.18362-SP0, Python 3.7.8)", "finished": "2020-09-18T00:00:01.142527Z", "userLocation": {"country": "ID"}, "created": "2020-09-18T00:00:00.606932Z", "bytes": 19968, "service": "fdsnws-dataselect", "userEmail": null, "trace": [{"cha": "BHN", "sta": "PB11", "start": "2010-09-04T11:59:52.076986Z", "net": "CX", "restricted": false, "loc": "", "bytes": 6656, "status": "OK", "end": "2010-09-04T12:03:32.076986Z"}, {"cha": "BHE", "sta": "PB11", "start": "2010-09-04T11:59:52.076986Z", "net": "CX", "restricted": false, "loc": "", "bytes": 6656, "status": "OK", "end": "2010-09-04T12:03:32.076986Z"}, {"cha": "BHZ", "sta": "PB11", "start": "2010-09-04T11:59:52.076986Z", "net": "CX", "restricted": false, "loc": "", "bytes": 6656, "status": "OK", "end": "2010-09-04T12:03:32.076986Z"}], "status": "OK", "userID": 589198147}
    """
    logfile = bz2.BZ2File(filename)
    statistics = {}
    line_number = 0
    # Initializing the counters

    with click.progressbar(logfile.readlines()) as bar:
        for jsondata in bar:
            line_number += 1
            try:
                data = json.loads(jsondata)
            except json.JSONDecodeError:
                logging.warning("Line %d could not be pardes as JSON. Ignoring", line_number)
            logging.debug(data)
            # Get the event timestamp as object
            event_datetime = datetime.fromisoformat(data['finished'].strip('Z')).date()
            # The date of the statistic is th end of the week's day
            event_weekday = event_datetime + timedelta(days=(6-event_datetime.weekday()))
            if data['status'] == "OK":
                for trace in data['trace']:
                    try:
                        new_stat = EidaStatistic(date=event_weekday, network=trace['net'], station=trace['sta'], location=trace['loc'], channel=trace['cha'], country=data['userLocation']['country'])
                    except KeyError as err:
                        logging.warning("Key error for data %s", trace)
                        continue
                    new_stat.nb_successful_requests = 1
                    new_stat.size = trace['bytes']
                    new_stat.unique_clients.add_raw(mmh3.hash(str(data['userID'])))# TODO avoir son IP
                    if new_stat.key() in statistics.keys():
                        statistics[new_stat.key()].aggregate(new_stat)
                    else:
                        statistics[new_stat.key()] = new_stat
            else:
                # TODO This is not very DRY but I did'nt figure a better way to do it for now
                try:
                    new_stat = EidaStatistic(date=event_weekday, country=data['userlocation']['country'])
                except KeyError as err:
                    logging.warning(data)
                    continue
                new_stat.nb_unsuccessful_requests = 1
                new_stat.unique_clients.add_raw(mmh3.hash(str(data['userID'])))# TODO avoir son IP
                if new_stat.key() in statistics.keys():
                    statistics[new_stat.key()].aggregate(new_stat)
                else:
                    statistics[new_stat.key()] = new_stat
    return statistics

def cli():
    """
    Command line interface
    """
