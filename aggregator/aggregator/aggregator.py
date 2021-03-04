#!/usr/bin/env python3

"""
Aggregate and submit metadata
"""

from datetime import datetime, timedelta, date
import logging
import sys
import json
import bz2
import mmh3
import click
import gzip
import requests
import magic
from . import __version__
from python_hll.hll import HLL
from python_hll.util import NumberUtil


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

    def to_dict(self):
        """
        Dump the statistic as a dictionary
        """
        unique_clients_bytes = self.unique_clients.to_bytes()
        json_dict = {'date': str(self.date),
                     'network': self.network,
                     'station': self.station,
                     'location': self.location,
                     'channel': self.channel,
                     'country': self.country,
                     'bytes': self.size,
                     'nb_requests': self.nb_requests,
                     'nb_successful_requests': self.nb_successful_requests,
                     'nb_unsuccessful_requests': self.nb_unsuccessful_requests,
                     'clients': "\\x" + NumberUtil.to_hex(unique_clients_bytes, 0, len(unique_clients_bytes))}
        return json_dict



def merge_statistics(stat1, stat2):
    """
    Merge stat1 in stat2
    Returns a new merged dictionary of aggregation
    """
    for key,stat in stat1.items():
        if key in stat2.keys():
            stat2[key].aggregate(stat)
        else:
            stat2[key] = stat
    return stat2

def shift_to_begin_of_month(event_date):
    """
    Returns the first day of week
    :param event_datetime is a DateTime or Date object. Must have a weekday() method.
    """
    if not isinstance(event_date, date):
        raise TypeError("datetime.date expected")
    return event_date - timedelta(days=(event_date.day-1))

def parse_file(filename):
    """
    Parse the file provided in order to aggregate the data.
    Returns a list of EidaStatistic
    Exemple of a line:
    {"clientID": "IRISDMC DataCenterMeasure/2019.136 Perl/5.018004 libwww-perl/6.13", "finished": "2020-09-18T00:00:00.758768Z", "userLocation": {"country": "US"}, "created": "2020-09-18T00:00:00.612126Z", "bytes":
98304, "service": "fdsnws-dataselect", "userEmail": null, "trace": [{"cha": "BHZ", "sta": "EIL", "start": "1997-08-09T00:00:00.0000Z", "net": "GE", "restricted": false, "loc": "", "bytes": 98304, "status": "OK", "end": "1997-08-09T01:00:00.0000Z"}], "status": "OK", "userID": 1497164453}
{"clientID": "ObsPy/1.2.2 (Windows-10-10.0.18362-SP0, Python 3.7.8)", "finished": "2020-09-18T00:00:01.142527Z", "userLocation": {"country": "ID"}, "created": "2020-09-18T00:00:00.606932Z", "bytes": 19968, "service": "fdsnws-dataselect", "userEmail": null, "trace": [{"cha": "BHN", "sta": "PB11", "start": "2010-09-04T11:59:52.076986Z", "net": "CX", "restricted": false, "loc": "", "bytes": 6656, "status": "OK", "end": "2010-09-04T12:03:32.076986Z"}, {"cha": "BHE", "sta": "PB11", "start": "2010-09-04T11:59:52.076986Z", "net": "CX", "restricted": false, "loc": "", "bytes": 6656, "status": "OK", "end": "2010-09-04T12:03:32.076986Z"}, {"cha": "BHZ", "sta": "PB11", "start": "2010-09-04T11:59:52.076986Z", "net": "CX", "restricted": false, "loc": "", "bytes": 6656, "status": "OK", "end": "2010-09-04T12:03:32.076986Z"}], "status": "OK", "userID": 589198147}
    """
    # Test if it's a bz2 compressed file
    if magic.from_file(filename).startswith('bzip2 compressed data'):
        logfile = bz2.BZ2File(filename)
    else:
        logfile = open(filename, 'r')
    # Initializing the counters
    statistics = {}
    line_number = 0
    with click.progressbar(logfile.readlines()) as bar:
        for jsondata in bar:
            line_number += 1
            try:
                data = json.loads(jsondata)
            except json.JSONDecodeError:
                logging.warning("Line %d could not be parsed as JSON. Ignoring", line_number)
            logging.debug(data)
            # Get the event timestamp as object
            event_month = shift_to_begin_of_month(datetime.strptime(data['finished'], '%Y-%m-%dT%H:%M:%S.%fZ',).date())
            if data['status'] == "OK":
                for trace in data['trace']:
                    try:
                        new_stat = EidaStatistic(date=event_month, network=trace['net'], station=trace['sta'], location=trace['loc'], channel=trace['cha'], country=data['userLocation']['country'])
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
                    new_stat = EidaStatistic(date=event_month, country=data['userLocation']['country'])
                except KeyError as err:
                    logging.warning("No key userlocation.country in %s", data)
                    continue
                new_stat.nb_unsuccessful_requests = 1
                new_stat.unique_clients.add_raw(mmh3.hash(str(data['userID'])))# TODO avoir son IP
                if new_stat.key() in statistics.keys():
                    statistics[new_stat.key()].aggregate(new_stat)
                else:
                    statistics[new_stat.key()] = new_stat
    return statistics

@click.command()
@click.option('--output-directory', type=click.Path(exists=True,dir_okay=True,resolve_path=True),
              help="File name prefix to write the statistics to. The full output file will be prefix_START_END.json.gz",
              default='/tmp')
@click.option('--token',
              help="Your EIDA token to the statistics webservice. Can be set by TOKEN environment variable",
              default='', envvar='TOKEN')
@click.option('--send-to',
              help="EIDA statistics webservice to post the result.")
@click.option('--version', is_flag=True)
@click.argument('files', type=click.Path(exists=True), nargs=-1)
def cli(files, output_directory, token, send_to, version):
    """
    Command line interface
    """
    if version:
        print(__version__)
        sys.exit(0)
    statslist = []
    for f in files:
        statslist.append(parse_file(f))
    statistics = {}
    for stat in statslist:
        statistics = merge_statistics(stat, statistics)

    # get start and end of statistics
    # sort statistics_dict by key, get first and last entry
    sorted_list = sorted(statistics)
    output_file = f"{output_directory}/{ sorted_list[0][0:10] }_{ sorted_list[-1][0:10] }.json.gz"
    logging.info("Statistics will be stored to Gzipped file %s", output_file)

    with gzip.open(output_file, 'wt', encoding='ascii') as dumpfile:
        dumpfile.write(json.dumps({'generated_at': datetime.now().strftime('%Y-%d-%d %H:%M:%S'),
                                   'version': __version__,
                                   'stats': [v for k,v in statistics.items()]
                                   }, default=lambda o: o.to_dict()))

        if send_to is not None and token is not None:
            logging.info("Posting stat file %s to %s", output_file, send_to)
            dumpfile.seek(0, 0)
            headers = {'Transfer-Encoding': 'gzip', 'Authorization': 'Bearer ' + token}
            requests.post(send_to, data=dumpfile, headers=headers)
