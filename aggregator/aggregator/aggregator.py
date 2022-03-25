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
from fdsnnetextender import FdsnNetExtender

net_extender = FdsnNetExtender()
logging.basicConfig(level=logging.INFO, format='%(levelname)s %(message)s')

class EidaStatistic:
    """
    One statistic object.
    """

    def __init__(self, date=datetime.now(), network="", station="", location="--", channel="", country=""):
        """
        Class initialisation
        """
        self.original_day = date
        self.network = network
        self.station = station
        self.location = location
        self.channel = channel
        self.country = country
        self.size = 0
        self.nb_requests = 1
        self.nb_successful_requests = 0
        self.nb_unsuccessful_requests = 0
        self.unique_clients = HLL(11, 5)

    def _shift_to_begin_of_month(self):
        """
        Set the date as the first day of month
        :param event_datetime is a DateTime or Date object. Must have a weekday() method.
        """
        if not isinstance(self.original_day, date):
            raise TypeError("datetime.date expected")
        return self.original_day - timedelta(days=(self.original_day.day-1))

    def key(self):
        """
        Generate a unique key for this object in order to ease
        """
        return f"{self._shift_to_begin_of_month()}_{self.network}_{self.station}_{self.location}_{self.channel}_{self.country}"

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
        return f"{self.original_day} {self.network} {self.station} {self.location} {self.channel} from {self.country} {self.size}b {self.nb_successful_requests} successful requests from {self.unique_clients.cardinality()} unique clients"

    def to_dict(self):
        """
        Dump the statistic as a dictionary
        """
        unique_clients_bytes = self.unique_clients.to_bytes()
        json_dict = {'month': str(self._shift_to_begin_of_month()),
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

class StatCollection():
    """ This object contains a list of EidaStatistics and some metadata related to the aggregation processing
    """

    def __init__(self):
        """
        :var _stats_days is a list of dates concerning the statistics collection. It is used as metadata to estimate month coverage
        """
        self._stats_dates = []
        self._generated_at = datetime.now()
        self._statistics = {}
        self.nbevents = 0

    def append(self, stat):
        """
        Append an EidaStatistic object into the collection
        :param stat is an EidaStatistic instance
        """
        if stat.key() in self._statistics:
            # append
            self._statistics[stat.key()].aggregate(stat)
        else:
            # create new stat
            self._statistics[stat.key()] = stat
        if stat.original_day not in self._stats_dates:
            self._stats_dates.append(stat.original_day)

#    def __iadd__(self, statcoll):
#        """
#        Incremental add
#        """
#        for k,stat in statcoll._statistics.items():
#            self.append(stat)
#        for day in statcoll._stats_dates:
#            if day not in self._stats_dates:
#                self._stats_dates.append(day)

    def get_days(self):
        return sorted(self._stats_dates)

    def to_json(self):
        """
        Dump the object as a dictionary
        """
        return json.dumps(
            {'generated_at': self._generated_at.strftime('%Y-%m-%d %H:%M:%S'),
             'version': __version__,
             'days_coverage': [ d.strftime('%Y-%m-%d') for d in sorted(self._stats_dates) ],
             'aggregation_score': round(self.nbevents/len(self._statistics)),
             'stats': [v for k,v in self._statistics.items()]
             }, default=lambda o: o.to_dict()
        )

    def parse_file(self,filename):
        """
        Parse the file provided in order to aggregate the data.
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
        line_number = 0
        with click.progressbar(logfile.readlines(), label=f"Parsing {filename}") as bar:
            for jsondata in bar:
                line_number += 1
                try:
                    data = json.loads(jsondata)
                except json.JSONDecodeError:
                    logging.warning("Line %d could not be parsed as JSON. Ignoring", line_number)
                logging.debug(data)
                # Get the event timestamp as object
                try:
                    event_date = datetime.strptime(data['finished'], '%Y-%m-%dT%H:%M:%S.%fZ',).date()
                except ValueError:
                    try:
                        event_date = datetime.strptime(data['finished'], '%Y-%m-%dT%H:%M:%SZ',).date()
                    except ValueError:
                        logging.warning("Could not parse date %s", data['finished'])
                        continue
                try:
                    countrycode = data['userLocation']['country']
                except KeyError:
                    logging.warning("Key error for data %s")
                    countrycode = ""
                if data['status'] == "OK":
                    for trace in data['trace']:
                        try:
                            extended_network = net_extender.extend(trace['net'], trace['start'][0:10])
                        except ValueError as err:
                            logging.error(err)
                            sys.exit(1)
                        new_stat = EidaStatistic(date=event_date, network=extended_network, station=trace['sta'], location=trace['loc'], channel=trace['cha'], country=countrycode)
                        new_stat.nb_successful_requests = 1
                        new_stat.size = trace['bytes']
                        new_stat.unique_clients.add_raw(mmh3.hash(str(data['userID'])))
                        self.append(new_stat)
                else:
                    # TODO This is not very DRY but I did'nt figure a better way to do it for now
                    new_stat = EidaStatistic(date=event_date, country=countrycode)
                    new_stat.nb_unsuccessful_requests = 1
                    new_stat.unique_clients.add_raw(mmh3.hash(str(data['userID'])))
                    self.append(new_stat)
        self.nbevents += line_number

    def nbaggs(self):
        return len(self._statistics)

# Global instance of statcollection

@click.command()
@click.option('--output-directory', '-o', type=click.Path(exists=True,dir_okay=True,resolve_path=True),
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
    statistics = StatCollection()
    for f in files:
        statistics.parse_file(f)

    logging.info("Generated %s aggregations from %s events. Aggregation score is %s", statistics.nbaggs(), statistics.nbevents, round(statistics.nbevents/statistics.nbaggs(), 1))
    # get start and end of statistics
    # sort statistics_dict by key, get first and last entry
    sorted_list = statistics.get_days()
    output_file = f"{output_directory}/{ sorted_list[0] }_{ sorted_list[-1] }.json.gz"
    logging.debug("Statistics will be stored to Gzipped file %s", output_file)

    payload = gzip.compress(statistics.to_json().encode('utf-8'))
    with open(output_file, 'wb') as dumpfile:
        dumpfile.write(payload)
        logging.info("Statistics stored to %s", output_file)

    if send_to is not None and token is not None:
        logging.info("Posting stat file %s to %s", output_file, send_to)
        headers = {'Authentication': 'Bearer ' + token, 'Content-Type': 'application/json'}
        r = requests.post(send_to, data=statistics.to_json(), headers=headers)
        logging.info(r.text)
