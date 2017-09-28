from __future__ import absolute_import
import json
from collections import OrderedDict
from exporters.stats_managers.basic_stats_manager import BasicStatsManager
import six


class LoggingStatsManager(BasicStatsManager):
    """
    This stats manager prints a log message with useful stats and times for every
    pipeline iteration.
    """

    def iteration_report(self, times):
        prev = times['started']
        times.pop('started')
        data = OrderedDict()
        for field, value in six.iteritems(times):
            data[field] = (value - prev).total_seconds()
            prev = value
        self.logger.info(json.dumps(data))
