# Copyright 2016 Tesora Inc.
# All Rights Reserved.
#
#    Licensed under the Apache License, Version 2.0 (the "License"); you may
#    not use this file except in compliance with the License. You may obtain
#    a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
#    WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
#    License for the specific language governing permissions and limitations
#    under the License.

import copy
import csv
import datetime

from oslo_log import log as logging
from sqlalchemy.sql.expression import literal_column
from sqlalchemy import text

from trove.common import cfg
from trove.common import exception
from trove.datastore.models import DBDatastoreVersion
from trove.db import get_db_api
from trove.instance.models import DBInstance

LOG = logging.getLogger(__name__)
CONF = cfg.CONF
db_api = get_db_api()

DSVS = None


class DataStoreCounter(object):
    def __init__(self):
        global DSVS
        self._dsv_counters = {}

        # Load all the datastore versions into a dict to track
        # active instance counts and initialize the counters to
        # 0.
        if not DSVS:
            # cache the list since it isn't expected to change
            # during the life of the report run and this object
            # will get instantiated a bunch of times.
            DSVS = DBDatastoreVersion.find_all()

        for dsv in DSVS:
            dsvname = "%s_%s" % (dsv.manager, dsv.name)
            # The dict key is the DSV Id
            # The list is DSV Name, Active Counter & High Watermark
            self._dsv_counters[dsv.id] = [dsvname, 0, 0]

    def increment(self, dsv_id):
        if not self._is_dsv_valid(dsv_id):
            raise exception.DatastoreVersionNotFound(dsv_id)

        counter = self._dsv_counters[dsv_id]
        counter[1] += 1
        if counter[1] > counter[2]:
            counter[2] = counter[1]

    def decrement(self, dsv_id):
        if not self._is_dsv_valid(dsv_id):
            raise exception.DatastoreVersionNotFound(dsv_id)

        counter = self._dsv_counters[dsv_id]
        counter[1] -= 1

    def get_total_active(self):
        active_count = 0
        for key in self._dsv_counters:
            active_count += self._dsv_counters[key][1]
        return active_count

    def get_dsvname(self, dsv_id):
        if not self._is_dsv_valid(dsv_id):
            raise exception.DatastoreVersionNotFound(dsv_id)

        return self._dsv_counters[dsv_id][0]

    def get_all_dsvnames(self):
        names = []
        for key in self._dsv_counters:
            names.append(self._dsv_counters[key][0])

        return names

    def to_dict(self):
        values = {}
        for key in self._dsv_counters:
            values[self._dsv_counters[key][0]] = self._dsv_counters[key][2]

        return values

    def _is_dsv_valid(self, dsv_id):
        return dsv_id in self._dsv_counters


class DailyCounter(DataStoreCounter):
    def __init__(self, the_date):
        self._date = the_date
        self._hwm = 0
        super(DailyCounter, self).__init__()

    def increment(self, dsv_id):
        super(DailyCounter, self).increment(dsv_id)
        current_active = self.get_total_active()
        if current_active > self._hwm:
            self._hwm = current_active

    def set_hwm_to_active(self):
        self._hwm = 0
        for key in self._dsv_counters:
            self._dsv_counters[key][2] = self._dsv_counters[key][1]
            self._hwm += self._dsv_counters[key][2]

    def get_hwm(self):
        return self._hwm

    def set_date(self, the_date):
        self._date = the_date

    def get_date(self):
        return self._date


class RangeCounter(DataStoreCounter):
    def __init__(self, start_date, end_date):
        self._start_date = start_date
        self._end_date = end_date
        self.hwm = 0
        super(RangeCounter, self).__init__()

    def increment(self, dsv_id):
        super(RangeCounter, self).increment(dsv_id)
        current_active = self.get_total_active()
        if current_active > self.hwm:
            self.hwm = current_active

    def get_date_range(self):
        return self._start_date, self._end_date

    def get_hwm(self):
        return self.hwm


def adjust_counters(event, daily_ctr, range_ctr):
    if event.deleted == 0:
        LOG.debug("Created %s instance at %s" %
                  (daily_ctr.get_dsvname(event.dsvid),
                   event.timestamp))
        daily_ctr.increment(event.dsvid)
        range_ctr.increment(event.dsvid)
    else:
        LOG.debug("Deleted %s instance at %s" %
                  (daily_ctr.get_dsvname(event.dsvid),
                   event.timestamp))
        daily_ctr.decrement(event.dsvid)
        range_ctr.decrement(event.dsvid)


def process_data(data, start_date, end_date):
    daily_counter = list()
    daily_counter.append(DailyCounter(start_date))
    overall_counter = RangeCounter(start_date, end_date)
    current_day = start_date

    daily_counter_idx = 0
    for event in data:
        event_date = event.timestamp.date()
        current_day = daily_counter[daily_counter_idx].get_date()
        LOG.debug("Processing event with timestamp %s for current day %s" %
                  (event.timestamp, current_day))

        if event_date > current_day:
            # an event happened after the date we are processing
            # need to forward the current_day up to the event_date
            # and store the counters for each day
            while current_day < event_date:
                daily_counter_copy = copy.deepcopy(
                    daily_counter[daily_counter_idx])
                current_day += datetime.timedelta(days=1)
                daily_counter_copy.set_date(current_day)
                daily_counter_copy.set_hwm_to_active()
                daily_counter.append(daily_counter_copy)
                daily_counter_idx += 1

        adjust_counters(event, daily_counter[daily_counter_idx],
                        overall_counter)

    # fill up any days after the last event
    while current_day < end_date:
        daily_counter_copy = copy.deepcopy(
            daily_counter[daily_counter_idx])
        current_day += datetime.timedelta(days=1)
        daily_counter_copy.set_date(current_day)
        daily_counter_copy.set_hwm_to_active()
        daily_counter.append(daily_counter_copy)

    return daily_counter, overall_counter


def _run_query(start_date, end_date):
    created_filters = [DBInstance.created < end_date,
                       DBInstance.deleted == 0]
    created_columns = [DBInstance.created.label('timestamp'),
                       literal_column("0").label('deleted'),
                       DBDatastoreVersion.id.label('dsvid')]
    deleted_filters = [DBInstance.created < end_date,
                       DBInstance.deleted_at >= start_date,
                       DBInstance.deleted == 1]
    deleted_columns = [DBInstance.deleted_at.label('timestamp'),
                       literal_column("1").label('deleted'),
                       DBDatastoreVersion.id.label('dsvid')]

    query1 = DBInstance.query().\
        join(DBDatastoreVersion).\
        add_columns(*created_columns)
    query1 = query1.filter(*created_filters)

    query2 = DBInstance.query().\
        join(DBDatastoreVersion).\
        add_columns(*created_columns)
    query2 = query2.filter(*deleted_filters)

    query3 = DBInstance.query().\
        join(DBDatastoreVersion).\
        add_columns(*deleted_columns)
    query3 = query3.filter(*deleted_filters)

    union_query = query1.union(query2, query3).\
        order_by(text('anon_1.timestamp'))

    return union_query.all()


def usage_report(start_date, end_date, output_file):

    start_date_dt = datetime.datetime.strptime(start_date, '%Y-%m-%d')
    start_date_d = start_date_dt.date()
    end_date_dt = datetime.datetime.strptime(end_date, '%Y-%m-%d') +\
        datetime.timedelta(hours=23, minutes=59, seconds=59)
    end_date_d = end_date_dt.date()

    if end_date_dt <= start_date_dt:
        raise Exception("ERROR: Start date (%s) must be before end date (%s)."
                        % (start_date_d, end_date_d))

    db_api.configure_db(CONF)

    LOG.debug("Calling run_query for range %s to %s" %
              (start_date_dt, end_date_dt))
    result = _run_query(start_date_dt, end_date_dt)

    LOG.debug("Calling process_data with %s rows" % len(result))
    (daily, overall) = process_data(result, start_date_d, end_date_d)

    LOG.debug("Generating CSV file (%s)" % output_file)
    with open(output_file, 'w') as outfile:
        csv_fields = ['start_date', 'end_date']
        csv_fields = csv_fields + overall.get_all_dsvnames()
        csv_fields.append('overall')
        outwriter = csv.DictWriter(outfile, csv_fields)
        outwriter.writeheader()

        for day in daily:
            outdata = {'start_date': day.get_date(),
                       'end_date': day.get_date()}
            outdata.update(day.to_dict())
            outdata.update({'overall': day.get_hwm()})
            outwriter.writerow(outdata)

        outdata = {'start_date': overall.get_date_range()[0],
                   'end_date': overall.get_date_range()[1]}
        outdata.update(overall.to_dict())
        outdata.update({'overall': overall.get_hwm()})
        outwriter.writerow(outdata)

        print("High watermark for range %s - %s is %d" %
              (overall.get_date_range()[0].strftime('%Y-%m-%d'),
               overall.get_date_range()[1].strftime('%Y-%m-%d'),
               overall.get_hwm()))
        print("Detailed output written to %s" % outfile.name)
