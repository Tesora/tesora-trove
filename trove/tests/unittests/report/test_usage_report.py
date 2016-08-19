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

import datetime as dt

from testtools import matchers

from trove.common import cfg
from trove.common import exception
from trove.report import usage_report
from trove.tests.unittests import trove_testtools
from trove.tests.unittests.util import util

CONF = cfg.CONF

DS_VERSIONS = [('1', 'mgr1', 'v1'),
               ('2', 'mgr2', 'v2')]
INVALID_DSV_ID = 'invalid-dsv-id'

DATEFMT = '%Y-%m-%d %H:%M:%S'


class MockEvent(object):
    def __init__(self, timestamp, deleted, dsvid):
        self.timestamp = timestamp
        self.deleted = deleted
        self.dsvid = dsvid


class MockDSVersion(object):
    def __init__(self, id, manager, name):
        self.id = id
        self.manager = manager
        self.name = name


class ExpectedResults(object):
    def __init__(self, daily_hwms, overall_hwm):
        self.daily_hwms = daily_hwms
        self.overall_hwm = overall_hwm

    def get_overall_hwm(self):
        return self.overall_hwm

    def check_overall_hwm(self, hwm):
        return self.overall_hwm == hwm

    def check_daily_hwms(self, daily_hwms):
        if len(self.daily_hwms) != len(daily_hwms):
            return False

        for key in daily_hwms:
            if self.daily_hwms[key] != daily_hwms[key]:
                return False

        return True


class UsageReportTest(trove_testtools.TestCase):
    @classmethod
    def setUpClass(cls):
        util.init_db()
        # put some fake datastore versions into the usage_report DSV cache
        # so that we don't read all the DSVs from the test database
        usage_report.DSVS = list()
        for dsv in DS_VERSIONS:
            usage_report.DSVS.append(MockDSVersion(dsv[0], dsv[1], dsv[2]))

        super(UsageReportTest, cls).setUpClass()

    def setUp(self):
        super(UsageReportTest, self).setUp()

    def test_datastore_counter_invalid_dsvid(self):
        ds_counter = usage_report.DataStoreCounter()
        self.assertRaises(exception.DatastoreVersionNotFound,
                          ds_counter.increment, INVALID_DSV_ID)
        self.assertRaises(exception.DatastoreVersionNotFound,
                          ds_counter.decrement, INVALID_DSV_ID)
        self.assertRaises(exception.DatastoreVersionNotFound,
                          ds_counter.get_dsvname, INVALID_DSV_ID)

    def test_datastore_counter_getnames(self):
        ds_counter = usage_report.DataStoreCounter()
        all_names = ds_counter.get_all_dsvnames()
        for dsv in DS_VERSIONS:
            dsv_name = dsv[1] + "_" + dsv[2]
            self.assertEqual(ds_counter.get_dsvname(dsv[0]), dsv_name)
            self.assertIn(dsv_name, all_names)

    def test_datastore_counter(self):
        ds_counter = usage_report.DataStoreCounter()
        ds_counter.increment('1')
        ds_counter.increment('2')
        self.assertEqual(ds_counter.get_total_active(), 2)
        ds_counter.decrement('1')
        self.assertEqual(ds_counter.get_total_active(), 1)
        ds_counter.decrement('2')
        self.assertEqual(ds_counter.get_total_active(), 0)

        self.assertThat(ds_counter.to_dict(), matchers.MatchesDict(
            {'mgr1_v1': matchers.Equals(1), 'mgr2_v2': matchers.Equals(1)}))

    def test_daily_counter(self):
        the_date = dt.datetime.strptime('2016-08-02', '%Y-%m-%d').date()
        daily_counter = usage_report.DailyCounter(the_date)
        daily_counter.increment('1')
        daily_counter.increment('1')
        daily_counter.increment('2')
        daily_counter.decrement('1')
        self.assertEqual(3, daily_counter.get_hwm())
        self.assertEqual(2, daily_counter.get_total_active())
        daily_counter.set_hwm_to_active()
        self.assertEqual(2, daily_counter.get_hwm())
        self.assertEqual(2, daily_counter.get_total_active())

    def test_range_counter(self):
        start_date = dt.datetime.strptime('2016-08-02', '%Y-%m-%d').date()
        end_date = dt.datetime.strptime('2016-08-04', '%Y-%m-%d').date()
        range_counter = usage_report.RangeCounter(start_date, end_date)
        range_counter.increment('1')
        range_counter.increment('1')
        range_counter.increment('2')
        range_counter.decrement('1')
        self.assertEqual(3, range_counter.get_hwm())

    def test_process_data_case1(self):
        # case 1 - 1 create event per datastore with date < start_date
        start_date = dt.datetime.strptime('2016-08-02', '%Y-%m-%d').date()
        end_date = dt.datetime.strptime('2016-08-02', '%Y-%m-%d').date()

        test_results = ExpectedResults({"mgr1_v1": 1, "mgr2_v2": 1}, 2)
        case1data = list()
        case1data.extend(self._generate_mock_events(start_date -
                                                    dt.timedelta(days=1), 0))

        (daily, overall) = usage_report.process_data(case1data,
                                                     start_date,
                                                     end_date)

        self._validate_testcase(test_results, start_date,
                                end_date, daily, overall)

    def test_process_data_case2(self):
        # case 2 - 1 create event per datastore with date = start_date
        start_date = dt.datetime.strptime('2016-08-02', '%Y-%m-%d').date()
        end_date = dt.datetime.strptime('2016-08-02', '%Y-%m-%d').date()

        test_results = ExpectedResults({"mgr1_v1": 1, "mgr2_v2": 1}, 2)
        case2data = list()
        case2data.extend(self._generate_mock_events(start_date, 0))

        (daily, overall) = usage_report.process_data(case2data,
                                                     start_date,
                                                     end_date)

        self._validate_testcase(test_results, start_date,
                                end_date, daily, overall)

    def _generate_mock_events(self, event_date, is_deleted):
        # give event_date an arbitrary time component
        event_date = dt.datetime.combine(event_date, dt.time(hour=6))
        generated_events = list()
        for dsv in usage_report.DataStoreCounter()._dsv_counters:
            generated_events.append(MockEvent(event_date, is_deleted, dsv))

        return generated_events

    def _validate_testcase(self, test_results, start_date,
                           end_date, daily, overall):
        # the daily structure should contain end_date - start_date entries
        self.assertEqual((end_date - start_date).days + 1, len(daily))

        # check that the daily high watermarks are what was expected
        for day in daily:
            self.assertTrue(test_results.check_daily_hwms(day.to_dict()),
                            "Daily high watermark for date %s doesn't "
                            "match expected result" % day.get_date())

        # check that the overall high watermark is what was expected
        self.assertTrue(test_results.check_overall_hwm(overall.get_hwm()),
                        "Overall high watermark of %d doesn't match "
                        "expected result %d" %
                        (overall.get_hwm(), test_results.get_overall_hwm()))
