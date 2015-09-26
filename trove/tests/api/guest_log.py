# Copyright 2015 Tesora Inc.
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

from oslo_concurrency import processutils
from proboscis.asserts import assert_equal
from proboscis.asserts import assert_false
from proboscis.asserts import assert_raises
from proboscis import before_class
from proboscis import test
import swiftclient
from trove.common import cfg
from trove.common import utils
from trove.common.utils import generate_uuid
from trove.common.utils import poll_until
from trove.guestagent.datastore.mysql.mysql_guest_log import MySQLGuestLog
from trove import tests
from trove.tests.api.instances import instance_info
from trove.tests.api.instances import WaitForGuestInstallationToFinish
from trove.tests.util.server_connection import create_server_connection
from trove.tests.util import test_config
from trove.tests.util.users import Requirements


GROUP = "dbaas.api.guest_log"

CKSUM_CMD = 'sha1sum'

backup_info = None
incremental_info = None
incremental_db = generate_uuid()
incremental_restore_instance_id = None
total_num_dbs = 0
backup_count_prior_to_create = 0
backup_count_for_instance_prior_to_create = 0

# For remote.py we need the actual swift endpoint
cfg.CONF.swift_url = test_config.swift_url


class LoggingUtilMixin(object):

    def wait_for_log_operation(self):
        def result_is_active():
            instance = instance_info.dbaas.instances.get(instance_info.id)
            if instance.status == "ACTIVE":
                return True
            else:
                assert_equal(instance.status, "LOGGING")
                return False

        poll_until(result_is_active)

    def cleanup_swift(self):
        """Remove all log files for this instance."""
        root_containers = self.swift_client.get_container('')
        inst_containers = [c['name'] for c in root_containers[1]
                           if c['name'].endswith(instance_info.id)]
        for i in inst_containers:
            # swift can't delete containers that have files with the API?
            # have to delete each object manually...
            files = [f['name'] for f in self.swift_client.get_container(i)[1]]
            for f in files:
                self.swift_client.delete_object(i, f)
            self.swift_client.delete_container(i)

    def compare_files(self, f1, f2):
        """Compare two files to see if they are the same."""
        out, err = processutils.execute('diff', '-q', f1, f2)
        cksum1, err = processutils.execute(CKSUM_CMD, f1)
        cksum2, err = processutils.execute(CKSUM_CMD, f2)
        assert_equal(cksum1.split(" ")[0], cksum2.split(" ")[0])

    def check_log_container_invariants(self, container):
        """Interesting invariants about the log container like:
        - files are all <= the max size
        - container metadata matches the actual file sizes
        - other interesting checks like last modified date, etc.
        """
        c = self.swift_client.get_container(container)

        total_filesize = sum([f['bytes'] for f in c[1]])
        header_filesize = c[0]['x-container-bytes-used']
        assert_equal(int(total_filesize), int(header_filesize))

        large_files = [f['name'] for f in c[1]
                       if f['bytes'] > cfg.CONF.guest_log_limit]
        assert_false(large_files)


@test(depends_on_classes=[WaitForGuestInstallationToFinish],
      groups=[GROUP, tests.INSTANCES])
class GuestLogPublish(LoggingUtilMixin):

    def __init__(self):
        self.instances = instance_info.dbaas.instances
        self.swift_client = None
        self.server = None

    @before_class
    def prepare_test(self):
        """Connect to swift, clean up containers."""
        self.server = create_server_connection(instance_info.id)

        user = test_config.users.find_user(
            Requirements(is_admin=False, services=["swift"]))
        self.swift_client = swiftclient.client.Connection(
            authurl=test_config.nova_client['auth_url'],
            user=user.auth_user,
            key=user.auth_key,
            tenant_name=user.tenant,
            auth_version='2')
        self.cleanup_swift()

    @test
    def test_log_list(self):
        l = list(self.instances.log_list(instance_info.id))

        # Should be two SYS types
        assert_equal(len([log.type for log in l if log.type == 'SYS']), 2)
        # All four publishable by default
        assert_equal(len([log.publishable for log in l if log.publishable]), 4)

        names = [log.name for log in l]
        assert_equal(names.count('error'), 1)
        assert_equal(names.count('slow_query'), 1)
        assert_equal(names.count('guest'), 1)
        assert_equal(names.count('general'), 1)

    @test
    def test_log_save_fresh(self):
        self.cleanup_swift()
        f = '/tmp/' + generate_uuid() + '.log'
        f2 = self.instances.log_save(instance_info.id, log='error',
                                     publish=True, filename=f)
        assert_equal(f, f2)
        copied = '/tmp/' + generate_uuid() + '.log'
        logfile = MySQLGuestLog.datastore_logs['error'][2]
        # TODO(atomic77) How to tell what user the tests should use on GA?
        self.server.scp(logfile, copied, user='fedora',
                        dest_is_remote=False)
        self.compare_files(f, copied)
        utils.execute_with_timeout("rm", f)
        utils.execute_with_timeout("rm", copied)

    @test
    def test_log_publish_fresh_log(self):
        self.cleanup_swift()
        l = self.instances.log_publish(instance_info.id, 'error')
        assert_equal(l.name, 'error')
        assert_equal(l.type, 'SYS')
        container = 'log-mysql-error-' + instance_info.id
        assert_equal(l.container, container)
        self.wait_for_log_operation()
        self.check_log_container_invariants(container)

    @test(depends_on=[test_log_publish_fresh_log])
    def test_log_publish_twice_nowait(self):
        self.cleanup_swift()
        container = 'log-mysql-error-' + instance_info.id
        l = self.instances.log_publish(instance_info.id, 'error')
        l2 = self.instances.log_publish(instance_info.id, 'error')
        assert_equal(l.name, l2.name)
        self.wait_for_log_operation()
        self.check_log_container_invariants(container)

    @test
    def test_log_disable(self):
        self.instances.log_publish(instance_info.id, 'error', disable=True)
        self.wait_for_log_operation()
        container = 'log-mysql-error-' + instance_info.id
        # Check that the container is actually gone
        assert_raises(swiftclient.exceptions.ClientException,
                      self.swift_client.get_container(container))

    @test
    def test_log_generator_simple(self):
        g = self.instances.log_generator(instance_info.id, log='error',
                                         publish=True, lines=10)
        log_contents = "".join([chunk for chunk in g()])
        assert_equal(len(log_contents.splitlines()), 10)

    @test
    def test_log_generator_passed_in_client(self):
        g = self.instances.log_generator(instance_info.id, log='error',
                                         publish=True, lines=10,
                                         swift=self.swift_client)
        log_contents = "".join([chunk for chunk in g()])
        assert_equal(len(log_contents.splitlines()), 10)

    @test(enabled=False)
    def test_non_overlapping_publish(self):
        # TODO(atomic77) Confirm we are not publishing the same logs twice
        pass

    @test(enabled=False)
    def test_log_file_larger_than_chunksize(self):
        # TODO(atomic77) Generate a large log file e.g. turn on general_log
        # and write a bunch of inserts
        pass

    @test(enabled=False)
    def test_log_expiry(self):
        # TODO(atomic77) Confirm, with very short log expiry, that swift
        # is auto-purging files
        pass

    @test
    def test_log_rotate(self):
        cmd = "sudo /usr/sbin/logrotate -f /etc/logrotate.conf"
        out, err = self.server.execute(cmd)
        # cmd = "sudo restart mysql"
        # out, err = self.server.execute(cmd)
        self.instances.log_publish(instance_info.id, 'error')
        container = 'log-mysql-error-' + instance_info.id
        self.check_log_container_invariants(container)
