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

from datetime import datetime
import enum
import hashlib
import os

from oslo_log import log as logging
from swiftclient.client import ClientException

from trove.common import cfg
from trove.common.i18n import _
from trove.common.remote import create_swift_client
from trove.guestagent.common import operating_system
from trove.guestagent.common.operating_system import FileMode


LOG = logging.getLogger(__name__)
CONF = cfg.CONF


class LogType(enum.Enum):
    """Represent the type of the log object."""

    # System logs.  These are always enabled.
    SYS = 1

    # User logs.  These can be enabled or disabled.
    USER = 2


class LogStatus(enum.Enum):
    """Represent the status of the log object."""

    # The log is disabled and potentially no data is being written to
    # the corresponding log file
    Disabled = 1

    # Logging is on, but no determination has been made about data availability
    Enabled = 2

    # Logging is on, but no log data is available to publish
    Unavailable = 2

    # Logging is on and data is available to be published
    Ready = 3

    # Logging is on and all data has been published
    Published = 4

    # Logging is on and some data has been published
    Partial = 5


class GuestLog(object):

    XCM_LOG_NAME = 'x-container-meta-log-name'
    XCM_LOG_TYPE = 'x-container-meta-log-type'
    XCM_LOG_FILE = 'x-container-meta-log-file'
    XCM_LOG_SIZE = 'x-container-meta-log-size'
    XCM_LOG_HEAD = 'x-container-meta-log-header-digest'

    def __init__(self, log_context, log_name, log_type, log_user, log_file,
                 log_exposed):
        self._context = log_context
        self._name = log_name
        self._type = log_type
        self._user = log_user
        self._file = log_file
        self._exposed = log_exposed
        self._size = None
        self._published_size = None
        self._header_digest = 'abc'
        self._published_header_digest = None
        self._status = None
        self._cached_context = None
        self._cached_swift_client = None

        self._set_status(self._type == LogType.USER,
                         LogStatus.Disabled, LogStatus.Enabled)

    @property
    def context(self):
        return self._context

    @context.setter
    def context(self, context):
        self._context = context

    @property
    def type(self):
        return self._type

    @property
    def swift_client(self):
        if not self._cached_swift_client or (
                self._cached_context != self.context):
            self._cached_swift_client = create_swift_client(self.context)
            self._cached_context = self.context
        return self._cached_swift_client

    @property
    def exposed(self):
        return self._exposed or self.context.is_admin

    def _set_status(self, use_first, first_status, second_status):
        if use_first:
            self._status = first_status
        else:
            self._status = second_status

    def show(self):
        show_details = None
        if self.exposed:
            self._refresh_details()
            container_name = 'None'
            if self._published_size:
                container_name = self._container_name()
            show_details = {
                'name': self._name,
                'type': self._type.name,
                'status': self._status.name,
                'published': self._published_size,
                'pending': self._size - self._published_size,
                'container': container_name,
            }
        return show_details

    def _refresh_details(self):

        headers = None
        if self._published_size is None:
            # Initializing, so get all the values
            try:
                headers = self.swift_client.head_container(
                    self._container_name())
                self._published_size = int(headers[self.XCM_LOG_SIZE])
                self._published_header_digest = headers[self.XCM_LOG_HEAD]
            except ClientException:
                self._published_size = 0

        self._update_details()
        LOG.debug("Log size for '%s' set to %d (published %d)" % (
            self._name, self._size, self._published_size))

    def _update_details(self):
        if os.path.isfile(self._file):
            # Make sure we can read the file
            log_dir = os.path.dirname(self._file)
            operating_system.chmod(
                log_dir, FileMode.ADD_GRP_RX, as_root=True)
            operating_system.chmod(
                self._file, FileMode.ADD_ALL_R, as_root=True)

            logstat = os.stat(self._file)
            self._size = logstat.st_size
            self._update_log_header_digest(self._file)

            # Check for potential log rotation
            if self._published_size > 0 and self._log_rotated():
                LOG.debug("Log file rotation detected for '%s'" % self._name)
                self._delete_container()

            # We have stuff to publish
            if logstat.st_size > self._published_size:
                self._set_status(self._published_size,
                                 LogStatus.Partial, LogStatus.Ready)
            # We've published everything so far
            elif logstat.st_size == self._published_size:
                self._set_status(self._published_size,
                                 LogStatus.Published, LogStatus.Enabled)
            # We've already handled this case (log rotated) so what gives?
            else:
                raise ("Bug in _log_rotated ?")
        else:
            self._published_size = 0
            self._size = 0

        if not self._size:
            self._set_status(self._type == LogType.USER,
                             LogStatus.Disabled, LogStatus.Unavailable)

    def _log_rotated(self):
        """If the file is smaller than the last reported size
        or the first line hash is different, we can probably assume
        the file changed under our nose.
        """
        if (self._size < self._published_size or
                self._published_header_digest != self._header_digest):
            return True

    def _update_log_header_digest(self, log_file):
        with open(log_file, 'r') as log:
            self._header_digest = hashlib.md5(log.readline()).hexdigest()

    def _container_name(self):
        return CONF.guest_log_container_name % {
            'datastore': CONF.datastore_manager,
            'log': self._name,
            'instance_id': CONF.guest_id
        }

    def publish_log(self, disable):
        if disable:
            self._delete_container()
        else:
            if os.path.isfile(self._file):
                self._publish_to_container(self._file)
            else:
                raise RuntimeError(_(
                    "Cannot publish log file '%s' as it does not exist.") %
                    self._file)
        return self.show()

    def _delete_container(self):
        c = self._container_name()
        files = [f['name'] for f in self.swift_client.get_container(c)[1]]
        for f in files:
            self.swift_client.delete_object(c, f)
        self.swift_client.delete_container(c)
        self._status = LogStatus.Disabled
        self._published_size = 0

    def _publish_to_container(self, log_filename):
        log_component, log_lines = '', 0
        chunk_size = CONF.guest_log_limit

        def _read_chunk(f):
            while True:
                chunk = f.read(chunk_size)
                if not chunk:
                    break
                yield chunk

        def _write_log_component():
            object_header.update({'X-Object-Meta-Lines': log_lines})
            self.swift_client.put_object(self._container_name(),
                                         self._object_name(), log_component,
                                         headers=object_header)
            self._published_size = (
                self._published_size + len(log_component))
            self._published_header_digest = self._header_digest

        self._refresh_details()
        self._set_container_details()
        object_header = {'X-Delete-After': CONF.guest_log_expiry}
        with open(log_filename, 'r') as log:
            LOG.debug("seeking to %s", self._published_size)
            log.seek(self._published_size)
            for chunk in _read_chunk(log):
                for log_line in chunk.splitlines():
                    if len(log_component) + len(log_line) > chunk_size:
                        _write_log_component()
                        log_component, log_lines = '', 0
                    log_component = log_component + log_line + '\n'
                    log_lines += 1
        if log_lines > 0:
            _write_log_component()
        self._set_container_details()

    def _object_name(self):
        return CONF.guest_log_object_name % {
            'timestamp': str(datetime.utcnow()).replace(' ', 'T'),
            'datastore': CONF.datastore_manager,
            'log': self._name,
            'instance_id': CONF.guest_id
        }

    def _set_container_details(self):
        container_header = {
            self.XCM_LOG_NAME: self._name,
            self.XCM_LOG_TYPE: self._type,
            self.XCM_LOG_FILE: self._file,
            self.XCM_LOG_SIZE: self._size,
            self.XCM_LOG_HEAD: self._header_digest,
        }
        self.swift_client.put_container(self._container_name(),
                                        headers=container_header)
        LOG.debug("_set_container_details has saved log size as %s",
                  self._published_size)
