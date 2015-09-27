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

import abc
from datetime import datetime
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


class GuestLog(object):

    instance_cache = {}

    @classmethod
    def instance(cls, context, log_name):
        if log_name not in cls.instance_cache:
            cls.instance_cache[log_name] = cls(context, log_name)
        return cls.instance_cache[log_name]

    @classmethod
    def list(cls, context, app, datastore_logs):
        return [cls.instance(context, log_name).show(app)
                for log_name in datastore_logs.keys()]

    @abc.abstractmethod
    def _datastore_logs(self):
        """Returns tuple with datastore info."""

    @abc.abstractmethod
    def _enable_log(self, app, log_name, log_filename, disable):
        """Enable log on system."""

    def __init__(self, context, log_name):
        self.log_name = log_name
        self.log_size = None
        self.logging_enabled = True
        self.log_header_digest = 'abc'
        self.context = context

    @property
    def swift_client(self):
        return create_swift_client(self.context)

    def show(self, app):
        dsl_info = self._datastore_logs()[self.log_name]
        log_type, log_user, log_filename = dsl_info
        self._get_container_details()
        return {
            'name': self.log_name,
            'type': log_type,
            'status': self.logging_enabled,
            'publishable': True,
            'container': self._container_name(),
        }

    def publish_log(self, app, disable):
        dsl_info = self._datastore_logs()[self.log_name]
        log_type, log_user, log_filename = dsl_info
        self._enable_log(app, self.log_name, log_filename, disable)
        if self.logging_enabled:
            if os.path.isfile(log_filename):
                log_dir = os.path.dirname(log_filename)
                operating_system.chmod(
                    log_dir, FileMode.ADD_GRP_RX, as_root=True)
                operating_system.chmod(
                    log_filename, FileMode.ADD_ALL_R, as_root=True)
                self._publish_to_container(log_filename)
            else:
                raise RuntimeError(_(
                    "Cannot publish log file '%s' as it does not exist.") %
                    log_filename)
        return self.show(app)

    def _disable_container(self):
        c = self._container_name()
        files = [f['name'] for f in self.swift_client.get_container(c)[1]]
        for f in files:
            self.swift_client.delete_object(c, f)
        self.swift_client.delete_container(c)
        self.logging_enabled = False

    def _container_name(self):
        return CONF.guest_log_container_name % {
            'datastore': CONF.datastore_manager,
            'log': self.log_name,
            'instance_id': CONF.guest_id
        }

    def _object_name(self):
        return CONF.guest_log_object_name % {
            'timestamp': str(datetime.utcnow()).replace(' ', 'T'),
            'datastore': CONF.datastore_manager,
            'log': self.log_name,
            'instance_id': CONF.guest_id
        }

    def _set_enable_state(self, log_type, disable):
        self.logging_enabled = True
        if log_type == "USER" and disable:
            self.logging = False

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
            self.log_size = self.log_size + len(log_component) + 1

        self._get_container_details()
        self._set_container_details()
        object_header = {'X-Delete-After': CONF.guest_log_expiry}
        with open(log_filename, 'r') as log:
            LOG.debug("seeking to %s", self.log_size)
            log.seek(self.log_size)
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

    def _get_container_details(self):
        if not self.log_size:
            try:
                headers = self.swift_client.head_container(
                    self._container_name())
                self.log_size = int(headers['x-container-meta-log-size'])
                self._check_for_log_rotation(headers)
            except ClientException:
                self.log_size = 0
        else:
            self.log_size = 0
        LOG.debug("_get_container_details sets log size to %s", self.log_size)

    def _set_container_details(self):
        dsl_info = self._datastore_logs()[self.log_name]
        log_type, log_user, log_filename = dsl_info
        container_header = {
            'X-Container-Meta-Log-Name': self.log_name,
            'X-Container-Meta-Log-Type': log_type,
            'X-Container-Meta-Last-Log': log_filename,
            'X-Container-Meta-Log-Size': self.log_size,
            'X-Container-Meta-Log-Header-Digest': self.log_header_digest,
        }
        self.swift_client.put_container(self._container_name(),
                                        headers=container_header)
        LOG.debug("_set_container_details has saved log size as %s",
                  self.log_size)

    def _check_for_log_rotation(self, headers):
        """
        If the file is smaller than the last reported size
        or the first line hash is different, we can probably assume
        the file changed under our nose, in which case start from 0
        """
        dsl_info = self._datastore_logs()[self.log_name]
        log_type, log_user, log_filename = dsl_info
        logstat = os.stat(log_filename)

        if logstat.st_size < self.log_size:
            self.log_size = 0

        container_digest = headers['x-container-meta-log-header-digest']
        self._get_log_header_digest(log_filename)
        if container_digest != self.log_header_digest:
            self.log_size = 0

    def _get_log_header_digest(self, log_filename):
        with open(log_filename, 'r') as log:
            self.log_header_digest = hashlib.md5(log.readline()).hexdigest()
