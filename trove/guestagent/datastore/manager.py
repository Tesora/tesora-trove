# Copyright 2014 Tesora, Inc.
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
#

import abc

from oslo_log import log as logging
from oslo_service import periodic_task

from trove.common import cfg
from trove.common.i18n import _


LOG = logging.getLogger(__name__)
CONF = cfg.CONF


class Manager(periodic_task.PeriodicTasks):
    """This is the base class for all datastore managers.  Over time, common
    functionality should be pulled back here from the existing managers.
    """

    def __init__(self):

        super(Manager, self).__init__(CONF)

        # Manager properties
        self.__status = None
        self.__error_occurred = False

    @abc.abstractproperty
    def status(self):
        return self.__status

    @status.setter
    def status(self, status):
        self.__status = status

    @property
    def error_occurred(self):
        return self.__error_occurred

    @error_occurred.setter
    def error_occurred(self, error_occurred):
        self.__error_occurred = error_occurred

    @periodic_task.periodic_task
    def update_status(self, context):
        """Updates the redis trove instance. It is decorated with
        perodic task so it is automatically called every 3 ticks.
        """
        LOG.debug("Update status called.")
        self.status.update()

    def prepare(self, context, packages, databases, memory_mb, users,
                device_path=None, mount_point=None, backup_info=None,
                config_contents=None, root_password=None, overrides=None,
                cluster_config=None, snapshot=None):
        """Set up datastore on a Guest Instance."""
        LOG.info(_("Starting datastore prepare."))
        self.status.begin_install()
        post_processing = True if cluster_config else False
        try:
            self.do_prepare(
                context, packages, databases, memory_mb, users,
                device_path=device_path, mount_point=mount_point,
                backup_info=backup_info, config_contents=config_contents,
                root_password=root_password, overrides=overrides,
                cluster_config=cluster_config, snapshot=snapshot)
        except Exception:
            self.error_occurred = True
            LOG.exception("An error occurred preparing datastore")
            raise
        finally:
            LOG.info(_("Ending datastore prepare."))
            self.status.end_install(error_occurred=self.error_occurred,
                                    post_processing=post_processing)
        LOG.info(_('Completed setup of datastore successfully.'))

    @abc.abstractmethod
    def do_prepare(self, context, packages, databases, memory_mb, users,
                   device_path, mount_point, backup_info, config_contents,
                   root_password, overrides, cluster_config, snapshot):
        """This is called from prepare when the Trove instance first comes
        online.  'Prepare' is the first rpc message passed from the
        task manager.  do_prepare handles all the base configuration of
        the instance and is where the actual work is done.  Each datastore
        must implement this method.
        """
        pass

    def cluster_complete(self, context):
        LOG.debug("Cluster creation complete, starting status checks.")
        self.status.end_install()
