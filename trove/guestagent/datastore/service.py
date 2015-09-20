# Copyright 2011 OpenStack Foundation
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


import os
import time

from oslo_log import log as logging

from trove.common import cfg
from trove.common import context as trove_context
from trove.common.i18n import _
from trove.common import instance
from trove.conductor import api as conductor_api
from trove.guestagent.common import guestagent_utils
from trove.guestagent.common import operating_system
from trove.guestagent.common import timeutils

LOG = logging.getLogger(__name__)
CONF = cfg.CONF


class BaseDbStatus(object):
    """
    Answers the question "what is the status of the DB application on
    this box?" The answer can be that the application is not installed, or
    the state of the application is determined by calling a series of
    commands.

    This class also handles saving and load the status of the DB application
    in the database.
    The status is updated whenever the update() method is called, except
    if the state is changed to building or restart mode using the
     "begin_install" and "begin_restart" methods.
    The building mode persists in the database while restarting mode does
    not (so if there is a Python Pete crash update() will set the status to
    show a failure).
    These modes are exited and functionality to update() returns when
    end_install or end_restart() is called, at which point the status again
    reflects the actual status of the DB app.

    This is a base class, subclasses must implement real logic for
    determining current status of DB in _get_actual_db_status()
    """

    _instance = None

    GUESTAGENT_DIR = '~'
    PREPARE_START_FILENAME = '.guestagent.prepare.start'
    PREPARE_END_FILENAME = '.guestagent.prepare.end'

    def __init__(self):
        if self._instance is not None:
            raise RuntimeError("Cannot instantiate twice.")
        self.status = None
        self.restart_mode = False

        self._prepare_completed = None

    @property
    def prepare_completed(self):
        if self._prepare_completed is None:
            self._prepare_completed = os.path.isfile(
                guestagent_utils.build_file_path(
                    self.GUESTAGENT_DIR, self.PREPARE_END_FILENAME))
        return self._prepare_completed

    @prepare_completed.setter
    def prepare_completed(self, value):
        # Set the value based on the existence of the file; 'value' is ignored
        self._prepare_completed = os.path.isfile(
            guestagent_utils.build_file_path(
                self.GUESTAGENT_DIR, self.PREPARE_END_FILENAME))

    def begin_install(self):
        """Called right before DB is prepared."""
        prepare_start_file = guestagent_utils.build_file_path(
            self.GUESTAGENT_DIR, self.PREPARE_START_FILENAME)
        operating_system.write_file(prepare_start_file, '')
        self.prepare_completed = False

        self.set_status(instance.ServiceStatuses.BUILDING, True)

    def begin_restart(self):
        """Called before restarting DB server."""
        self.restart_mode = True

    def end_install(self, error_occurred=False, post_processing=False):
        """Called after prepare completes."""

        # Set the "we're done" flag if there's no error and
        # no post_processing is necessary
        if not (error_occurred or post_processing):
            prepare_end_file = guestagent_utils.build_file_path(
                self.GUESTAGENT_DIR, self.PREPARE_END_FILENAME)
            operating_system.write_file(prepare_end_file, '')
            self.prepare_completed = True

        final_status = None
        if error_occurred:
            final_status = instance.ServiceStatuses.FAILED
        elif post_processing:
            final_status = instance.ServiceStatuses.INSTANCE_READY

        if final_status:
            LOG.info(_("Set final status to %s.") % final_status)
            self.set_status(final_status, force=True)
        else:
            self._end_install_or_restart(True)

    def end_restart(self):
        self.restart_mode = False
        LOG.info(_("Ending restart."))
        self._end_install_or_restart(False)

    def _end_install_or_restart(self, force):
        """Called after DB is installed or restarted.
        Updates the database with the actual DB server status.
        """
        real_status = self._get_actual_db_status()
        LOG.info(_("Current database status is '%s'.") % real_status)
        self.set_status(real_status, force=force)

    def _get_actual_db_status(self):
        raise NotImplementedError()

    @property
    def is_installed(self):
        """
        True if DB app should be installed and attempts to ascertain
        its status won't result in nonsense.
        """
        return self.prepare_completed

    @property
    def _is_restarting(self):
        return self.restart_mode

    @property
    def is_running(self):
        """True if DB server is running."""
        return (self.status is not None and
                self.status == instance.ServiceStatuses.RUNNING)

    def set_status(self, status, force=False):
        """Use conductor to update the DB app status."""

        if force or self.is_installed:
            LOG.debug("Casting set_status message to conductor "
                      "(status is '%s')." % status.description)
            context = trove_context.TroveContext()

            heartbeat = {'service_status': status.description}
            conductor_api.API(context).heartbeat(
                CONF.guest_id, heartbeat, sent=timeutils.float_utcnow())
            LOG.debug("Successfully cast set_status.")
            self.status = status
        else:
            LOG.debug("Prepare has not completed yet, skipping heartbeat.")

    def update(self):
        """Find and report status of DB on this machine.
        The database is updated and the status is also returned.
        """
        if self.is_installed and not self._is_restarting:
            LOG.debug("Determining status of DB server.")
            status = self._get_actual_db_status()
            self.set_status(status)
        else:
            LOG.info(_("DB server is not installed or is in restart mode, so "
                       "for now we'll skip determining the status of DB on "
                       "this instance."))

    def wait_for_real_status_to_change_to(self, status, max_time,
                                          update_db=False):
        """
        Waits the given time for the real status to change to the one
        specified. Does not update the publicly viewable status Unless
        "update_db" is True.
        """
        WAIT_TIME = 3
        waited_time = 0
        while waited_time < max_time:
            time.sleep(WAIT_TIME)
            waited_time += WAIT_TIME
            LOG.debug("Waiting for DB status to change to %s." % status)
            actual_status = self._get_actual_db_status()
            LOG.debug("DB status was %s after %d seconds."
                      % (actual_status, waited_time))
            if actual_status == status:
                if update_db:
                    self.set_status(actual_status)
                return True
        LOG.error(_("Timeout while waiting for database status to change."))
        return False

    def report_root(self, context, user):
        """Use conductor to update the root-enable status."""
        LOG.debug("Casting report_root message to conductor.")
        conductor_api.API(context).report_root(CONF.guest_id, user)
        LOG.debug("Successfully cast report_root.")
