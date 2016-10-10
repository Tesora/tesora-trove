# flake8: noqa

# Copyright (c) 2015 Tesora, Inc.
#
# This file is part of the Tesora DBaas Platform Enterprise Edition.
#
# Tesora DBaaS Platform is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as published by
# the Free Software Foundation, either version 3 of the License, or (at your
# option) any later version.
#
# This program is distributed in the hope that it will be useful, but
# WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
# FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Affero General Public License
# for more details.
#
# You should have received a copy of the GNU Affero General Public License
# along with this program. If not, see <http://www.gnu.org/licenses/>.
#
# According to sec. 7 of the GNU Affero General Public License, version 3, the
# terms of the AGPL are supplemented with the following terms:
#
# "Tesora", "Tesora DBaaS Platform", and the Tesora logo are trademarks
#  of Tesora, Inc.,
#
# The licensing of the Program under the AGPL does not imply a trademark
# license. Therefore any rights, title and interest in our trademarks remain
# entirely with us.
#
# However, if you propagate an unmodified version of the Program you are
# allowed to use the term "Tesora" solely to indicate that you distribute the
# Program. Furthermore you may use our trademarks where it is necessary to
# indicate the intended purpose of a product or service provided you use it in
# accordance with honest practices in industrial or commercial matters.
#
# If you want to propagate modified versions of the Program under the name
# "Tesora" or "Tesora DBaaS Platform", you may only do so if you have a written
# permission by Tesora, Inc. (to acquire a permission please contact
# Tesora, Inc at trademark@tesora.com).
#
# The interactive user interface of the software displays an attribution notice
# containing the term "Tesora" and/or the logo of Tesora.  Interactive user
# interfaces of unmodified and modified versions must display Appropriate Legal
# Notices according to sec. 5 of the GNU Affero General Public License,
# version 3, when you propagate unmodified or modified versions of  the
# Program. In accordance with sec. 7 b) of the GNU Affero General Public
# License, version 3, these Appropriate Legal Notices must retain the logo of
# Tesora or display the words "Initial Development by Tesora" if the display of
# the logo is not reasonably feasible for technical reasons.

from os import path
import socket

import cx_Oracle
from oslo_log import log as logging

from trove.common import cfg
from trove.common import exception
from trove.common.i18n import _
from trove.common import instance as rd_instance
from trove.common import pagination
from trove.common import stream_codecs
from trove.common import utils as utils
from trove.guestagent.common import configuration
from trove.guestagent.common import operating_system
from trove.guestagent.datastore.oracle_common import service
from trove.guestagent.datastore.oracle_common import sql_query
from trove.guestagent.datastore import service as ds_service
from trove.guestagent.db import models

LOG = logging.getLogger(__name__)
CONF = cfg.CONF
MANAGER = CONF.datastore_manager if CONF.datastore_manager else 'oracle'

ROOT_USER_NAME = 'sys'
ADMIN_USER_NAME = 'os_admin'
INSTANCE_OWNER = 'oracle'
INSTANCE_OWNER_GROUP = 'oinstall'
ORATAB_FILE = '/etc/oratab'


def run_sys_command(command, user=INSTANCE_OWNER, **kwargs):
    return service.run_sys_command(command, user, **kwargs)


class OracleVMAppStatus(ds_service.BaseDbStatus):

    def _get_actual_db_status(self):
        try:
            cmd = ("ps -ef | grep %s | grep %s | grep -v grep | wc -l"
                   % (INSTANCE_OWNER, 'ora_smon'))
            out, err = utils.execute_with_timeout(cmd, shell=True)
            if out != '0\n':
                # If the number of 'ora' process is not zero, it means an
                # Oracle instance is running.
                LOG.debug("Setting state to "
                          "rd_instance.ServiceStatuses.RUNNING")
                return rd_instance.ServiceStatuses.RUNNING
            else:
                LOG.debug("Setting state to "
                          "rd_instance.ServiceStatuses.SHUTDOWN")
                return rd_instance.ServiceStatuses.SHUTDOWN
        except exception.ProcessExecutionError:
            LOG.exception(_("Error getting the Oracle server status."))
            return rd_instance.ServiceStatuses.CRASHED

    def start_db_service(self, service_candidates=None,
                         timeout=CONF.state_change_wait_time,
                         enable_on_boot=True, update_db=False):
        if not service_candidates:
            service_candidates = ['dbora']
        super(OracleVMAppStatus, self).start_db_service(
            service_candidates, timeout, enable_on_boot, update_db)

    def stop_db_service(self, service_candidates=None,
                        timeout=CONF.state_change_wait_time,
                        disable_on_boot=False, update_db=False):
        if not service_candidates:
            service_candidates = ['dbora']
        super(OracleVMAppStatus, self).stop_db_service(
            service_candidates, timeout, disable_on_boot, update_db)


class OracleVMConfig(service.OracleConfig):
    tag_db_unique_name = 'db_unique_name'

    def __init__(self):
        self.key_names[self.tag_root_password] = 'sys_pwd'
        self.key_names[self.tag_db_unique_name] = 'db_unique_name'
        super(OracleVMConfig, self).__init__(CONF.get(MANAGER).conf_file)

    @property
    def db_unique_name(self):
        return self._values[self.tag_db_unique_name] or self.db_name

    @db_unique_name.setter
    def db_unique_name(self, value):
        self._set_option(self.tag_db_unique_name, value)


class OracleVMClient(service.OracleClient):

    def __init__(self, sid,
                 hostname='localhost',
                 port=None,
                 user_id=None,
                 password=None,
                 use_service=False,
                 mode=cx_Oracle.SYSDBA):
        port = port if port else CONF.get(MANAGER).listener_port
        user_id = user_id if user_id else ADMIN_USER_NAME
        password = password if password else OracleVMConfig().admin_password
        super(OracleVMClient, self).__init__(
            sid, CONF.get(MANAGER).oracle_home,
            hostname, port, user_id, password, use_service, mode)


class OracleVMCursor(service.OracleCursor, OracleVMClient):
    pass


class OracleVMPaths(object):
    """Definitions of paths for the Oracle in-VM datastore."""
    oratab_file = ORATAB_FILE
    oracle_home = CONF.get(MANAGER).oracle_home
    oracle_base = CONF.get(MANAGER).oracle_base
    fast_recovery_area = CONF.get(MANAGER).fast_recovery_area
    data_dir = CONF.get(MANAGER).mount_point
    dbs_dir = path.join(oracle_home, 'dbs')
    os_pfile = path.join(dbs_dir, 'os_pfile.ora')
    os_spfile = path.join(dbs_dir, 'os_spfile.ora')
    backup_dir = path.join(data_dir, 'backupset_files')
    admin_dir = path.join(oracle_base, 'admin')
    oranet_dir = path.join(oracle_home, 'network', 'admin')
    tns_file = path.join(oranet_dir, 'tnsnames.ora')
    lsnr_file = path.join(oranet_dir, 'listener.ora')
    overrides_file = path.join(oranet_dir, 'overrides')

    def __init__(self, db_name):
        if db_name:
            self.orapw_file = path.join(self.dbs_dir, 'orapw%s' % db_name)
            self.pfile = path.join(self.dbs_dir, 'init%s.ora' % db_name)
            self.spfile = path.join(self.dbs_dir, 'spfile%s.ora' % db_name)
            self.base_spfile = path.join(
                self.dbs_dir, 'spfile%s.ora.bak' % db_name)
            self.db_backup_dir = path.join(self.backup_dir, db_name)
            self.db_data_dir = path.join(self.data_dir, db_name)
            self.db_fast_recovery_logs_dir = path.join(
                self.fast_recovery_area, db_name.upper())
            self.db_fast_recovery_dir = path.join(
                self.fast_recovery_area, db_name)
            self.redo_logs_backup_dir = path.join(
                self.db_fast_recovery_logs_dir, 'backupset')
            self.audit_dir = path.join(self.admin_dir, db_name, 'adump')
            self.ctlfile1_dir = path.join(self.db_data_dir, 'controlfile')
            self.ctlfile2_dir = path.join(self.db_fast_recovery_dir,
                                          'controlfile')
            self.diag_dir = path.join(
                self.oracle_base, 'diag', 'rdbms', db_name.lower(), db_name)
            self.alert_log_file = path.join(self.diag_dir, 'alert', 'log.xml')
            self.standby_log_file = path.join(
                self.db_data_dir, 'standby_redo%s.log')

    def update_db_name(self, db_name):
        self.__init__(db_name)


class RmanScript(object):
    """Interface to Oracle's RMAN utility.
    Generates a script like the follow example:
        "rman TARGET / AUXILIARY user/password@host/db <<EOF
        run {
        RESTORE_DATABASE;
        }
        EXIT;
        EOF
        "
    """

    def __init__(
            self, commands=None, sid=None, role=None,
            target=True, t_user=None, t_pswd=None, t_host=None, t_db=None,
            catalog=False, c_user=None, c_pswd=None, c_host=None, c_db=None,
            auxiliary=False, a_user=None, a_pswd=None, a_host=None, a_db=None):
        self._commands = []
        self.sid = sid
        self.role = role
        self._target_dict = {}
        self._catalog_dict = {}
        self._auxiliary_dict = {}
        if commands:
            self.commands = commands
        if target or t_user or t_pswd or t_host or t_db:
            self.set_target(t_user, t_pswd, t_host, t_db)
        if catalog or c_user or c_pswd or c_host or c_db:
            self.set_catalog(c_user, c_pswd, c_host, c_db)
        if auxiliary or a_user or a_pswd or a_host or a_db:
            self.set_auxiliary(a_user, a_pswd, a_host, a_db)

    def _set_conn(self, conn_dict, user, pswd, host, db):
        if (user or pswd) and not (user and pswd):
            raise ValueError(_(
                "RMAN authentication string '%s/%s' missing username or "
                "password."
                % (user, pswd)))
        if db and not user:
            raise ValueError(_(
                "RMAN connection specified database %s but no user." % db))
        if host and not db:
            raise ValueError(_(
                "RMAN connection to host %s missing database name." % host))
        conn_dict['user'] = user
        conn_dict['pswd'] = pswd
        conn_dict['host'] = host
        conn_dict['db'] = db

    @property
    def commands(self):
        return self._commands

    def set_target(self, user=None, pswd=None, host=None, db=None):
        self._set_conn(self._target_dict, user, pswd, host, db)

    def set_catalog(self, user=None, pswd=None, host=None, db=None):
        self._set_conn(self._catalog_dict, user, pswd, host, db)

    def set_auxiliary(self, user=None, pswd=None, host=None, db=None):
        self._set_conn(self._auxiliary_dict, user, pswd, host, db)

    @commands.setter
    def commands(self, commands):
        def _add_command(cmd):
            if cmd[-1] != ';':
                cmd += ';'
            self._commands.append(cmd)
        if isinstance(commands, list):
            for command in commands:
                _add_command(command)
        else:
            _add_command(commands)

    def _conn_str(self, conn_type, conn_dict):
        user = conn_dict['user']
        pswd = conn_dict['pswd']
        result = '%(type)s %(user)s/%(pswd)s' % {
            'type': conn_type,
            'user': user if user else '',
            'pswd': pswd if pswd else ''}
        if conn_dict['db']:
            result += '@'
            if conn_dict['host']:
                result += '%s/' % conn_dict['host']
            result += conn_dict['db']
        elif not self.sid:
            raise ValueError(_(
                "RMAN %s missing both database name and SID value. At least "
                "one must be set." % conn_type))
        return result

    def full_connection_str(self):
        connection_strings = []
        if self._target_dict:
            connection_strings.append(
                self._conn_str('TARGET', self._target_dict))
        if self._catalog_dict:
            connection_strings.append(
                self._conn_str('CATALOG', self._catalog_dict))
        if self._auxiliary_dict:
            connection_strings.append(
                self._conn_str('AUXILIARY', self._auxiliary_dict))
        if not connection_strings:
            raise ValueError(_(
                "RMAN script has no connection information."))
        if self.role:
            connection_strings.append("AS %s" % self.role)
        return ' '.join(connection_strings)

    def __str__(self):
        lines = ['export ORACLE_SID=%s;' % self.sid] if self.sid else []
        lines.extend(['rman %s <<EOF' % self.full_connection_str(),
                      'run {'])
        lines.extend(self.commands)
        lines.extend(['}',
                      'EXIT;',
                      'EOF'])
        return "\"%s\"\n" % '\n'.join(lines)

    def run(self, shell=True, **kwargs):
        # return run_sys_command(str(self), shell=shell, **kwargs)
        return utils.execute_with_timeout(
            "su - %s -c %s" % (INSTANCE_OWNER, str(self)),
            run_as_root=True, root_helper='sudo',
            shell=shell, **kwargs)


class OracleVMAdmin(service.OracleAdmin):

    def __init__(self):
        super(OracleVMAdmin, self).__init__(
            OracleVMConfig, OracleVMClient, OracleVMCursor,
            ROOT_USER_NAME, CONF.get(MANAGER).cloud_user_role.upper())

    def _create_database(self, db_name):
        LOG.debug("Creating database %s." % db_name)
        sys_pwd = service.new_oracle_password()
        self.ora_config.root_password = sys_pwd
        try:
            run_sys_command(
                ("dbca -silent -createDatabase "
                 "-templateName %(template)s "
                 "-gdbName %(gdbname)s "
                 "-sid %(sid)s "
                 "-sysPassword %(pswd)s "
                 "-systemPassword %(pswd)s "
                 "-storageType FS "
                 "-characterSet %(db_charset)s "
                 "-memoryPercentage %(db_ram)s" %
                 {'gdbname': db_name, 'sid': db_name,
                  'pswd': sys_pwd,
                  'db_charset': CONF.get(MANAGER).db_charset,
                  'db_ram': CONF.get(MANAGER).db_ram,
                  'template': CONF.get(MANAGER).template}))
        except exception.ProcessExecutionError:
            LOG.exception(_(
                "There was an error creating database: %s.") % db_name)
            raise
        # Create the Trove admin user
        admin_pwd = self.ora_config.admin_password
        if not admin_pwd:
            admin_pwd = service.new_oracle_password()
            self.ora_config.admin_password = admin_pwd
        with self.cursor(db_name,
                         user_id='sys',
                         password=sys_pwd) as sys_cursor:
            sys_cursor.execute(str(sql_query.CreateTablespace(
                ADMIN_USER_NAME)))
            sys_cursor.execute(str(sql_query.CreateUser(ADMIN_USER_NAME,
                                                        admin_pwd)))
            sys_cursor.execute(str(
                sql_query.Grant(ADMIN_USER_NAME, ['SYSDBA', 'SYSOPER'])))
        LOG.debug("Successfully created database.")

    def _get_database_names(self):
        oratab_items = operating_system.read_file(
            OracleVMPaths.oratab_file,
            stream_codecs.PropertiesCodec(delimiter=':'))
        return oratab_items.keys()

    def _list_databases(self, limit=None, marker=None, include_marker=False):
        LOG.debug("Listing databases (limit of %s, marker %s, "
                  "%s marker)." %
                  (limit, marker,
                   ('including' if include_marker else 'not including')))
        db_list_page, next_marker = pagination.paginate_list(
            self._get_database_names(), limit, marker, include_marker)
        result = [models.OracleSchema(name).serialize()
                  for name in db_list_page]
        LOG.debug("Successfully listed databases. "
                  "Databases: %s (next marker %s)." %
                  (db_list_page, next_marker))
        return result, next_marker

    def _create_parameter_file(self, source=None, target=None,
                               from_memory=False, create_system=False,
                               cursor=None):
        """Create a parameter file.
        :param source:          source file path, if None then default
        :param target:          target file path, if None then default
        :param create_system:   if True, creates an SPFILE, else a PFILE
        :param cursor:          cx_Oracle connection cursor object to use,
                                    else created
        """
        q = (sql_query.CreateSPFile(source, target, from_memory)
             if create_system
             else sql_query.CreatePFile(source, target, from_memory))
        LOG.debug("Managing parameter files with '%s'" % q)
        if cursor:
            cursor.execute(str(q))
        else:
            with self.cursor(self.database_name) as cursor:
                cursor.execute(str(q))

    def create_pfile(self, source=None, target=None,
                     from_memory=False, cursor=None):
        self._create_parameter_file(source, target, from_memory, False, cursor)

    def create_spfile(self, source=None, target=None,
                      from_memory=False, cursor=None):
        self._create_parameter_file(source, target, from_memory, True, cursor)


class OracleVMApp(service.OracleApp):

    instance_owner = INSTANCE_OWNER
    instance_owner_group = INSTANCE_OWNER_GROUP
    root_user_name = ROOT_USER_NAME
    admin_user_name = ADMIN_USER_NAME
    rman_scripter = RmanScript

    def __init__(self, status, state_change_wait_time=None):
        super(OracleVMApp, self).__init__(
            status, OracleVMClient, OracleVMCursor, OracleVMAdmin,
            state_change_wait_time)
        self.paths = OracleVMPaths(self.admin.database_name)
        self.configuration_manager = None
        if operating_system.exists(self.paths.os_pfile):
            self._init_configuration_manager()

    def run_oracle_sys_command(self, *args, **kwargs):
        run_sys_command(*args, **kwargs)

    def pfile_codec(self):
        return stream_codecs.KeyValueCodec(
            delimiter='=',
            comment_marker='#',
            line_terminator='\n',
            value_quoting=True,
            value_quote_char="'",
            bool_case=stream_codecs.KeyValueCodec.BOOL_UPPER,
            big_ints=True,
            hidden_marker='_')

    def _init_configuration_manager(self):
        cm = configuration.ConfigurationManager
        conf_dir = path.join(
            self.paths.dbs_dir,
            cm.DEFAULT_STRATEGY_OVERRIDES_SUB_DIR)
        self.configuration_manager = cm(
            self.paths.os_pfile,
            self.instance_owner,
            self.instance_owner_group,
            self.pfile_codec(),
            requires_root=True,
            override_strategy=configuration.OneFileOverrideStrategy(conf_dir))

    def start_db_with_conf_changes(self, config_contents):
        LOG.debug('Inside the guest - Status is_running = (%s).'
                  % self.status.is_running)
        if not self.status.is_running:
            self.start_db()

    def start_db(self, update_db=False):
        LOG.debug("Start the Oracle database.")
        self.update_spfile()
        self.status.start_db_service()
        with self.cursor(self.admin.database_name) as cursor:
            cursor.execute(str(sql_query.Query(
                columns=['DATABASE_ROLE', 'OPEN_MODE'],
                tables=['V$DATABASE'])))
            row = cursor.fetchone()
            if row[0] == 'PHYSICAL STANDBY' and row[1] == 'READ ONLY':
                # Start up log apply if this database is supposed to be
                # a standby.
                cursor.execute(str(sql_query.AlterDatabase(
                    "RECOVER MANAGED STANDBY DATABASE USING CURRENT LOGFILE "
                    "DISCONNECT FROM SESSION")))

    def stop_db(self, do_not_start_on_reboot=False):
        LOG.debug("Stop the Oracle database.")
        self.status.stop_db_service()

    def restart(self):
        LOG.debug("Restarting Oracle server instance.")
        try:
            self.status.begin_restart()
            self.stop_db()
            self.start_db()
        finally:
            self.status.end_restart()

    def prep_pfile_management(self):
        """Generate the base PFILE from the original SPFILE,
        cleanse it of internal settings,
        create a backup spfile,
        and initialize the configuration manager to use it.
        """
        self.admin.create_pfile(target=self.paths.os_pfile, from_memory=True)
        parameters = operating_system.read_file(
            self.paths.os_pfile,
            codec=self.pfile_codec(),
            as_root=True)
        cleansed_parameters = dict()
        for k, v in parameters.items():
            if k.startswith('_'):
                continue
            if v.find('rdbms') != -1:
                continue
            cleansed_parameters[k] = v
        operating_system.write_file(
            self.paths.os_pfile,
            cleansed_parameters,
            codec=self.pfile_codec(),
            as_root=True)
        self.admin.create_spfile(target=self.paths.base_spfile,
                                 source=self.paths.os_pfile)
        self._init_configuration_manager()

    def generate_spfile(self):
        LOG.debug("Generating a new SPFILE.")
        self.admin.create_spfile(
            source=self.paths.os_pfile,
            target=self.paths.os_spfile)

    def remove_overrides(self):
        LOG.debug("Removing overrides.")
        self.configuration_manager.remove_user_override()
        self.generate_spfile()

    def update_overrides(self, overrides):
        if overrides:
            LOG.debug("Updating PFILE.")
            self.configuration_manager.apply_user_override(overrides)
            self.generate_spfile()

    def apply_overrides(self, overrides):
        self.admin.set_initialization_parameters(overrides)

    def update_spfile(self):
        """Checks if there is a new SPFILE and replaces the old.
        The database must be shutdown before running this.
        """
        if operating_system.exists(self.paths.os_spfile, as_root=True):
            LOG.debug('Found a new SPFILE.')
            operating_system.move(
                self.paths.os_spfile,
                self.paths.spfile,
                as_root=True,
                force=True
            )

    def make_read_only(self, read_only):
        if read_only:
            db_name = self.admin.database_name
            LOG.debug("Making database %s read only." % db_name)
            if self.admin.database_open_mode.startswith('READ ONLY'):
                LOG.debug("Database already read only.")
                return
            with self.cursor(db_name) as cursor:
                cursor.execute(str(sql_query.AlterDatabase(
                    'COMMIT TO SWITCHOVER TO STANDBY')))
            # The COMMIT TO SWITCHOVER TO STANDBY command has the side effect
            # of shutting down the database and closing the connection.
            # Therefore the database needs to be restarted.
            self.restart()

    def set_db_start_flag_in_oratab(self, db_start_flag='Y'):
        """Set the database start flag of all entries in the oratab file to the
        specified value.
        """
        oratab = operating_system.read_file(
            ORATAB_FILE,
            stream_codecs.PropertiesCodec(delimiter=':'),
            as_root=True)
        for key in oratab.keys():
            oratab[key][1] = db_start_flag
        operating_system.write_file(
            ORATAB_FILE, oratab,
            stream_codecs.PropertiesCodec(delimiter=':',
                                          line_terminator='\n'),
            as_root=True)
        operating_system.chown(ORATAB_FILE,
                               self.instance_owner,
                               self.instance_owner_group,
                               as_root=True)

    def create_lsnr_file(self):
        """Create the listener.ora file"""
        content = ('SID_LIST_LISTENER=(SID_LIST=(SID_DESC='
                   '(GLOBAL_DBNAME=%(db_name)s)'
                   '(ORACLE_HOME=%(ora_home)s)'
                   '(SID_NAME=%(db_name)s)))\n' %
                   {'db_name': self.admin.database_name,
                    'ora_home': self.paths.oracle_home})
        content += ('LISTENER=(DESCRIPTION_LIST=(DESCRIPTION=(ADDRESS='
                    '(PROTOCOL=TCP)(HOST=%(host)s)(PORT=%(port)s))))\n' %
                    {'host': socket.gethostname(),
                     'port': CONF.get(MANAGER).listener_port})
        content += ('ADR_BASE_LISTENER=%s\n' %
                    self.paths.oracle_base)
        content += ('SECURE_REGISTER_LISTENER = (TCP)\n')
        operating_system.write_file(self.paths.lsnr_file,
                                    content,
                                    as_root=True)
        operating_system.chown(self.paths.lsnr_file,
                               self.instance_owner,
                               self.instance_owner_group,
                               as_root=True)

    def configure_listener(self):
        self.create_lsnr_file()
        self.run_oracle_sys_command('lsnrctl reload')
