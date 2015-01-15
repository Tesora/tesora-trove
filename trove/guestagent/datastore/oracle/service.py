# Copyright 2013 OpenStack Foundation
# Copyright 2013 Rackspace Hosting
# Copyright 2013 Hewlett-Packard Development Company, L.P.
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

import ConfigParser
import os
import re
import uuid
from collections import defaultdict

import cx_Oracle
import sqlalchemy
from sqlalchemy import exc
from sqlalchemy import interfaces
from sqlalchemy.sql.expression import text

from trove.common import cfg
from trove.common import utils as utils
from trove.common import exception
from trove.common import instance as rd_instance
from trove.common.exception import PollTimeOut
from trove.guestagent.common import operating_system
from trove.guestagent.common import sql_query
from trove.guestagent.db import models
from trove.guestagent import pkg
from trove.guestagent.datastore import service
from trove.instance import models as rd_models
from trove.openstack.common import log as logging
from trove.openstack.common.gettextutils import _

ADMIN_USER_NAME = "os_admin"
LOG = logging.getLogger(__name__)
FLUSH = text(sql_query.FLUSH)
ENGINE = None
PREPARING = False
UUID = False

TMP_MYCNF = "/tmp/my.cnf.tmp"
MYSQL_BASE_DIR = "/var/lib/mysql"

CONF = cfg.CONF
MANAGER = CONF.datastore_manager if CONF.datastore_manager else 'mysql'

INCLUDE_MARKER_OPERATORS = {
    True: ">=",
    False: ">"
}

MYSQL_CONFIG = "/etc/mysql/my.cnf"
MYSQL_SERVICE_CANDIDATES = ["mysql", "mysqld", "mysql-server"]
MYSQL_BIN_CANDIDATES = ["/usr/sbin/mysqld", "/usr/libexec/mysqld"]
MYCNF_OVERRIDES = "/etc/mysql/conf.d/overrides.cnf"
MYCNF_OVERRIDES_TMP = "/tmp/overrides.cnf.tmp"
MYCNF_REPLMASTER = "/etc/mysql/conf.d/0replication.cnf"
MYCNF_REPLMASTER_TMP = "/tmp/replication.cnf.tmp"

ORACLE_CONFIG_FILE = "/etc/oracle/oracle-proxy.cnf"
ORACLE_CONFIG_FILE_TEMP = "/tmp/oracle-proxy.cnf.tmp"
ORACLE_SYS_CREDENTIALS = None
GUEST_INFO_FILE = "/etc/guest_info"

PDB_ADMIN_ID = "pdbadmin"
PDB_ADMIN_PSWD = "pdbpassword"

# Create a package impl
packager = pkg.Package()
proxy_conf = None


def clear_expired_password():
    """
    Some mysql installations generate random root password
    and save it in /root/.mysql_secret, this password is
    expired and should be changed by client that supports expired passwords.
    """
    LOG.debug("Removing expired password.")
    secret_file = "/root/.mysql_secret"
    try:
        out, err = utils.execute("cat", secret_file,
                                 run_as_root=True, root_helper="sudo")
    except exception.ProcessExecutionError:
        LOG.exception(_("/root/.mysql_secret does not exist."))
        return
    m = re.match('# The random password set for the root user at .*: (.*)',
                 out)
    if m:
        try:
            out, err = utils.execute("mysqladmin", "-p%s" % m.group(1),
                                     "password", "", run_as_root=True,
                                     root_helper="sudo")
        except exception.ProcessExecutionError:
            LOG.exception(_("Cannot change mysql password."))
            return
        utils.execute("rm", "-f", secret_file, run_as_root=True,
                      root_helper="sudo")
        LOG.debug("Expired password removed.")


def get_auth_password():
    pwd, err = utils.execute_with_timeout(
        "sudo",
        "awk",
        "/password\\t=/{print $3; exit}",
        MYSQL_CONFIG)
    if err:
        LOG.error(err)
        raise RuntimeError("Problem reading my.cnf! : %s" % err)
    return pwd.strip()


def get_engine():
    """Create the default engine with the updated admin user."""
    #TODO(rnirmal):Based on permissions issues being resolved we may revert
    #url = URL(drivername='mysql', host='localhost',
    #          query={'read_default_file': '/etc/mysql/my.cnf'})
    global ENGINE
    if ENGINE:
        return ENGINE
    pwd = get_auth_password()
    ENGINE = sqlalchemy.create_engine("mysql://%s:%s@localhost:3306" %
                                      (ADMIN_USER_NAME, pwd.strip()),
                                      pool_recycle=7200,
                                      echo=CONF.sql_query_logging,
                                      listeners=[KeepAliveConnection()])
    return ENGINE


def load_mysqld_options():
    #find mysqld bin
    for bin in MYSQL_BIN_CANDIDATES:
        if os.path.isfile(bin):
            mysqld_bin = bin
            break
    else:
        return {}
    try:
        out, err = utils.execute(mysqld_bin, "--print-defaults",
                                 run_as_root=True, root_helper="sudo")
        arglist = re.split("\n", out)[1].split()
        args = defaultdict(list)
        for item in arglist:
            if "=" in item:
                key, value = item.split("=", 1)
                args[key.lstrip("--")].append(value)
            else:
                args[item.lstrip("--")].append(None)
        return args
    except exception.ProcessExecutionError:
        return {}


class OracleAppStatus(service.BaseDbStatus):
    def __init__(self):
        if self._instance is not None:
            raise RuntimeError("Cannot instantiate twice.")
        self.status = rd_models.InstanceServiceStatus(
            instance_id=CONF.guest_id,
            status=rd_instance.ServiceStatuses.NEW)
        self.restart_mode = False

    @property
    def is_installed(self):
        """
        True if DB app should be installed and attempts to ascertain
        its status won't result in nonsense.
        """
        # (Simon Chang) TO-DO: Fix this
        return True
#         return (self.status is not None and
#                 self.status != rd_instance.ServiceStatuses.NEW and
#                 self.status != rd_instance.ServiceStatuses.BUILDING and
#                 self.status != rd_instance.ServiceStatuses.BUILD_PENDING and
#                 self.status != rd_instance.ServiceStatuses.FAILED)

    @property
    def _is_restarting(self):
        # (Simon Chang) TO-DO: Fix this
        return False
#        return self.restart_mode

    @property
    def is_running(self):
        """True if DB server is running."""
        # (Simon Chang) TO-DO: Fix this
        return True
#        return (self.status is not None and
#                self.status == rd_instance.ServiceStatuses.RUNNING)

    @classmethod
    def get(cls):
        if not cls._instance:
            cls._instance = OracleAppStatus()
        return cls._instance

    def _get_actual_db_status(self):
        try:
            LOG.info(_("Oracle Status is RUNNING."))
            return rd_instance.ServiceStatuses.RUNNING
        except exception.ProcessExecutionError:
            LOG.exception(_("Failed to get database status."))


class LocalSqlClient(object):
    """A sqlalchemy wrapper to manage transactions."""

    def __init__(self, engine, use_flush=True):
        self.engine = engine
        self.use_flush = use_flush

    def __enter__(self):
        self.conn = self.engine.connect()
        self.trans = self.conn.begin()
        return self.conn

    def __exit__(self, type, value, traceback):
        if self.trans:
            if type is not None:  # An error occurred
                self.trans.rollback()
            else:
                if self.use_flush:
                    self.conn.execute(FLUSH)
                self.trans.commit()
        self.conn.close()

    def execute(self, t, **kwargs):
        try:
            return self.conn.execute(t, kwargs)
        except Exception:
            self.trans.rollback()
            self.trans = None
            raise


class LocalOracleClient(object):
    """A wrapper to manage Oracle connection."""

    def __init__(self, sid, service=False):
        self.sid = sid
        self.service = service

    def __enter__(self):
        if self.service:
            ora_dsn = cx_Oracle.makedsn(proxy_conf['oracle_host'],
                                        proxy_conf['oracle_port'],
                                        service_name=self.sid)
        else:
            ora_dsn = cx_Oracle.makedsn(proxy_conf['oracle_host'],
                                        proxy_conf['oracle_port'],
                                        self.sid)

        self.conn = cx_Oracle.connect(ORACLE_SYS_CREDENTIALS,
                                      dsn=ora_dsn,
                                      mode=cx_Oracle.SYSDBA)
        return self.conn.cursor()

    def __exit__(self, type, value, traceback):
        self.conn.close()


class OracleAdmin(object):
    """Handles administrative tasks on the Oracle database."""

    def _associate_dbs(self, user):
        """Internal. Given a MySQLUser, populate its databases attribute."""
        LOG.debug("Associating dbs to user %s at %s." %
                  (user.name, user.host))
        with LocalSqlClient(get_engine()) as client:
            q = sql_query.Query()
            q.columns = ["grantee", "table_schema"]
            q.tables = ["information_schema.SCHEMA_PRIVILEGES"]
            q.group = ["grantee", "table_schema"]
            q.where = ["privilege_type != 'USAGE'"]
            t = text(str(q))
            db_result = client.execute(t)
            for db in db_result:
                LOG.debug("\t db: %s." % db)
                if db['grantee'] == "'%s'@'%s'" % (user.name, user.host):
                    mysql_db = models.MySQLDatabase()
                    mysql_db.name = db['table_schema']
                    user.databases.append(mysql_db.serialize())

    def change_passwords(self, users):
        """Change the passwords of one or more existing users."""
        LOG.debug("Changing the password of some users.")
        with LocalSqlClient(get_engine()) as client:
            for item in users:
                LOG.debug("Changing password for user %s." % item)
                user_dict = {'_name': item['name'],
                             '_host': item['host'],
                             '_password': item['password']}
                user = models.MySQLUser()
                user.deserialize(user_dict)
                LOG.debug("\tDeserialized: %s." % user.__dict__)
                uu = sql_query.UpdateUser(user.name, host=user.host,
                                          clear=user.password)
                t = text(str(uu))
                client.execute(t)

    def update_attributes(self, username, hostname, user_attrs):
        """Change the attributes of an existing user."""
        LOG.debug("Changing user attributes for user %s." % username)
        user = self._get_user(username, hostname)
        db_access = set()
        grantee = set()
        with LocalSqlClient(get_engine()) as client:
            q = sql_query.Query()
            q.columns = ["grantee", "table_schema"]
            q.tables = ["information_schema.SCHEMA_PRIVILEGES"]
            q.group = ["grantee", "table_schema"]
            q.where = ["privilege_type != 'USAGE'"]
            t = text(str(q))
            db_result = client.execute(t)
            for db in db_result:
                grantee.add(db['grantee'])
                if db['grantee'] == "'%s'@'%s'" % (user.name, user.host):
                    db_name = db['table_schema']
                    db_access.add(db_name)
        with LocalSqlClient(get_engine()) as client:
            uu = sql_query.UpdateUser(user.name, host=user.host,
                                      clear=user_attrs.get('password'),
                                      new_user=user_attrs.get('name'),
                                      new_host=user_attrs.get('host'))
            t = text(str(uu))
            client.execute(t)
            uname = user_attrs.get('name') or username
            host = user_attrs.get('host') or hostname
            find_user = "'%s'@'%s'" % (uname, host)
            if find_user not in grantee:
                self.grant_access(uname, host, db_access)

    def create_database(self, databases):
        """Create the list of specified databases."""
        LOG.debug("Creating pluggable database")
        with LocalOracleClient(proxy_conf['cdb_name']) as client:
            client.execute("CREATE PLUGGABLE DATABASE %(pdb_name)s "
                           "ADMIN USER %(admin_id)s "
                           "IDENTIFIED BY %(admin_pswd)s" %
                           {'pdb_name': proxy_conf['pdb_name'],
                            'admin_id': PDB_ADMIN_ID,
                            'admin_pswd': PDB_ADMIN_PSWD})
            client.execute("ALTER PLUGGABLE DATABASE %s OPEN" %
                           proxy_conf['pdb_name'])
        LOG.debug("Finished creating pluggable database")

    def create_user(self, users):
        """Create users and grant them privileges for the
           specified databases.
        """
        LOG.debug("Creating database user")
        user_id = users[0]['_name']
        password = users[0]['_password']
        with LocalOracleClient(proxy_conf['pdb_name'], service=True) as client:
            client.execute('CREATE USER %(user_id)s IDENTIFIED BY %(password)s'
                           % {'user_id': user_id, 'password': password})
            client.execute('GRANT CREATE SESSION to %s' % user_id)
            client.execute('GRANT CREATE TABLE to %s' % user_id)
            client.execute('GRANT UNLIMITED TABLESPACE to %s' % user_id)
            client.execute('GRANT SELECT ANY TABLE to %s' % user_id)
            client.execute('GRANT UPDATE ANY TABLE to %s' % user_id)
            client.execute('GRANT INSERT ANY TABLE to %s' % user_id)
            client.execute('GRANT DROP ANY TABLE to %s' % user_id)
        LOG.debug("Finished creating database user")

    def delete_database(self, database):
        """Delete the specified database."""
        LOG.debug("Deleting pluggable database %s" % database)
        with LocalOracleClient(proxy_conf['cdb_name']) as client:
            client.execute("ALTER PLUGGABLE DATABASE %s CLOSE IMMEDIATE" %
                           proxy_conf['pdb_name'])
            client.execute("DROP PLUGGABLE DATABASE %s INCLUDING DATAFILES" %
                           proxy_conf['pdb_name'])
        LOG.debug("Finished deleting pluggable database")
        return True

    def delete_user(self, user):
        """Delete the specified user."""
        oracle_user = models.OracleUser()
        oracle_user.deserialize(user)
        self.delete_user_by_name(oracle_user.name, oracle_user.host)

    def delete_user_by_name(self, name, host='%'):
        LOG.debug("Deleting user %s" % name)
        with LocalOracleClient(proxy_conf['pdb_name'], service=True) as client:
            client.execute("DROP USER %s" % name)
        LOG.debug("Deleted user %s" % name)

    def get_user(self, username, hostname):
        user = self._get_user(username, hostname)
        if not user:
            return None
        return user.serialize()

    def _get_user(self, username, hostname):
        """Return a single user matching the criteria."""
        with LocalOracleClient(proxy_conf['pdb_name'], service=True) as client:
            client.execute("SELECT USERNAME FROM ALL_USERS "
                           "WHERE USERNAME = '%s'" % username.upper())
            users = client.fetchall()
        if client.rowcount != 1:
            return None
        user = models.OracleUser()
        try:
            user.name = users[0][0]  # Could possibly throw a BadRequest here.
        except exception.ValueError as ve:
            LOG.exception(_("Error Getting user information"))
            raise exception.BadRequest(_("Username %(user)s is not valid"
                                         ": %(reason)s") %
                                       {'user': username, 'reason': ve.message}
                                       )
        return user

    def grant_access(self, username, hostname, databases):
        """Grant a user permission to use a given database."""
        user = self._get_user(username, hostname)
        mydb = models.ValidatedMySQLDatabase()
        with LocalSqlClient(get_engine()) as client:
            for database in databases:
                    try:
                        mydb.name = database
                    except ValueError:
                        LOG.exception(_("Error granting access"))
                        raise exception.BadRequest(_(
                            "Grant access to %s is not allowed") % database)

                    g = sql_query.Grant(permissions='ALL', database=mydb.name,
                                        user=user.name, host=user.host,
                                        hashed=user.password)
                    t = text(str(g))
                    client.execute(t)

    def is_root_enabled(self):
        """Return True if root access is enabled; False otherwise."""
        return MySqlRootAccess.is_root_enabled()

    def enable_root(self, root_password=None):
        """Enable the root user global access and/or
           reset the root password.
        """
        return MySqlRootAccess.enable_root(root_password)

    def list_databases(self, limit=None, marker=None, include_marker=False):
        """List databases the user created on this mysql instance."""
        LOG.debug("---Listing Databases---")
        databases = []
        return databases, None

    def list_users(self, limit=None, marker=None, include_marker=False):
        """List users that have access to the database."""
        LOG.debug("---Listing Users---")
        users = []
        with LocalOracleClient(proxy_conf['pdb_name'], service=True) as client:
            client.execute('SELECT USERNAME FROM ALL_USERS')
            for row in client:
                oracle_user = models.OracleUser()
                oracle_user.name = row[0]
                # mysql_user.host = row['Host']
                # self._associate_dbs(mysql_user)
                users.append(oracle_user.serialize())
        return users, None

    def revoke_access(self, username, hostname, database):
        """Revoke a user's permission to use a given database."""
        user = self._get_user(username, hostname)
        with LocalSqlClient(get_engine()) as client:
            r = sql_query.Revoke(database=database,
                                 user=user.name,
                                 host=user.host)
            t = text(str(r))
            client.execute(t)

    def list_access(self, username, hostname):
        """Show all the databases to which the user has more than
           USAGE granted.
        """
        user = self._get_user(username, hostname)
        return user.databases


class KeepAliveConnection(interfaces.PoolListener):
    """
    A connection pool listener that ensures live connections are returned
    from the connection pool at checkout. This alleviates the problem of
    MySQL connections timing out.
    """

    def checkout(self, dbapi_con, con_record, con_proxy):
        """Event triggered when a connection is checked out from the pool."""
        try:
            try:
                dbapi_con.ping(False)
            except TypeError:
                dbapi_con.ping()
        except dbapi_con.OperationalError as ex:
            if ex.args[0] in (2006, 2013, 2014, 2045, 2055):
                raise exc.DisconnectionError()
            else:
                raise


class OracleApp(object):
    """Prepares DBaaS on a Guest container."""

    TIME_OUT = 1000

    def __init__(self, status):
        """By default login with root no password for initial setup."""
        self.state_change_wait_time = CONF.state_change_wait_time
        self.status = status

    def _create_admin_user(self, client, password):
        """
        Create a os_admin user with a random password
        with all privileges similar to the root user.
        """
        localhost = "localhost"
        g = sql_query.Grant(permissions='ALL', user=ADMIN_USER_NAME,
                            host=localhost, grant_option=True, clear=password)
        t = text(str(g))
        client.execute(t)

    @staticmethod
    def _generate_root_password(client):
        """Generate and set a random root password and forget about it."""
        localhost = "localhost"
        uu = sql_query.UpdateUser("root", host=localhost,
                                  clear=utils.generate_random_password())
        t = text(str(uu))
        client.execute(t)

    def install_if_needed(self, packages):
        """Prepare the guest machine with a secure
           mysql server installation.
        """
        LOG.info(_("Preparing Guest as Oracle Server."))
        if not packager.pkg_is_installed(packages):
            LOG.debug("Installing Oracle server.")
            self._clear_mysql_config()
            # set blank password on pkg configuration stage
            pkg_opts = {'root_password': '',
                        'root_password_again': ''}
            packager.pkg_install(packages, pkg_opts, self.TIME_OUT)
            self._create_mysql_confd_dir()
            LOG.info(_("Finished installing MySQL server."))
        self.start_mysql()

    def complete_install_or_restart(self):
        self.status.end_install_or_restart()

    def configure(self, config_contents):
        LOG.info(_("Writing my.cnf templates."))
        try:
            with open(ORACLE_CONFIG_FILE_TEMP, 'w') as t:
                t.write(config_contents)
            utils.execute_with_timeout("sudo", "mkdir", "-p",
                                       os.path.dirname(ORACLE_CONFIG_FILE))
            utils.execute_with_timeout("sudo", "mv", ORACLE_CONFIG_FILE_TEMP,
                                       ORACLE_CONFIG_FILE)
            config = ConfigParser.RawConfigParser()
            config.read(GUEST_INFO_FILE)
            global proxy_conf
            global ORACLE_SYS_CREDENTIALS
            proxy_conf = {}
            proxy_conf['pdb_name'] = config.get('DEFAULT', 'guest_name')

            config.read(ORACLE_CONFIG_FILE)
            proxy_conf['oracle_host'] = config.get('ORACLE', 'oracle_host')
            proxy_conf['oracle_port'] = config.getint('ORACLE', 'oracle_port')
            proxy_conf['sys_usr'] = config.get('ORACLE', 'sys_usr')
            proxy_conf['sys_pswd'] = config.get('ORACLE', 'sys_pswd')
            proxy_conf['cdb_name'] = config.get('ORACLE', 'cdb_name')
            ORACLE_SYS_CREDENTIALS = '%s/%s' % (proxy_conf['sys_usr'],
                                                proxy_conf['sys_pswd'])
        except Exception:
            os.unlink(ORACLE_CONFIG_FILE_TEMP)
            raise

    def secure(self, config_contents, overrides):
        LOG.info(_("Generating admin password."))
        admin_password = utils.generate_random_password()
        clear_expired_password()
        engine = sqlalchemy.create_engine("mysql://root:@localhost:3306",
                                          echo=True)
        with LocalSqlClient(engine) as client:
            self._remove_anonymous_user(client)
            self._create_admin_user(client, admin_password)

        self.stop_db()
        self._write_mycnf(admin_password, config_contents, overrides)
        self.start_mysql()

        LOG.debug("MySQL secure complete.")

    def secure_root(self, secure_remote_root=True):
        with LocalSqlClient(get_engine()) as client:
            LOG.info(_("Preserving root access from restore."))
            self._generate_root_password(client)
            if secure_remote_root:
                self._remove_remote_root_access(client)

    def _clear_mysql_config(self):
        """Clear old configs, which can be incompatible with new version."""
        LOG.debug("Clearing old MySQL config.")
        random_uuid = str(uuid.uuid4())
        configs = ["/etc/my.cnf", "/etc/mysql/conf.d", "/etc/mysql/my.cnf"]
        for config in configs:
            command = "mv %s %s_%s" % (config, config, random_uuid)
            try:
                utils.execute_with_timeout(command, shell=True,
                                           root_helper="sudo")
                LOG.debug("%s saved to %s_%s." %
                          (config, config, random_uuid))
            except exception.ProcessExecutionError:
                pass

    def _create_mysql_confd_dir(self):
        conf_dir = "/etc/mysql/conf.d"
        LOG.debug("Creating %s." % conf_dir)
        command = "sudo mkdir -p %s" % conf_dir
        utils.execute_with_timeout(command, shell=True)

    def _enable_mysql_on_boot(self):
        LOG.debug("Enabling Oracle on boot.")

    def _disable_mysql_on_boot(self):
        try:
            mysql_service = operating_system.service_discovery(
                MYSQL_SERVICE_CANDIDATES)
            utils.execute_with_timeout(mysql_service['cmd_disable'],
                                       shell=True)
        except KeyError:
            LOG.exception(_("Error disabling MySQL start on boot."))
            raise RuntimeError("Service is not discovered.")

    def stop_db(self, update_db=False, do_not_start_on_reboot=False):
        LOG.info(_("Stopping MySQL."))
        if do_not_start_on_reboot:
            self._disable_mysql_on_boot()
        try:
            mysql_service = operating_system.service_discovery(
                MYSQL_SERVICE_CANDIDATES)
            utils.execute_with_timeout(mysql_service['cmd_stop'], shell=True)
        except KeyError:
            LOG.exception(_("Error stopping MySQL."))
            raise RuntimeError("Service is not discovered.")
        if not self.status.wait_for_real_status_to_change_to(
                rd_instance.ServiceStatuses.SHUTDOWN,
                self.state_change_wait_time, update_db):
            LOG.error(_("Could not stop MySQL."))
            self.status.end_install_or_restart()
            raise RuntimeError("Could not stop MySQL!")

    def _remove_anonymous_user(self, client):
        t = text(sql_query.REMOVE_ANON)
        client.execute(t)

    def _remove_remote_root_access(self, client):
        t = text(sql_query.REMOVE_ROOT)
        client.execute(t)

    def restart(self):
        try:
            self.status.begin_restart()
            self.stop_db()
            self.start_mysql()
        finally:
            self.status.end_install_or_restart()

    def update_overrides(self, overrides_file, remove=False):
        """
        This function will either update or remove the MySQL overrides.cnf file
        If remove is set to True the function will remove the overrides file.

        :param overrides:
        :param remove:
        :return:
        """

        if overrides_file:
            LOG.debug("Writing new overrides.cnf config file.")
            self._write_config_overrides(overrides_file)
        if remove:
            LOG.debug("Removing overrides.cnf config file.")
            self._remove_overrides()

    def apply_overrides(self, overrides):
        LOG.debug("Applying overrides to MySQL.")
        with LocalSqlClient(get_engine()) as client:
            LOG.debug("Updating override values in running MySQL.")
            for k, v in overrides.iteritems():
                q = sql_query.SetServerVariable(key=k, value=v)
                t = text(str(q))
                try:
                    client.execute(t)
                except exc.OperationalError:
                    output = {'key': k, 'value': v}
                    LOG.exception(_("Unable to set %(key)s with value "
                                    "%(value)s.") % output)

    def _write_temp_mycnf_with_admin_account(self, original_file_path,
                                             temp_file_path, password):
        mycnf_file = open(original_file_path, 'r')
        tmp_file = open(temp_file_path, 'w')
        for line in mycnf_file:
            tmp_file.write(line)
            if "[client]" in line:
                tmp_file.write("user\t\t= %s\n" % ADMIN_USER_NAME)
                tmp_file.write("password\t= %s\n" % password)
        mycnf_file.close()
        tmp_file.close()

    def wipe_ib_logfiles(self):
        """Destroys the iblogfiles.

        If for some reason the selected log size in the conf changes from the
        current size of the files MySQL will fail to start, so we delete the
        files to be safe.
        """
        LOG.info(_("Wiping ib_logfiles."))
        for index in range(2):
            try:
                # On restarts, sometimes these are wiped. So it can be a race
                # to have MySQL start up before it's restarted and these have
                # to be deleted. That's why its ok if they aren't found and
                # that is why we use the "-f" option to "rm".
                (utils.
                 execute_with_timeout("sudo", "rm", "-f", "%s/ib_logfile%d"
                                                    % (MYSQL_BASE_DIR, index)))
            except exception.ProcessExecutionError:
                LOG.exception("Could not delete logfile.")
                raise

    def _write_mycnf(self, admin_password, config_contents, overrides=None):
        """
        Install the set of mysql my.cnf templates.
        Update the os_admin user and password to the my.cnf
        file for direct login from localhost.
        """
        LOG.info(_("Writing my.cnf templates."))
        if admin_password is None:
            admin_password = get_auth_password()

        try:
            with open(TMP_MYCNF, 'w') as t:
                t.write(config_contents)

            utils.execute_with_timeout("sudo", "mv", TMP_MYCNF,
                                       MYSQL_CONFIG)

            self._write_temp_mycnf_with_admin_account(MYSQL_CONFIG,
                                                      TMP_MYCNF,
                                                      admin_password)

            utils.execute_with_timeout("sudo", "mv", TMP_MYCNF,
                                       MYSQL_CONFIG)
        except Exception:
            os.unlink(TMP_MYCNF)
            raise

        self.wipe_ib_logfiles()

        # write configuration file overrides
        if overrides:
            self._write_config_overrides(overrides)

    def _write_config_overrides(self, overrideValues):
        LOG.info(_("Writing new temp overrides.cnf file."))

        with open(MYCNF_OVERRIDES_TMP, 'w') as overrides:
            overrides.write(overrideValues)
        LOG.info(_("Moving overrides.cnf into correct location."))
        utils.execute_with_timeout("sudo", "mv", MYCNF_OVERRIDES_TMP,
                                   MYCNF_OVERRIDES)

        LOG.info(_("Setting permissions on overrides.cnf."))
        utils.execute_with_timeout("sudo", "chmod", "0644",
                                   MYCNF_OVERRIDES)

    def _remove_overrides(self):
        LOG.info(_("Removing overrides configuration file."))
        if os.path.exists(MYCNF_OVERRIDES):
            utils.execute_with_timeout("sudo", "rm", MYCNF_OVERRIDES)

    def write_replication_overrides(self, overrideValues):
        LOG.info(_("Writing replication.cnf file."))

        with open(MYCNF_REPLMASTER_TMP, 'w') as overrides:
            overrides.write(overrideValues)
        LOG.debug("Moving temp replication.cnf into correct location.")
        utils.execute_with_timeout("sudo", "mv", MYCNF_REPLMASTER_TMP,
                                   MYCNF_REPLMASTER)

        LOG.debug("Setting permissions on replication.cnf.")
        utils.execute_with_timeout("sudo", "chmod", "0644",
                                   MYCNF_REPLMASTER)

    def remove_replication_overrides(self):
        LOG.info(_("Removing replication configuration file."))
        if os.path.exists(MYCNF_REPLMASTER):
            utils.execute_with_timeout("sudo", "rm", MYCNF_REPLMASTER)

    def grant_replication_privilege(self, replication_user):
        LOG.info(_("Granting Replication Slave privilege."))

        with LocalSqlClient(get_engine()) as client:
            g = sql_query.Grant(permissions=['REPLICATION SLAVE'],
                                user=replication_user['name'],
                                clear=replication_user['password'])

            t = text(str(g))
            client.execute(t)

    def revoke_replication_privilege(self):
        LOG.info(_("Revoking Replication Slave privilege."))

        with LocalSqlClient(get_engine()) as client:
            results = client.execute('SHOW SLAVE STATUS').fetchall()
            slave_status_info = results[0]

            r = sql_query.Revoke(permissions=['REPLICATION SLAVE'],
                                 user=slave_status_info['master_user'])

            t = text(str(r))
            client.execute(t)

    def get_port(self):
        with LocalSqlClient(get_engine()) as client:
            result = client.execute('SELECT @@port').first()
            return result[0]

    def get_binlog_position(self):
        with LocalSqlClient(get_engine()) as client:
            result = client.execute('SHOW MASTER STATUS').first()
            binlog_position = {
                'log_file': result['File'],
                'position': result['Position']
            }
            return binlog_position

    def change_master_for_binlog(self, host, port, logging_config):
        LOG.info(_("Configuring replication from %s.") % host)

        replication_user = logging_config['replication_user']
        change_master_cmd = ("CHANGE MASTER TO MASTER_HOST='%(host)s', "
                             "MASTER_PORT=%(port)s, "
                             "MASTER_USER='%(user)s', "
                             "MASTER_PASSWORD='%(password)s', "
                             "MASTER_LOG_FILE='%(log_file)s', "
                             "MASTER_LOG_POS=%(log_pos)s" %
                             {
                                 'host': host,
                                 'port': port,
                                 'user': replication_user['name'],
                                 'password': replication_user['password'],
                                 'log_file': logging_config['log_file'],
                                 'log_pos': logging_config['log_position']
                             })

        with LocalSqlClient(get_engine()) as client:
            client.execute(change_master_cmd)

    def start_slave(self):
        LOG.info(_("Starting slave replication."))
        with LocalSqlClient(get_engine()) as client:
            client.execute('START SLAVE')
            self._wait_for_slave_status("ON", client, 60)

    def stop_slave(self):
        replication_user = None
        LOG.info(_("Stopping slave replication."))
        with LocalSqlClient(get_engine()) as client:
            result = client.execute('SHOW SLAVE STATUS')
            replication_user = result.first()['Master_User']
            client.execute('STOP SLAVE')
            client.execute('RESET SLAVE ALL')
            self._wait_for_slave_status("OFF", client, 30)
            client.execute('DROP USER ' + replication_user)
        return {
            'replication_user': replication_user
        }

    def _wait_for_slave_status(self, status, client, max_time):

        def verify_slave_status():
            actual_status = client.execute(
                "SHOW GLOBAL STATUS like 'slave_running'").first()[1]
            return actual_status.upper() == status.upper()

        LOG.debug("Waiting for SLAVE_RUNNING to change to %s.", status)
        try:
            utils.poll_until(verify_slave_status, sleep_time=3,
                             time_out=max_time)
            LOG.info(_("Replication is now %s.") % status.lower())
        except PollTimeOut:
            raise RuntimeError(
                _("Replication is not %(status)s after %(max)d seconds.") % {
                    'status': status.lower(), 'max': max_time})

    def start_mysql(self, update_db=False):
        LOG.info(_("Starting MySQL."))
        # This is the site of all the trouble in the restart tests.
        # Essentially what happens is that mysql start fails, but does not
        # die. It is then impossible to kill the original, so

        self._enable_mysql_on_boot()

        try:
            mysql_service = operating_system.service_discovery(
                MYSQL_SERVICE_CANDIDATES)
            utils.execute_with_timeout(mysql_service['cmd_start'], shell=True)
        except KeyError:
            raise RuntimeError("Service is not discovered.")
        except exception.ProcessExecutionError:
            # it seems mysql (percona, at least) might come back with [Fail]
            # but actually come up ok. we're looking into the timing issue on
            # parallel, but for now, we'd like to give it one more chance to
            # come up. so regardless of the execute_with_timeout() response,
            # we'll assume mysql comes up and check it's status for a while.
            pass
        if not self.status.wait_for_real_status_to_change_to(
                rd_instance.ServiceStatuses.RUNNING,
                self.state_change_wait_time, update_db):
            LOG.error(_("Start up of MySQL failed."))
            # If it won't start, but won't die either, kill it by hand so we
            # don't let a rouge process wander around.
            try:
                utils.execute_with_timeout("sudo", "pkill", "-9", "mysql")
            except exception.ProcessExecutionError:
                LOG.exception(_("Error killing stalled MySQL start command."))
                # There's nothing more we can do...
            self.status.end_install_or_restart()
            raise RuntimeError("Could not start MySQL!")

    def start_db_with_conf_changes(self, config_contents):
        LOG.info(_("Starting MySQL with conf changes."))
        LOG.debug("Inside the guest - Status is_running = (%s)."
                  % self.status.is_running)
        if self.status.is_running:
            LOG.error(_("Cannot execute start_db_with_conf_changes because "
                        "MySQL state == %s.") % self.status)
            raise RuntimeError("MySQL not stopped.")
        LOG.info(_("Resetting configuration."))
        self._write_mycnf(None, config_contents)
        self.start_mysql(True)

    def reset_configuration(self, configuration):
        config_contents = configuration['config_contents']
        LOG.info(_("Resetting configuration."))
        self._write_mycnf(None, config_contents)


class MySqlRootAccess(object):
    @classmethod
    def is_root_enabled(cls):
        """Return True if root access is enabled; False otherwise."""
        with LocalSqlClient(get_engine()) as client:
            t = text(sql_query.ROOT_ENABLED)
            result = client.execute(t)
            LOG.debug("Found %s with remote root access." % result.rowcount)
            return result.rowcount != 0

    @classmethod
    def enable_root(cls, root_password=None):
        """Enable the root user global access and/or
           reset the root password.
        """
        user = models.RootUser()
        user.name = "root"
        user.host = "%"
        user.password = root_password or utils.generate_random_password()
        with LocalSqlClient(get_engine()) as client:
            print(client)
            try:
                cu = sql_query.CreateUser(user.name, host=user.host)
                t = text(str(cu))
                client.execute(t, **cu.keyArgs)
            except exc.OperationalError as err:
                # Ignore, user is already created, just reset the password
                # TODO(rnirmal): More fine grained error checking later on
                LOG.debug(err)
        with LocalSqlClient(get_engine()) as client:
            print(client)
            uu = sql_query.UpdateUser(user.name, host=user.host,
                                      clear=user.password)
            t = text(str(uu))
            client.execute(t)

            LOG.debug("CONF.root_grant: %s CONF.root_grant_option: %s." %
                      (CONF.root_grant, CONF.root_grant_option))

            g = sql_query.Grant(permissions=CONF.root_grant,
                                user=user.name,
                                host=user.host,
                                grant_option=CONF.root_grant_option,
                                clear=user.password)

            t = text(str(g))
            client.execute(t)
            return user.serialize()
