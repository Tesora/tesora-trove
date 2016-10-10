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
import re
import socket

import cx_Oracle
from oslo_log import log as logging

from trove.common import cfg
from trove.common import exception
from trove.common.i18n import _
from trove.common import instance as rd_instance
from trove.common.strategies.cluster.oracle_rac import utils as rac_utils
from trove.common import utils
from trove.guestagent.common import operating_system
from trove.guestagent.datastore.oracle_common import service
from trove.guestagent.datastore.oracle_common import sql_query
from trove.guestagent.datastore.service import BaseDbStatus

LOG = logging.getLogger(__name__)
CONF = cfg.CONF
MANAGER = CONF.datastore_manager if CONF.datastore_manager else 'oracle_rac'

INSTANCE_OWNER = 'oracle'
INSTANCE_OWNER_GROUP = 'oinstall'
ROOT_SOFTWARE_DIR = '/u01'
SHARED_DISK_PATHS = {'votedisk': path.join(ROOT_SOFTWARE_DIR, 'votedisk'),
                     'registry': path.join(ROOT_SOFTWARE_DIR, 'registry'),
                     'database': path.join(ROOT_SOFTWARE_DIR, 'oradata')}
GRID_CONF_RSP = 'grid-configure.rsp'
ORACLE_INST_RSP = 'oracle-install.rsp'
GRID_HOME = path.join(ROOT_SOFTWARE_DIR, 'app', 'grid_home', 'grid')
ORACLE_BASE = path.join(ROOT_SOFTWARE_DIR, 'app', 'oracle')
ORACLE_INST = path.join(ORACLE_BASE, 'oracle-software')
ORACLE_HOME = path.join(ORACLE_BASE, 'product', 'dbaas')
CONF_FILE = path.join('/etc', 'oracle', 'oracle_rac.cnf')
SCAN_NAME = 'scan'
ROOT_USER_NAME = 'sys'
ADMIN_USER_NAME = 'os_admin'


def run_sys_command(command, user=INSTANCE_OWNER,
                    timeout=CONF.get(MANAGER).configuration_timeout,
                    **kwargs):
    return service.run_sys_command(command, user, timeout, **kwargs)


class OracleRACAppStatus(BaseDbStatus):

    def _get_actual_db_status(self):
        return rd_instance.ServiceStatuses.RUNNING


class OracleRACConfig(service.OracleConfig):
    tag_cluster_sid = 'cluster_sid'

    def __init__(self):
        self.key_names[self.tag_cluster_sid] = 'cluster_sid'
        super(OracleRACConfig, self).__init__(CONF_FILE)

    @property
    def cluster_sid(self):
        return self._values[self.tag_cluster_sid]

    @cluster_sid.setter
    def cluster_sid(self, value):
        self._set_option(self.tag_cluster_sid, value)


class OracleRACClient(service.OracleClient):

    def __init__(self, sid,
                 hostname=SCAN_NAME,
                 port=1521,
                 user_id=None,
                 password=None,
                 use_service=False,
                 mode=cx_Oracle.SYSDBA):
        user_id = user_id if user_id else ADMIN_USER_NAME
        password = password if password else OracleRACConfig().admin_password
        super(OracleRACClient, self).__init__(
            sid, ORACLE_HOME,
            hostname, port, user_id, password, use_service, mode)


class OracleRACCursor(service.OracleCursor, OracleRACClient):
    pass


class OracleRACAdmin(service.OracleAdmin):

    def __init__(self):
        super(OracleRACAdmin, self).__init__(
            OracleRACConfig, OracleRACClient, OracleRACCursor,
            ROOT_USER_NAME, CONF.get(MANAGER).cloud_user_role.upper())

    def store_cluster_info(self, dbname, sys_password, admin_password):
        self.ora_config.db_name = dbname
        self.ora_config.root_password = sys_password
        self.ora_config.admin_password = admin_password

    def create_rac_database(self, nodes_string):
        db_name = self.ora_config.db_name
        LOG.debug("Creating database %s." % db_name)
        sys_pwd = self.ora_config.root_password
        try:
            run_sys_command(
                ("{dbca} -silent -createDatabase "
                 "-nodelist {nodes} "
                 "-templateName {template} "
                 "-gdbName {name} -sid {name} "
                 "-SysPassword '{sys_pwd}' -SystemPassword '{sys_pwd}' "
                 "-emConfiguration NONE "
                 "-characterSet {db_charset} "
                 "-storageType FS -datafileDestination {dest} "
                 "-memoryPercentage {db_ram}".format(
                    dbca=path.join(ORACLE_HOME, 'bin', 'dbca'),
                    nodes=nodes_string,
                    name=db_name,
                    sys_pwd=self.ora_config.root_password,
                    dest=SHARED_DISK_PATHS['database'],
                    db_charset=CONF.get(MANAGER).db_charset,
                    db_ram=CONF.get(MANAGER).db_ram,
                    template=CONF.get(MANAGER).template)))
        except exception.ProcessExecutionError:
            LOG.exception(_(
                "There was an error creating database: %s.") % db_name)
            raise
        # sid = "{dbname}1".format(dbname=self.ora_config.db_name)
        # self.ora_config.cluster_sid = sid
        # Create the Trove admin user
        with self.cursor(db_name,
                         user_id='sys',
                         password=sys_pwd) as sys_cursor:
            sys_cursor.execute(str(sql_query.CreateTablespace(
                ADMIN_USER_NAME)))
            sys_cursor.execute(str(sql_query.CreateUser(
                ADMIN_USER_NAME, self.ora_config.admin_password)))
            sys_cursor.execute(str(
                sql_query.Grant(ADMIN_USER_NAME, ['SYSDBA', 'SYSOPER'])))
        with self.cursor(db_name) as cursor:
            cursor.execute(str(sql_query.CreateRole(self.cloud_role_name)))
        LOG.debug("Successfully created database.")


class OracleRACApp(service.OracleApp):

    def __init__(self, status, state_change_wait_time=None):
        super(OracleRACApp, self).__init__(
            status, service.OracleClient, service.OracleCursor,
            OracleRACAdmin, state_change_wait_time)

    @property
    def user_home_dir(self):
        return path.expanduser('~' + INSTANCE_OWNER)

    def mount_storage(self, storage_info):
        fstab = path.join('/etc', 'fstab')
        default_mount_options = ('rw,bg,hard,nointr,tcp,vers=3,timeo=600,'
                                 'rsize=32768,wsize=32768,actimeo=0')
        data_mount_options = ('user,tcp,rsize=32768,wsize=32768,hard,intr,'
                              'noac,nfsvers=3')
        if storage_info['type'] == 'nfs':
            sources = storage_info['data']
            data = list()
            if operating_system.exists(fstab):
                data.append(operating_system.read_file(fstab, as_root=True))

            def _line(source, target, options=default_mount_options):
                data.append('{source} {target} nfs {options} 0 0'.format(
                    source=source, target=target, options=options))

            _line(sources['votedisk_mount'], SHARED_DISK_PATHS['votedisk'],)
            _line(sources['registry_mount'], SHARED_DISK_PATHS['registry'],)
            _line(sources['database_mount'], SHARED_DISK_PATHS['database'],
                  data_mount_options)
            operating_system.write_file(fstab, '\n'.join(data),
                                        as_root=True)
            utils.execute_with_timeout('mount', '-a',
                                       run_as_root=True,
                                       root_helper='sudo',
                                       timeout=service.ORACLE_TIMEOUT,
                                       log_output_on_error=True)
        else:
            raise exception.GuestError(_(
                "Storage type {t} not valid.").format(t=storage_info['type']))

    def write_oracle_user_file(self, filepath, contents,
                               filemode=operating_system.FileMode.SET_USR_RW):
        operating_system.write_file(filepath, contents, as_root=True)
        operating_system.chown(filepath, INSTANCE_OWNER, INSTANCE_OWNER_GROUP,
                               force=True, as_root=True)
        operating_system.chmod(filepath, filemode,
                               force=True, as_root=True)

    def configure_ssh(self, pem, pub):
        ssh_dir = path.join(self.user_home_dir, '.ssh')

        def _write(filename, contents,
                   filemode=operating_system.FileMode.SET_USR_RW):
            filepath = path.join(ssh_dir, filename)
            self.write_oracle_user_file(filepath, contents, filemode)

        _write('id_rsa', pem)
        _write('id_rsa.pub', pub)
        _write('authorized_keys', pub)

    def store_cluster_info(self, dbname, sys_password, admin_password):
        self.admin.store_cluster_info(dbname, sys_password, admin_password)

    def configure_hosts(self, cluster_name, public_cidr, private_cidr):
        """Configure /etc/hosts file."""
        hosts = path.join('/etc', 'hosts')
        pub_subnet = rac_utils.RACPublicSubnetManager(public_cidr)
        pri_subnet = rac_utils.CommonSubnetManager(private_cidr)
        data = []
        if operating_system.exists(hosts):
            data.append(operating_system.read_file(hosts, as_root=True))
        for scan_ip in pub_subnet.scan_list:
            data.append("{ip} {name}".format(ip=scan_ip, name=SCAN_NAME))
        for i in range(0, pub_subnet.max_instances()):
            hostname = rac_utils.make_instance_hostname(cluster_name, i)
            data.append("{pubip} {hostname}".format(
                pubip=pub_subnet.instance_ip(i), hostname=hostname))
            data.append("{vip} {hostname}-vip".format(
                vip=pub_subnet.instance_vip(i), hostname=hostname))
            data.append("{priip} {hostname}-priv".format(
                priip=pri_subnet.instance_ip(i), hostname=hostname))
        operating_system.write_file(hosts, '\n'.join(data),
                                    as_root=True)

    def establish_ssh_user_equivalency(self, host_ip_pairs):
        """Establish SSH user equivalency by using ssh-keyscan against the
        cluster hostnames and IPs.
        :arg host_ip_pairs: list of (hostname, ip) tuples to add
        """
        filepath = path.join(self.user_home_dir, '.ssh', 'known_hosts')
        data = []
        if operating_system.exists(filepath, as_root=True):
            data.append(operating_system.read_file(filepath, as_root=True))
        for host_ip_pair in host_ip_pairs:
            host = host_ip_pair[0]
            ip = host_ip_pair[1]
            (stdout, stderr) = run_sys_command(
                'ssh-keyscan {host},{ip}'.format(host=host, ip=ip))
            data.append(stdout.strip())
        self.write_oracle_user_file(filepath, '\n'.join(data))

    def edit_response_file(self, filename, edits):
        """Edit the given response file. Given a dictionary of edits, changes
        specified occurances of '<key>' to 'value'.
        """
        templates_dir = path.join(self.user_home_dir, 'rsp')
        template_file = path.join(templates_dir, filename)
        response_file = path.join(self.user_home_dir, filename)
        contents = operating_system.read_file(template_file, as_root=True)
        for key in edits.keys():
            contents = contents.replace('<{key}>'.format(key=key), edits[key])
        self.write_oracle_user_file(response_file, contents)
        return response_file

    def _update_crsconfig_params_hostname(self, hostname):
        filepath = path.join(GRID_HOME, 'crs', 'install', 'crsconfig_params')
        contents = re.sub(r'INSTALL_NODE=.*',
                          'INSTALL_NODE={hostname}'.format(hostname=hostname),
                          operating_system.read_file(filepath, as_root=True))
        self.write_oracle_user_file(
            filepath, contents, filemode=operating_system.FileMode.SET_FULL)

    def _get_hostname(self):
        return socket.gethostname().split('.')[0]

    def _get_if_addr_pairs(self):
        stdout, stderr = utils.execute_with_timeout('ip', '-4', '-o', 'a')
        return [(words[1], words[3]) for words in
                [line.split() for line in stdout.strip().split('\n')]]

    def configure_grid(self, cluster_id, cluster_name, nodes_string,
                       public_cidr, private_cidr):
        """Generate a response file from the GRID_CONF_RSP file and run the
        installer in silent mode.
        """
        # Get the interfaces used for the public and private connections by
        # getting a list of interface address and checking them against
        # the given subnets
        hostname = self._get_hostname()
        if_addr_pairs = self._get_if_addr_pairs()
        interfaces = []
        for pair in if_addr_pairs:
            if pair[0] == 'lo':
                continue
            subnet = rac_utils.CommonSubnetManager(pair[1])
            cidr = str(subnet.cidr)
            iftype = 3
            if cidr == public_cidr:
                iftype = 1
            if cidr == private_cidr:
                iftype = 2
            interfaces.append(
                ':'.join([pair[0], str(subnet.network_id), str(iftype)]))
        edits = {'HOST': hostname,
                 'CLUSTER_NAME': cluster_name,
                 'CLUSTER_ID': cluster_id,
                 'NODES': nodes_string,
                 'IFS': ','.join(interfaces)}
        rsp_file = self.edit_response_file(GRID_CONF_RSP, edits)
        installer_path = path.join(GRID_HOME, 'crs', 'config', 'config.sh')
        run_sys_command("{inst} -silent -ignorePrereq -waitforcompletion "
                        "-responseFile {rsp}".format(inst=installer_path,
                                                     rsp=rsp_file))
        self._update_crsconfig_params_hostname(hostname)

    def run_grid_root(self):
        root_script = path.join(GRID_HOME, 'root.sh')
        utils.execute_with_timeout(
            root_script, run_as_root=True, root_helper='sudo',
            timeout=CONF.get(MANAGER).configuration_timeout,
            log_output_on_error=True)

    def install_oracle_database(self, nodes_string):
        edits = {'HOST': self._get_hostname(),
                 'NODES': nodes_string}
        rsp_file = self.edit_response_file(ORACLE_INST_RSP, edits)
        installer_path = path.join(ORACLE_INST, 'database', 'runInstaller')
        run_sys_command("{inst} -silent -waitforcompletion -ignoreSysPrereqs "
                        "-ignorePrereq -showProgress -responseFile "
                        "{rsp}".format(inst=installer_path, rsp=rsp_file))

    def run_oracle_root(self):
        root_script = path.join(ORACLE_HOME, 'root.sh')
        utils.execute_with_timeout(
            root_script, run_as_root=True, root_helper='sudo',
            timeout=CONF.get(MANAGER).configuration_timeout,
            log_output_on_error=True)

    def get_private_vip(self, ip):
        if_addr_pairs = self._get_if_addr_pairs()
        for i in range(len(if_addr_pairs)):
            # address shows up as 192.168.70.3/24, so discard the /24
            if if_addr_pairs[i][1].split('/')[0] == ip:
                if if_addr_pairs[i][0] != if_addr_pairs[i+1][0]:
                    raise exception.GuestError(_(
                        "Missing interconnect {interface} virtual IP."
                    ).format(interface=if_addr_pairs[i][0]))
                return if_addr_pairs[i+1][1].split('/')[0]
        raise exception.GuestError(_(
            "Could not find interface with address {ip}").format(ip=ip))

    def create_rac_database(self, nodes_string):
        self.admin.create_rac_database(nodes_string)

    def determine_sid(self):
        hostname = self._get_hostname()
        stdout, stderr = run_sys_command(
            '{crsctl} stat res -w "TYPE = ora.database.type" -t'.format(
                crsctl=path.join(GRID_HOME, 'bin', 'crsctl')))
        instance_id = None
        for line in stdout.split('\n'):
            words = line.strip().split()
            if hostname in words:
                instance_id = words[0]
        if not instance_id:
            raise exception.GuestError(_(
                "Could not find hostname in crsctl information."))
        self.admin.ora_config.cluster_sid = "{dbname}{instance_id}".format(
            dbname=self.admin.ora_config.db_name, instance_id=instance_id)

    def stop_db(self, do_not_start_on_reboot=False):
        pass
