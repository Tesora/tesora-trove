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

from trove.common import cfg

CONF = cfg.CONF


TIMEOUT = 1200
ORACLE_INSTANCE_OWNER = "oracle"
ORACLE_GROUP_OWNER = "oinstall"
ORACLE_STATUS = ("ps -ef | grep " + ORACLE_INSTANCE_OWNER +
                 " | grep ora_smon | "
                 "grep -v grep | wc -l")
CHANGE_DB_DIR_OWNER = "sudo chown " + ORACLE_INSTANCE_OWNER + " %(datadir)s"
CHANGE_DB_DIR_GROUP_OWNER = (
    "sudo chgrp " + ORACLE_GROUP_OWNER + " %(datadir)s")

CREATE_DB_COMMAND = ("dbca -silent -createDatabase "
                     "-templateName %(template)s "
                     "-gdbName %(gdbname)s "
                     "-sid %(sid)s "
                     "-sysPassword %(pswd)s "
                     "-systemPassword %(pswd)s "
                     "-storageType FS "
                     "-totalMemory %(db_ram)s")

DELETE_DB_COMMAND = ("dbca -silent -deleteDatabase -sourceDB %(db)s "
                     "-sysDBAUserName %(sys_user)s "
                     "-sysDBAPassword %(sys_pswd)s")

PARAMETER_DIR_NAME = "dbs"
PFILE_NAME = "os_pfile.ora"
SPFILE_NAME = "os_spfile.ora"
NEW_SPFILE_NAME = "new_os_spfile.ora"
