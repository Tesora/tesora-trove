# flake8: noqa

# Copyright (c) 2016 Tesora, Inc.
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

from oslo_log import log as logging

from trove.common import cfg
from trove.common.strategies.cluster import base
from trove.guestagent import api as guest_api


LOG = logging.getLogger(__name__)
CONF = cfg.CONF
CONFIGURATION_TIMEOUT = CONF.get('oracle_rac').configuration_timeout


class OracleRACGuestAgentStrategy(base.BaseGuestAgentStrategy):

    @property
    def guest_client_class(self):
        return OracleRACGuestAgentAPI


class OracleRACGuestAgentAPI(guest_api.API):

    def configure_hosts(self, cluster_name, public_cidr, private_cidr):
        return self._call("configure_hosts", guest_api.AGENT_LOW_TIMEOUT,
                          self.version_cap, cluster_name=cluster_name,
                          public_cidr=public_cidr,
                          private_cidr=private_cidr)

    def establish_ssh_user_equivalency(self, host_ip_pairs):
        return self._call("establish_ssh_user_equivalency",
                          guest_api.AGENT_HIGH_TIMEOUT, self.version_cap,
                          host_ip_pairs=host_ip_pairs)

    def configure_grid(self, cluster_id, cluster_name, nodes_string,
                       public_cidr, private_cidr):
        return self._call("configure_grid", CONFIGURATION_TIMEOUT,
                          self.version_cap, cluster_id=cluster_id,
                          cluster_name=cluster_name, nodes_string=nodes_string,
                          public_cidr=public_cidr, private_cidr=private_cidr)

    def run_grid_root(self):
        return self._call("run_grid_root", CONFIGURATION_TIMEOUT,
                          self.version_cap)

    def install_oracle_database(self, nodes_string):
        return self._call("install_oracle_database", CONFIGURATION_TIMEOUT,
                          self.version_cap, nodes_string=nodes_string)

    def run_oracle_root(self):
        return self._call("run_oracle_root", CONFIGURATION_TIMEOUT,
                          self.version_cap)

    def get_private_vip(self, ip):
        return self._call("get_private_vip", guest_api.AGENT_LOW_TIMEOUT,
                          self.version_cap, ip=ip)

    def create_rac_database(self, nodes_string):
        return self._call("create_rac_database", CONFIGURATION_TIMEOUT,
                          self.version_cap, nodes_string=nodes_string)

    def determine_sid(self):
        return self._call("determine_sid", guest_api.AGENT_LOW_TIMEOUT,
                          self.version_cap)

    def cluster_complete(self):
        LOG.debug("Notifying cluster install completion.")
        return self._call("cluster_complete", guest_api.AGENT_HIGH_TIMEOUT,
                          self.version_cap)

