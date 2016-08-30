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

import netaddr


SCAN_COUNT = 1


def make_object_name(conf, tags, cluster_id):
    """Create an object name using the conf prefix, a list of tags,
    and cluster id.
    """
    return '-'.join([conf.network_object_name_prefix] + tags + [cluster_id])


def make_interconnect_network_name(conf, cluster_id):
    return make_object_name(
        conf, ['interconnect', 'network'], cluster_id)


def make_instance_hostname(cluster_name, index):
    return '-'.join([cluster_name, 'node', str(index + 1)])


class CommonSubnetManager(object):
    """A common subnet. dhcp enabled. Allocation pool is all valid ips."""

    def __init__(self, cidr):
        self._network = netaddr.IPNetwork(cidr)

    def __repr__(self):
        return str(self._network)

    @property
    def cidr(self):
        return self._network.cidr

    @property
    def network_id(self):
        return netaddr.IPAddress(self._network.first)

    @property
    def broadcast_address(self):
        return netaddr.IPAddress(self._network.last)

    @property
    def gateway(self):
        return False

    @property
    def dhcp_server(self):
        if self.gateway:
            return self.gateway + 1
        return self.network_id + 1

    @property
    def allocation_pool(self):
        """dhcp allocation pool range."""
        return netaddr.IPRange(self.dhcp_server + 1,
                               self.broadcast_address - 1)

    def instance_index_from_ip(self, ip):
        for i, addr in enumerate(self.allocation_pool):
            if ip == str(addr):
                return i
        return None

    def instance_ip(self, i):
        return str(self.allocation_pool[i])

    def max_instances(self):
        return len(self.allocation_pool)


class RACPublicSubnetManager(CommonSubnetManager):
    """Provides functions for Oracle RAC ips based on a given subnet.
    The subnet starts with a gateway, then has the SCAN ips.
    The remaining room is split in 2: the first half is designated to the
    physical ips, and the last half is the virtual ips.
    """

    @property
    def gateway(self):
        return self.network_id + 1

    @property
    def scan_ips(self):
        """SCAN ip addresses are the first n ips after the gateway."""
        return netaddr.IPRange(self.dhcp_server + 1,
                               self.dhcp_server + SCAN_COUNT)

    @property
    def scan_list(self):
        return [str(ip) for ip in self.scan_ips]

    @property
    def unreserved_addresses(self):
        return netaddr.IPRange(self.scan_ips.last + 1,
                               self.broadcast_address - 1)

    @property
    def allocation_pool(self):
        """First half of the available address range."""
        return netaddr.IPRange(self.unreserved_addresses.first,
                               self.unreserved_addresses.first +
                               len(self.unreserved_addresses) / 2 - 1)

    @property
    def virtual_ips(self):
        """Virtual ips assigned to the instances. Defined as the second half
        of the available address range.
        """
        return netaddr.IPRange(self.unreserved_addresses.first +
                               len(self.unreserved_addresses) / 2,
                               self.unreserved_addresses.last)

    def instance_vip(self, i):
        return str(self.virtual_ips[i])
