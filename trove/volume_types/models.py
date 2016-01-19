# Copyright 2016 Tesora, Inc
#
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

"""Model classes that form the core of volume-support functionality"""

from cinderclient import exceptions as cinder_exception
from trove.common import exception as trove_exception
from trove.common.models import CinderRemoteModelBase
from trove.common.remote import create_cinder_client


class VolumeType(object):

    _data_fields = ['id', 'name', 'is_public', 'description']

    def __init__(self, volume_type=None, context=None, volume_type_id=None):
        """
        Initialize the volume type either from the volume_type parameter, or
        by querying cinder using the context provided.
        """

        if volume_type and not (volume_type_id or context):
            self.volume_type = volume_type
        elif volume_type_id and context:
            try:
                client = create_cinder_client(context)
                self.volume_type = client.volume_types.get(volume_type_id)
            except cinder_exception.NotFound:
                raise trove_exception.NotFound(uuid=volume_type_id)
            except cinder_exception.ClientException as ce:
                raise trove_exception.TroveError(str(ce))

            return
        else:
            raise trove_exception.InvalidModelError(
                errors="An invalid set of arguments were provided.")

    @property
    def id(self):
        return self.volume_type.id

    @property
    def name(self):
        return self.volume_type.name

    @property
    def is_public(self):
        return self.volume_type.is_public

    @property
    def description(self):
        return self.volume_type.description


class VolumeTypes(CinderRemoteModelBase):

    def __init__(self, context):
        volume_types = create_cinder_client(context).volume_types.list()
        self.volume_types = [VolumeType(volume_type=item)
                             for item in volume_types]

    def __iter__(self):
        for item in self.volume_types:
            yield item
