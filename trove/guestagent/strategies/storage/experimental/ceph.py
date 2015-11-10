# Copyright 2015 Tesora Inc.
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

import hashlib

from oslo_log import log as logging

from trove.common import cfg
from trove.common.i18n import _
from trove.guestagent.strategies.storage import swift

LOG = logging.getLogger(__name__)
CONF = cfg.CONF

BACKUP_CONTAINER = CONF.backup_swift_container


class CephStorage(swift.SwiftStorage):
    """Implementation of Storage Strategy for Ceph.
    This uses Ceph's Swift API compatibility features.
    """
    __strategy_name__ = 'ceph'

    def save(self, filename, stream, metadata=None):
        """Persist information from stream to Ceph.

        The file is saved to the location <BACKUP_CONTAINER>/<filename>.
        The filename is defined on the backup runner manifest property
        which is typically in the format '<backup_id>.<ext>.gz'

        This is different to Swift's implementation
        """

        # Create the container if it doesn't already exist
        self.connection.put_container(BACKUP_CONTAINER)

        # Swift Checksum is the checksum of the concatenated segment checksums
        swift_checksum = hashlib.md5()

        # Wrap the output of the backup process to segment it for swift
        stream_reader = swift.StreamReader(stream, filename)

        url = self.connection.url
        # Full location where the backup manifest is stored
        location = "%s/%s/%s" % (url, BACKUP_CONTAINER, filename)

        # Read from the stream and write to the container in swift
        while not stream_reader.end_of_file:
            etag = self.connection.put_object(BACKUP_CONTAINER,
                                              stream_reader.segment,
                                              stream_reader)

            segment_checksum = stream_reader.segment_checksum.hexdigest()

            # Check each segment MD5 hash against swift etag
            # Raise an error and mark backup as failed
            if etag != segment_checksum:
                LOG.error(_("Error saving data segment to swift. "
                          "ETAG: %(tag)s Segment MD5: %(checksum)s."),
                          {'tag': etag, 'checksum': segment_checksum})
                return False, "Error saving data to Swift!", None, location

            swift_checksum.update(segment_checksum)

        # Create the manifest file
        # We create the manifest file after all the segments have been uploaded
        # so a partial swift object file can't be downloaded; if the manifest
        # file exists then all segments have been uploaded so the whole backup
        # file can be downloaded.
        headers = {'X-Object-Manifest': stream_reader.prefix}

        self.connection.put_object(BACKUP_CONTAINER,
                                   filename,
                                   contents='',
                                   headers=headers)

        resp = self.connection.head_object(BACKUP_CONTAINER, filename)
        # swift returns etag in double quotes
        # e.g. '"dc3b0827f276d8d78312992cc60c2c3f"'
        etag = resp['etag'].strip('"')

        # Ceph ETAG calculations on manifest files are based on the contents
        # of the manifest file itself - which is empty - and not on the
        # contents of each segment.  This is different from how Swift
        # calculates ETAGs for manifest files (using the contents of each
        # segment).  Therefore it is not possible to compare the manifest
        # ETAG to the swift_checksum.

        # Force the final_swift_checksum to match the incorrect ETAG
        # calculation.
        # final_swift_checksum = swift_checksum.hexdigest()
        final_swift_checksum = etag

        # if etag != final_swift_checksum:
        #     LOG.error(
        #         _("Error saving data to swift. Manifest "
        #           "ETAG: %(tag)s Swift MD5: %(checksum)s"),
        #         {'tag': etag, 'checksum': final_swift_checksum})
        #     return False, "Error saving data to Swift!", None, location

        return (True, "Successfully saved data to Swift!",
                final_swift_checksum, location)
