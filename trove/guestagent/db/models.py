# Copyright (c) 2011 OpenStack Foundation
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

import abc
import re
import string

import netaddr

from trove.common import cfg
from trove.common import exception
from trove.common.i18n import _
from trove.common import utils

CONF = cfg.CONF


class Base(object):

    def serialize(self):
        return self.__dict__

    def deserialize(self, o):
        self.__dict__ = o

    @classmethod
    def _validate_dict(cls, value):
        reqs = cls._dict_requirements()
        return (isinstance(value, dict) and
                all(key in value for key in reqs))

    @classmethod
    @abc.abstractmethod
    def _dict_requirements(cls):
        """Get the dictionary requirements for a user created via
        deserialization.
        :returns:           List of required dictionary keys.
        """


class DatastoreSchema(Base):
    """Represents a database schema."""

    def __init__(self, name, deserializing=False, *args, **kwargs):
        self._name = None
        self._collate = None
        self._character_set = None

        # need one or the other, not both, not none (!= ~ XOR)
        if not (bool(deserializing) != bool(name)):
            raise ValueError(_("Bad args. name: %(name)s, "
                               "deserializing %(deser)s.")
                             % ({'name': bool(name),
                                 'deser': bool(deserializing)}))
        if not deserializing:
            self.name = name

    @classmethod
    def deserialize_schema(cls, value):
        if not cls._validate_dict(value):
            raise ValueError(_("Bad dictionary. Keys: %(keys)s. "
                               "Required: %(reqs)s")
                             % ({'keys': value.keys(),
                                 'reqs': cls._dict_requirements()}))
        schema = cls(name=None, deserializing=True)
        schema.deserialize(value)
        return schema

    @property
    def name(self):
        return self._name

    @name.setter
    def name(self, value):
        self._validate_schema_name(value)
        self._name = value

    # TODO(pmalik): The collate and character_set properties are datastore
    # specific and should be moved to derived classes that require them.
    # They are currently here because many datastores still use the MySQL
    # extensions which try to access them.
    @property
    def collate(self):
        return self._collate

    @property
    def character_set(self):
        return self._character_set

    def _validate_schema_name(self, value):
        """Perform validations on a given schema name.
        :param value:        Validated schema name.
        :type value:         string
        :raises:             ValueError On validation errors.
        """
        if self._max_schema_name_length and (len(value) >
                                             self._max_schema_name_length):
            raise ValueError(_("Schema name '%(name)s' is too long. "
                               "Max length = %(max_length)d.")
                             % {'name': value,
                                'max_length': self._max_schema_name_length})
        elif not self._is_valid_schema_name(value):
            raise ValueError(_("'%s' is not a valid schema name.") % value)

    @abc.abstractproperty
    def _max_schema_name_length(self):
        """Return the maximum valid schema name length if any.
        :returns:            Maximum schema name length or None if unlimited.
        """

    @abc.abstractmethod
    def _is_valid_schema_name(self, value):
        """Validate a given schema name.
        :param value:        Validated schema name.
        :type value:         string
        :returns:            TRUE if valid, FALSE otherwise.
        """

    @classmethod
    @abc.abstractmethod
    def _dict_requirements(cls):
        """Get the dictionary requirements for a user created via
        deserialization.
        :returns:           List of required dictionary keys.
        """


class MongoDBSchema(DatastoreSchema):
    """Represents the MongoDB schema and its associated properties.

    MongoDB database names are limited to 128 characters,
    alphanumeric and - and _ only.
    """

    name_regex = re.compile(r'^[a-zA-Z0-9_\-]+$')

    def __init__(self, name, *args, **kwargs):
        super(MongoDBSchema, self).__init__(name, *args, **kwargs)

    @property
    def _max_schema_name_length(self):
        return 64

    def _is_valid_schema_name(self, value):
        # check against the invalid character set from
        # http://docs.mongodb.org/manual/reference/limits
        return not any(c in value for c in '/\. "$')

    @classmethod
    def _dict_requirements(cls):
        return ['_name']


class OracleSchema(DatastoreSchema):
    """Represents a Oracle schema and its associated properties.

    Oracle database names need to be alphanumeric and the length cannot exceed
    8 characters.
    """
    name_regex = re.compile(r'[a-zA-Z0-9]\w+$')

    def __init__(self, name, *args, **kwargs):
        super(OracleSchema, self).__init__(name, *args, **kwargs)

    @property
    def _max_schema_name_length(self):
        return 8

    def _is_valid_schema_name(self, value):
        return self.name_regex.match(value) is not None

    @classmethod
    def _dict_requirements(cls):
        return ['_name']


class CassandraSchema(DatastoreSchema):
    """Represents a Cassandra schema and its associated properties.

    Keyspace names are 32 or fewer alpha-numeric characters and underscores,
    the first of which is an alpha character.
    """

    def __init__(self, name, *args, **kwargs):
        super(CassandraSchema, self).__init__(name, *args, **kwargs)

    @property
    def _max_schema_name_length(self):
        return 32

    def _is_valid_schema_name(self, value):
        return True

    @classmethod
    def _dict_requirements(cls):
        return ['_name']


class PostgreSQLSchema(DatastoreSchema):
    """Represents a PostgreSQL schema and its associated properties.

    Permitted characters in quoted identifiers include the full
    Unicode Basic Multilingual Plane (BMP), except U+0000.
    Database, table, and column names cannot end with space characters.
    """
    name_regex = re.compile(ur'^[\u0001-\u007F\u0080-\uFFFF]+[^\s]$')

    def __init__(self, name, character_set=None, collate=None,
                 *args, **kwargs):
        super(PostgreSQLSchema, self).__init__(name, *args, **kwargs)

        self.character_set = character_set
        self.collate = collate

    @DatastoreSchema.collate.setter
    def collate(self, value):
        self._collate = value

    @DatastoreSchema.character_set.setter
    def character_set(self, value):
        self._character_set = value

    @property
    def _max_schema_name_length(self):
        return 63

    def _is_valid_schema_name(self, value):
        return self.name_regex.match(value) is not None

    @classmethod
    def _dict_requirements(cls):
        return ['_name']


class CouchbaseSchema(DatastoreSchema):
    """Represents a Couchbase bucket and its associated properties.

    The bucket name can only contain characters in range A-Z, a-z, 0-9
    as well as underscore, period, dash and percent symbols and
    can be a maximum of 100 characters in length.
    """

    name_regex = re.compile(ur'^[a-zA-Z0-9_\.\-%]+$')

    def __init__(self, name, *args, **kwargs):
        super(CouchbaseSchema, self).__init__(name, *args, **kwargs)

    @property
    def _max_schema_name_length(self):
        return 100

    def _is_valid_schema_name(self, value):
        return self.name_regex.match(value) is not None


class CouchDBSchema(DatastoreSchema):
    '''Represents the CouchDB schema and its associated properties.

    The database name must consist of one or more of the following characters
    and the name must begin with a lowercase letter.

    - Lowercase characters (a-z)
    - Digits (0-9)
    - Any of the characters _, $, (, ), +, -, and /
    '''

    name_regex = re.compile(r'^[a-z][a-z0-9_$()+/-]*$')

    def __init__(self, name=None, deserializing=False):
        super(CouchDBSchema, self).__init__()
        self._ignore_dbs = cfg.get_ignored_dbs()
        # need one or the other, not both, not none (!= ~ XOR)
        if not (bool(deserializing) != bool(name)):
            raise ValueError(_("Bad args. name: %(name)s, "
                               "deserializing %(deser)s.")
                             % ({'name': bool(name),
                                 'deser': bool(deserializing)}))
        if not deserializing:
            self.name = name

    @property
    def _max_schema_name_length(self):
        return None

    def _is_valid_schema_name(self, value):
        # https://wiki.apache.org/couchdb/HTTP_database_API
        if value.lower() in self._ignore_dbs:
            return False
        if re.match(r'^[a-z]*$', value[0]):
            return True
        else:
            return False

    @classmethod
    def _dict_requirements(cls):
        return ['_name']


class MySQLDatabase(Base):
    """Represents a Database and its properties."""

    # Defaults
    __charset__ = "utf8"
    __collation__ = "utf8_general_ci"
    dbname = re.compile("^[A-Za-z0-9_-]+[\s\?\#\@]*[A-Za-z0-9_-]+$")

    # Complete list of acceptable values
    charset = {"big5": ["big5_chinese_ci", "big5_bin"],
               "dec8": ["dec8_swedish_ci", "dec8_bin"],
               "cp850": ["cp850_general_ci", "cp850_bin"],
               "hp8": ["hp8_english_ci", "hp8_bin"],
               "koi8r": ["koi8r_general_ci", "koi8r_bin"],
               "latin1": ["latin1_swedish_ci",
                          "latin1_german1_ci",
                          "latin1_danish_ci",
                          "latin1_german2_ci",
                          "latin1_bin",
                          "latin1_general_ci",
                          "latin1_general_cs",
                          "latin1_spanish_ci"],
               "latin2": ["latin2_general_ci",
                          "latin2_czech_cs",
                          "latin2_hungarian_ci",
                          "latin2_croatian_ci",
                          "latin2_bin"],
               "swe7": ["swe7_swedish_ci", "swe7_bin"],
               "ascii": ["ascii_general_ci", "ascii_bin"],
               "ujis": ["ujis_japanese_ci", "ujis_bin"],
               "sjis": ["sjis_japanese_ci", "sjis_bin"],
               "hebrew": ["hebrew_general_ci", "hebrew_bin"],
               "tis620": ["tis620_thai_ci", "tis620_bin"],
               "euckr": ["euckr_korean_ci", "euckr_bin"],
               "koi8u": ["koi8u_general_ci", "koi8u_bin"],
               "gb2312": ["gb2312_chinese_ci", "gb2312_bin"],
               "greek": ["greek_general_ci", "greek_bin"],
               "cp1250": ["cp1250_general_ci",
                          "cp1250_czech_cs",
                          "cp1250_croatian_ci",
                          "cp1250_bin",
                          "cp1250_polish_ci"],
               "gbk": ["gbk_chinese_ci", "gbk_bin"],
               "latin5": ["latin5_turkish_ci", "latin5_bin"],
               "armscii8": ["armscii8_general_ci", "armscii8_bin"],
               "utf8": ["utf8_general_ci",
                        "utf8_bin",
                        "utf8_unicode_ci",
                        "utf8_icelandic_ci",
                        "utf8_latvian_ci",
                        "utf8_romanian_ci",
                        "utf8_slovenian_ci",
                        "utf8_polish_ci",
                        "utf8_estonian_ci",
                        "utf8_spanish_ci",
                        "utf8_swedish_ci",
                        "utf8_turkish_ci",
                        "utf8_czech_ci",
                        "utf8_danish_ci",
                        "utf8_lithuanian_ci",
                        "utf8_slovak_ci",
                        "utf8_spanish2_ci",
                        "utf8_roman_ci",
                        "utf8_persian_ci",
                        "utf8_esperanto_ci",
                        "utf8_hungarian_ci"],
               "ucs2": ["ucs2_general_ci",
                        "ucs2_bin",
                        "ucs2_unicode_ci",
                        "ucs2_icelandic_ci",
                        "ucs2_latvian_ci",
                        "ucs2_romanian_ci",
                        "ucs2_slovenian_ci",
                        "ucs2_polish_ci",
                        "ucs2_estonian_ci",
                        "ucs2_spanish_ci",
                        "ucs2_swedish_ci",
                        "ucs2_turkish_ci",
                        "ucs2_czech_ci",
                        "ucs2_danish_ci",
                        "ucs2_lithuanian_ci",
                        "ucs2_slovak_ci",
                        "ucs2_spanish2_ci",
                        "ucs2_roman_ci",
                        "ucs2_persian_ci",
                        "ucs2_esperanto_ci",
                        "ucs2_hungarian_ci"],
               "cp866": ["cp866_general_ci", "cp866_bin"],
               "keybcs2": ["keybcs2_general_ci", "keybcs2_bin"],
               "macce": ["macce_general_ci", "macce_bin"],
               "macroman": ["macroman_general_ci", "macroman_bin"],
               "cp852": ["cp852_general_ci", "cp852_bin"],
               "latin7": ["latin7_general_ci",
                          "latin7_estonian_cs",
                          "latin7_general_cs",
                          "latin7_bin"],
               "cp1251": ["cp1251_general_ci",
                          "cp1251_bulgarian_ci",
                          "cp1251_ukrainian_ci",
                          "cp1251_bin",
                          "cp1251_general_cs"],
               "cp1256": ["cp1256_general_ci", "cp1256_bin"],
               "cp1257": ["cp1257_general_ci",
                          "cp1257_lithuanian_ci",
                          "cp1257_bin"],
               "binary": ["binary"],
               "geostd8": ["geostd8_general_ci", "geostd8_bin"],
               "cp932": ["cp932_japanese_ci", "cp932_bin"],
               "eucjpms": ["eucjpms_japanese_ci", "eucjpms_bin"]}

    collation = {"big5_chinese_ci": "big5",
                 "big5_bin": "big5",
                 "dec8_swedish_ci": "dec8",
                 "dec8_bin": "dec8",
                 "cp850_general_ci": "cp850",
                 "cp850_bin": "cp850",
                 "hp8_english_ci": "hp8",
                 "hp8_bin": "hp8",
                 "koi8r_general_ci": "koi8r",
                 "koi8r_bin": "koi8r",
                 "latin1_german1_ci": "latin1",
                 "latin1_swedish_ci": "latin1",
                 "latin1_danish_ci": "latin1",
                 "latin1_german2_ci": "latin1",
                 "latin1_bin": "latin1",
                 "latin1_general_ci": "latin1",
                 "latin1_general_cs": "latin1",
                 "latin1_spanish_ci": "latin1",
                 "latin2_czech_cs": "latin2",
                 "latin2_general_ci": "latin2",
                 "latin2_hungarian_ci": "latin2",
                 "latin2_croatian_ci": "latin2",
                 "latin2_bin": "latin2",
                 "swe7_swedish_ci": "swe7",
                 "swe7_bin": "swe7",
                 "ascii_general_ci": "ascii",
                 "ascii_bin": "ascii",
                 "ujis_japanese_ci": "ujis",
                 "ujis_bin": "ujis",
                 "sjis_japanese_ci": "sjis",
                 "sjis_bin": "sjis",
                 "hebrew_general_ci": "hebrew",
                 "hebrew_bin": "hebrew",
                 "tis620_thai_ci": "tis620",
                 "tis620_bin": "tis620",
                 "euckr_korean_ci": "euckr",
                 "euckr_bin": "euckr",
                 "koi8u_general_ci": "koi8u",
                 "koi8u_bin": "koi8u",
                 "gb2312_chinese_ci": "gb2312",
                 "gb2312_bin": "gb2312",
                 "greek_general_ci": "greek",
                 "greek_bin": "greek",
                 "cp1250_general_ci": "cp1250",
                 "cp1250_czech_cs": "cp1250",
                 "cp1250_croatian_ci": "cp1250",
                 "cp1250_bin": "cp1250",
                 "cp1250_polish_ci": "cp1250",
                 "gbk_chinese_ci": "gbk",
                 "gbk_bin": "gbk",
                 "latin5_turkish_ci": "latin5",
                 "latin5_bin": "latin5",
                 "armscii8_general_ci": "armscii8",
                 "armscii8_bin": "armscii8",
                 "utf8_general_ci": "utf8",
                 "utf8_bin": "utf8",
                 "utf8_unicode_ci": "utf8",
                 "utf8_icelandic_ci": "utf8",
                 "utf8_latvian_ci": "utf8",
                 "utf8_romanian_ci": "utf8",
                 "utf8_slovenian_ci": "utf8",
                 "utf8_polish_ci": "utf8",
                 "utf8_estonian_ci": "utf8",
                 "utf8_spanish_ci": "utf8",
                 "utf8_swedish_ci": "utf8",
                 "utf8_turkish_ci": "utf8",
                 "utf8_czech_ci": "utf8",
                 "utf8_danish_ci": "utf8",
                 "utf8_lithuanian_ci": "utf8",
                 "utf8_slovak_ci": "utf8",
                 "utf8_spanish2_ci": "utf8",
                 "utf8_roman_ci": "utf8",
                 "utf8_persian_ci": "utf8",
                 "utf8_esperanto_ci": "utf8",
                 "utf8_hungarian_ci": "utf8",
                 "ucs2_general_ci": "ucs2",
                 "ucs2_bin": "ucs2",
                 "ucs2_unicode_ci": "ucs2",
                 "ucs2_icelandic_ci": "ucs2",
                 "ucs2_latvian_ci": "ucs2",
                 "ucs2_romanian_ci": "ucs2",
                 "ucs2_slovenian_ci": "ucs2",
                 "ucs2_polish_ci": "ucs2",
                 "ucs2_estonian_ci": "ucs2",
                 "ucs2_spanish_ci": "ucs2",
                 "ucs2_swedish_ci": "ucs2",
                 "ucs2_turkish_ci": "ucs2",
                 "ucs2_czech_ci": "ucs2",
                 "ucs2_danish_ci": "ucs2",
                 "ucs2_lithuanian_ci": "ucs2",
                 "ucs2_slovak_ci": "ucs2",
                 "ucs2_spanish2_ci": "ucs2",
                 "ucs2_roman_ci": "ucs2",
                 "ucs2_persian_ci": "ucs2",
                 "ucs2_esperanto_ci": "ucs2",
                 "ucs2_hungarian_ci": "ucs2",
                 "cp866_general_ci": "cp866",
                 "cp866_bin": "cp866",
                 "keybcs2_general_ci": "keybcs2",
                 "keybcs2_bin": "keybcs2",
                 "macce_general_ci": "macce",
                 "macce_bin": "macce",
                 "macroman_general_ci": "macroman",
                 "macroman_bin": "macroman",
                 "cp852_general_ci": "cp852",
                 "cp852_bin": "cp852",
                 "latin7_estonian_cs": "latin7",
                 "latin7_general_ci": "latin7",
                 "latin7_general_cs": "latin7",
                 "latin7_bin": "latin7",
                 "cp1251_bulgarian_ci": "cp1251",
                 "cp1251_ukrainian_ci": "cp1251",
                 "cp1251_bin": "cp1251",
                 "cp1251_general_ci": "cp1251",
                 "cp1251_general_cs": "cp1251",
                 "cp1256_general_ci": "cp1256",
                 "cp1256_bin": "cp1256",
                 "cp1257_lithuanian_ci": "cp1257",
                 "cp1257_bin": "cp1257",
                 "cp1257_general_ci": "cp1257",
                 "binary": "binary",
                 "geostd8_general_ci": "geostd8",
                 "geostd8_bin": "geostd8",
                 "cp932_japanese_ci": "cp932",
                 "cp932_bin": "cp932",
                 "eucjpms_japanese_ci": "eucjpms",
                 "eucjpms_bin": "eucjpms"}

    def __init__(self):
        self._name = None
        self._collate = None
        self._character_set = None
        self._ignore_dbs = cfg.get_ignored_dbs()

    @property
    def name(self):
        return self._name

    def _is_valid(self, value):
        return value.lower() not in self._ignore_dbs

    @name.setter
    def name(self, value):
        self._name = value

    @property
    def collate(self):
        """Get the appropriate collate value."""
        if not self._collate and not self._character_set:
            return self.__collation__
        elif not self._collate:
            return self.charset[self._character_set][0]
        else:
            return self._collate

    @collate.setter
    def collate(self, value):
        """Validate the collation and set it."""
        if not value:
            pass
        elif self._character_set:
            if value not in self.charset[self._character_set]:
                msg = (_("%(val)s not a valid collation for charset %(char)s.")
                       % {'val': value, 'char': self._character_set})
                raise ValueError(msg)
            self._collate = value
        else:
            if value not in self.collation:
                raise ValueError(_("'%s' not a valid collation.") % value)
            self._collate = value
            self._character_set = self.collation[value]

    @property
    def character_set(self):
        """Get the appropriate character set value."""
        if not self._character_set:
            return self.__charset__
        else:
            return self._character_set

    @character_set.setter
    def character_set(self, value):
        """Validate the character set and set it."""
        if not value:
            pass
        elif value not in self.charset:
            raise ValueError(_("'%s' not a valid character set.") % value)
        else:
            self._character_set = value


class ValidatedMySQLDatabase(MySQLDatabase):

    @MySQLDatabase.name.setter
    def name(self, value):
        if any([not value,
                not self._is_valid(value),
                not self.dbname.match(value),
                string.find("%r" % value, "\\") != -1]):
            raise ValueError(_("'%s' is not a valid database name.") % value)
        elif len(value) > 64:
            msg = _("Database name '%s' is too long. Max length = 64.")
            raise ValueError(msg % value)
        else:
            self._name = value

    @property
    def collate(self):
        """Get the appropriate collate value."""
        if not self._collate and not self._character_set:
            return self.__collation__
        elif not self._collate:
            return self.charset[self._character_set][0]
        else:
            return self._collate

    @collate.setter
    def collate(self, value):
        """Validate the collation and set it."""
        if not value:
            pass
        elif self._character_set:
            if value not in self.charset[self._character_set]:
                msg = (_("%(val)s not a valid collation for charset %(char)s.")
                       % {'val': value, 'char': self._character_set})
                raise ValueError(msg)
            self._collate = value
        else:
            if value not in self.collation:
                raise ValueError(_("'%s' not a valid collation.") % value)
            self._collate = value
            self._character_set = self.collation[value]

    @property
    def character_set(self):
        """Get the appropriate character set value."""
        if not self._character_set:
            return self.__charset__
        else:
            return self._character_set

    @character_set.setter
    def character_set(self, value):
        """Validate the character set and set it."""
        if not value:
            pass
        elif value not in self.charset:
            raise ValueError(_("'%s' not a valid character set.") % value)
        else:
            self._character_set = value


class DatastoreUser(Base):
    """Represents a datastore user."""

    _HOSTNAME_WILDCARD = '%'

    def __init__(self, name, password, roles=None,
                 deserializing=False, *args, **kwargs):
        self._name = None
        self._password = None
        self._host = None
        self._databases = []
        self._roles = []

        # need only one of: deserializing, name, or (name and password)
        if ((not (bool(deserializing) != bool(name))) or
                (bool(deserializing) and bool(password))):
            raise ValueError(_("Bad args. name: %(name)s, "
                               "password %(pass)s, "
                               "deserializing %(deser)s.")
                             % ({'name': bool(name),
                                 'pass': bool(password),
                                 'deser': bool(deserializing)}))
        if not deserializing:
            if name:
                self.name = name
            if password is not None:
                self.password = password
            if roles is not None:
                self.roles = roles

    @classmethod
    def deserialize_user(cls, value):
        if not cls._validate_dict(value):
            raise ValueError(_("Bad dictionary. Keys: %(keys)s. "
                               "Required: %(reqs)s")
                             % ({'keys': value.keys(),
                                 'reqs': cls._dict_requirements()}))
        user = cls(name=None, password=None, deserializing=True)
        user.deserialize(value)
        return user

    @property
    def name(self):
        return self._name

    @name.setter
    def name(self, value):
        self._validate_user_name(value)
        self._name = value

    @property
    def password(self):
        return self._password

    @password.setter
    def password(self, value):
        if self._is_valid_password(value):
            self._password = value
        else:
            raise ValueError(_("'%s' is not a valid password.") % value)

    @property
    def databases(self):
        return self._databases

    @databases.setter
    def databases(self, value):
        mydb = self._build_database_schema(value)
        self._databases.append(mydb.serialize())

    @property
    def roles(self):
        if not hasattr(self, '_roles'):
            self._roles = []
        return self._roles

    @roles.setter
    def roles(self, value):
        if isinstance(value, list):
            self._roles.extend(value)
        else:
            self._roles.append(value)

    def revoke_role(self, role):
        if role in self.roles:
            self._roles.remove(role)

    @property
    def host(self):
        if self._host is None:
            return self._HOSTNAME_WILDCARD
        return self._host

    @host.setter
    def host(self, value):
        if self._is_valid_host_name(value):
            self._host = value
        else:
            raise ValueError(_("'%s' is not a valid hostname.") % value)

    @abc.abstractmethod
    def _build_database_schema(self, name):
        """Build a schema for this user.
        :type name:              string
        :type character_set:     string
        :type collate:           string
        """

    def _validate_user_name(self, value):
        """Perform validations on a given user name.
        :param value:        Validated user name.
        :type value:         string
        :raises:             ValueError On validation errors.
        """
        if self._max_username_length and (len(value) >
                                          self._max_username_length):
            raise ValueError(_("User name '%(name)s' is too long. "
                               "Max length = %(max_length)d.")
                             % {'name': value,
                                'max_length': self._max_username_length})
        elif not self._is_valid_name(value):
            raise ValueError(_("'%s' is not a valid user name.") % value)

    @abc.abstractproperty
    def _max_username_length(self):
        """Return the maximum valid user name length if any.
        :returns:            Maximum user name length or None if unlimited.
        """

    @abc.abstractmethod
    def _is_valid_name(self, value):
        """Validate a given user name.
        :param value:        User name to be validated.
        :type value:         string
        :returns:            TRUE if valid, FALSE otherwise.
        """

    @abc.abstractmethod
    def _is_valid_host_name(self, value):
        """Validate a given host name.
        :param value:        Host name to be validated.
        :type value:         string
        :returns:            TRUE if valid, FALSE otherwise.
        """

    @abc.abstractmethod
    def _is_valid_password(self, value):
        """Validate a given password.
        :param value:        Password to be validated.
        :type value:         string
        :returns:            TRUE if valid, FALSE otherwise.
        """

    @classmethod
    @abc.abstractmethod
    def _dict_requirements(cls):
        """Get the dictionary requirements for a user created via
        deserialization.
        :returns:           List of required dictionary keys.
        """


class CassandraUser(DatastoreUser):
    """Represents a Cassandra user and its associated properties."""

    def __init__(self, name, password=None, *args, **kwargs):
        super(CassandraUser, self).__init__(name, password, *args, **kwargs)

    def _build_database_schema(self, name):
        return CassandraSchema(name)

    @property
    def _max_username_length(self):
        return 65535

    def _is_valid_name(self, value):
        return True

    def _is_valid_host_name(self, value):
        return True

    def _is_valid_password(self, value):
        return True

    @classmethod
    def _dict_requirements(cls):
        return ['_name']


class MongoDBUser(DatastoreUser):
    """Represents a MongoDB user and its associated properties.
    MongoDB users are identified using their namd and database.
    Trove stores this as <database>.<username>
    """

    def __init__(self, name=None, password=None, *args, **kwargs):
        self._username = None
        self._database = None
        super(MongoDBUser, self).__init__(name, password, *args, **kwargs)

    @property
    def username(self):
        return self._username

    @username.setter
    def username(self, value):
        self._update_name(username=value)

    @property
    def database(self):
        return MongoDBSchema.deserialize_schema(self._database)

    @database.setter
    def database(self, value):
        self._update_name(database=value)

    @property
    def databases(self):
        return [MongoDBSchema(role['database']).serialize()
                for role in self.roles if role['name'] == 'readWrite']

    @databases.setter
    def databases(self, value):
        self.add_access_role(value)

    @property
    def name(self):
        return self._name

    @name.setter
    def name(self, value):
        self._update_name(name=value)

    def _update_name(self, name=None, username=None, database=None):
        """Keep the name, username, and database values in sync."""
        if name:
            (database, username) = self._parse_name(name)
            if not (database and username):
                missing = 'username' if self.database else 'database'
                raise ValueError(_("MongoDB user's name missing %s.")
                                 % missing)
        else:
            if username:
                if not self.database:
                    raise ValueError(_('MongoDB user missing database.'))
                database = self.database.name
            else:  # database
                if not self.username:
                    raise ValueError(_('MongoDB user missing username.'))
                username = self.username
            name = '%s.%s' % (database, username)
        self._name = name
        self._username = username
        self._database = self._build_database_schema(database).serialize()

    def convert_role_mongo_to_trove(self, role):
        return {'name': role['role'], 'database': role['db']}

    def convert_role_trove_to_mongo(self, role):
        if role.get('database'):
            return {'role': role['name'], 'db': role['database']}
        else:
            return {'role': role['name'], 'db': self.database.name}

    @property
    def mongo_roles(self):
        return [self.convert_role_trove_to_mongo(role)
                for role in self.roles]

    @mongo_roles.setter
    def mongo_roles(self, value):
        if isinstance(value, list):
            for mongo_role in value:
                self._roles.append(
                    self.convert_role_mongo_to_trove(mongo_role))
        else:
            self._roles.append(self.convert_role_mongo_to_trove(value))

    def _init_roles(self):
        if '_roles' not in self.__dict__:
            self._roles = []
        if '_databases' in self.__dict__:
            for db in self._databases:
                self.add_access_role(db['_name'])
            del self._databases

    def access_role(self, value):
        return {'name': 'readWrite', 'database': value}

    def add_access_role(self, value):
        """Access is tracked not via the old-style _databases but via _roles,
        so if given access to a database convert it to the readWrite role.
        """
        access_role = self.access_role(value)
        if access_role not in self._roles:
            self._roles.append(access_role)

    @classmethod
    def deserialize_user(cls, value):
        user = super(MongoDBUser, cls).deserialize_user(value)
        user.name = user._name
        user._init_roles()
        return user

    def serialize(self):
        dbs = self.databases
        d = super(MongoDBUser, self).serialize()
        d['_databases'] = dbs
        return d

    def _build_database_schema(self, name):
        return MongoDBSchema(name)

    @staticmethod
    def _parse_name(value):
        """The name will be <database>.<username>, so split it."""
        parts = value.split('.', 1)
        if len(parts) != 2:
            raise exception.BadRequest(_(
                'MongoDB user name "%s" not in <database>.<username> format.'
            ) % value)
        return parts[0], parts[1]

    @property
    def _max_username_length(self):
        return None

    def _is_valid_name(self, value):
        return True

    def _is_valid_host_name(self, value):
        return True

    def _is_valid_password(self, value):
        return True

    @classmethod
    def _dict_requirements(cls):
        return ['_name']


class CouchDBUser(DatastoreUser):
    """Represents a CouchDB user and its associated properties."""

    def __init__(self):
        self._name = None
        self._host = None
        self._password = None
        self._databases = []
        self._ignore_users = cfg.get_ignored_users()

    def _is_valid(self, value):
        return True

    @property
    def name(self):
        return self._name

    @name.setter
    def name(self, value):
        if not self._is_valid(value):
            raise ValueError(_("'%s' is not a valid user name.") % value)
        else:
            self._name = value

    @property
    def password(self):
        return self._password

    @password.setter
    def password(self, value):
        if not self._is_valid(value):
            raise ValueError(_("'%s' is not a valid password.") % value)
        else:
            self._password = value

    @property
    def databases(self):
        return self._databases

    @databases.setter
    def databases(self, value):
        mydb = ValidatedMySQLDatabase()
        mydb.name = value
        self._databases.append(mydb.serialize())

    @property
    def host(self):
        if self._host is None:
            return '%'
        return self._host

    @host.setter
    def host(self, value):
        if not self._is_valid_host_name(value):
            raise ValueError(_("'%s' is not a valid hostname.") % value)
        else:
            self._host = value


class MySQLUser(Base):
    """Represents a MySQL User and its associated properties."""

    not_supported_chars = re.compile("^\s|\s$|'|\"|;|`|,|/|\\\\")

    def __init__(self):
        self._name = None
        self._host = None
        self._password = None
        self._databases = []
        self._roles = []
        self._ignore_users = cfg.get_ignored_users()

    def _is_valid(self, value):
        if (not value or
                self.not_supported_chars.search(value) or
                string.find("%r" % value, "\\") != -1):
            return False
        else:
            return True

    def _is_valid_user_name(self, value):
        if (self._is_valid(value) and
                value.lower() not in self._ignore_users):
            return True
        return False

    def _is_valid_host_name(self, value):
        if value in [None, "%"]:
            # % is MySQL shorthand for "everywhere". Always permitted.
            # Null host defaults to % anyway.
            return True
        if CONF.hostname_require_valid_ip:
            try:
                # '%' works as a MySQL wildcard, but it is not a valid
                # part of an IPAddress
                netaddr.IPAddress(value.replace('%', '1'))
            except (ValueError, netaddr.AddrFormatError):
                return False
            else:
                return True
        else:
            # If it wasn't required, anything else goes.
            return True

    @property
    def name(self):
        return self._name

    @name.setter
    def name(self, value):
        if not self._is_valid_user_name(value):
            raise ValueError(_("'%s' is not a valid user name.") % value)
        elif len(value) > 16:
            raise ValueError(_("User name '%s' is too long. Max length = 16.")
                             % value)
        else:
            self._name = value

    @property
    def password(self):
        return self._password

    @password.setter
    def password(self, value):
        if not self._is_valid(value):
            raise ValueError(_("'%s' is not a valid password.") % value)
        else:
            self._password = value

    @property
    def databases(self):
        return self._databases

    @databases.setter
    def databases(self, value):
        mydb = ValidatedMySQLDatabase()
        mydb.name = value
        self._databases.append(mydb.serialize())

    @property
    def roles(self):
        if not hasattr(self, '_roles'):
            self._roles = []
        return self._roles

    @roles.setter
    def roles(self, value):
        if isinstance(value, list):
            self._roles.extend(value)
        else:
            self._roles.append(value)

    def revoke_role(self, role):
        if role in self.roles:
            self._roles.remove(role)

    @property
    def host(self):
        if self._host is None:
            return '%'
        return self._host

    @host.setter
    def host(self, value):
        if not self._is_valid_host_name(value):
            raise ValueError(_("'%s' is not a valid hostname.") % value)
        else:
            self._host = value


class OracleUser(DatastoreUser):
    """Represents an Oracle user and its associated properties."""

    def __init__(self, name, password=None, *args, **kwargs):
        super(OracleUser, self).__init__(name, password, *args, **kwargs)

    def _build_database_schema(self, name):
        return OracleSchema(name)

    @property
    def _max_username_length(self):
        return 30

    def _is_valid_name(self, value):
        return True

    def _is_valid_host_name(self, value):
        return True

    def _is_valid_password(self, value):
        return True

    @classmethod
    def _dict_requirements(cls):
        return ['_name']


class PostgreSQLUser(DatastoreUser):
    """Represents a PostgreSQL user and its associated properties."""

    def __init__(self, name, password=None, *args, **kwargs):
        super(PostgreSQLUser, self).__init__(name, password, *args, **kwargs)

    def _build_database_schema(self, name):
        return PostgreSQLSchema(name)

    @property
    def _max_username_length(self):
        return 63

    def _is_valid_name(self, value):
        return True

    def _is_valid_host_name(self, value):
        return True

    def _is_valid_password(self, value):
        return True

    @classmethod
    def _dict_requirements(cls):
        return ['_name']


class CouchbaseUser(DatastoreUser):
    """Represents a Couchbase user and its associated properties."""

    MAX_PASSWORD_LEN = 24
    MAX_REPLICA_COUNT = 3
    MIN_BUCKET_RAMSIZE_MB = 100
    VALID_BUCKET_PRIORITY = ['low', 'high']
    VALID_BUCKET_EVICTION_POLICY = ['valueOnly', 'fullEviction']

    def __init__(self, name,
                 password=None,
                 roles=None,
                 bucket_ramsize_mb=None,
                 bucket_replica_count=None,
                 enable_index_replica=None,
                 bucket_eviction_policy=None,
                 bucket_priority=None,
                 used_ram_mb=None,
                 bucket_port=None, * args, **kwargs):
        super(CouchbaseUser, self).__init__(name, password, roles=roles,
                                            *args, **kwargs)
        self._bucket_ramsize_mb = None
        self._bucket_replica_count = None
        self._enable_index_replica = None
        self._bucket_eviction_policy = None
        self._bucket_priority = None
        self._used_ram_mb = used_ram_mb
        self._bucket_port = bucket_port

        if bucket_ramsize_mb is not None:
            self.bucket_ramsize_mb = bucket_ramsize_mb
        if bucket_replica_count is not None:
            self.bucket_replica_count = bucket_replica_count
        if enable_index_replica is not None:
            self.enable_index_replica = enable_index_replica
        if bucket_eviction_policy is not None:
            self.bucket_eviction_policy = bucket_eviction_policy
        if bucket_priority is not None:
            self.bucket_priority = bucket_priority

    def _build_database_schema(self, name):
        return CouchbaseSchema(name)

    @property
    def _max_username_length(self):
        return 24

    def _is_valid_name(self, value):
        return True

    def _is_valid_host_name(self, value):
        return True

    def _is_valid_password(self, value):
        length = len(value)
        return length > 5 and length <= self.MAX_PASSWORD_LEN

    @property
    def bucket_ramsize_mb(self):
        return self._bucket_ramsize_mb

    @bucket_ramsize_mb.setter
    def bucket_ramsize_mb(self, value):
        if not self._is_integer(value, self.MIN_BUCKET_RAMSIZE_MB, None):
            raise ValueError(
                _("Bucket RAM quota cannot be less than 100MB."))
        self._bucket_ramsize_mb = value

    def _is_non_negative_int(self, value):
        return self._is_integer(value, 0)

    def _is_integer(self, value, lower_bound=None, upper_bound=None):
        try:
            if str(value).isdigit():
                int_value = int(value)
                return ((lower_bound is None or
                         int_value >= lower_bound) and
                        (upper_bound is None or
                         int_value <= upper_bound))
        except (ValueError, TypeError):
            pass

        return False

    @property
    def bucket_replica_count(self):
        return self._bucket_replica_count

    @bucket_replica_count.setter
    def bucket_replica_count(self, value):
        if not self._is_integer(value, 0, self.MAX_REPLICA_COUNT):
            raise ValueError(
                _("Replica count must be an integer between 0 and 3."))
        self._bucket_replica_count = value

    @property
    def enable_index_replica(self):
        return self._enable_index_replica

    @enable_index_replica.setter
    def enable_index_replica(self, value):
        if not self._is_non_negative_int(value):
            raise ValueError(
                _("Index replica value must be '1' (yes) or '0' (no)."))
        self._enable_index_replica = value

    @property
    def bucket_eviction_policy(self):
        return self._bucket_eviction_policy

    @bucket_eviction_policy.setter
    def bucket_eviction_policy(self, value):
        if value not in self.VALID_BUCKET_EVICTION_POLICY:
            raise ValueError(_("Bucket eviction policy must be one of: '%s'")
                             % "', '".join(self.VALID_BUCKET_EVICTION_POLICY))
        self._bucket_eviction_policy = value

    @property
    def bucket_priority(self):
        return self._bucket_priority

    @bucket_priority.setter
    def bucket_priority(self, value):
        if value not in self.VALID_BUCKET_PRIORITY:
            raise ValueError(_("Bucket priority must be one of: '%s'")
                             % "', '".join(self.VALID_BUCKET_PRIORITY))
        self._bucket_priority = value

    @property
    def used_ram_mb(self):
        return self._used_ram_mb

    @property
    def bucket_port(self):
        return self._bucket_port

    @classmethod
    def _dict_requirements(cls):
        return ['_name']


# TODO(pmalik): Datastores should be using their own user.
# Not this one which is just a MySQL user.
class RootUser(MySQLUser):
    """Overrides _ignore_users from the MySQLUser class."""

    def __init__(self):
        self._ignore_users = []


class MySQLRootUser(MySQLUser):
    """Represents the MySQL root user."""

    _ignore_users = []

    def __init__(self, password=None):
        super(MySQLRootUser, self).__init__()
        self._name = "root"
        self._host = "%"
        if password is None:
            self._password = utils.generate_random_password()
        else:
            self._password = password
        self.roles = {'name': 'root'}


class PostgreSQLRootUser(PostgreSQLUser):
    """Represents the PostgreSQL default superuser."""

    def __init__(self, password=None, *args, **kwargs):
        if password is None:
            password = utils.generate_random_password()
        super(PostgreSQLRootUser, self).__init__("postgres", password=password,
                                                 *args, **kwargs)
        self.roles = {'name': 'root'}


class EnterpriseDBRootUser(PostgreSQLUser):
    """Represents the EnterpriseDB default superuser."""

    def __init__(self, password=None, *args, **kwargs):
        if password is None:
            password = utils.generate_random_password()
        super(EnterpriseDBRootUser, self).__init__(
            "enterprisedb", password=password, *args, **kwargs)
        self.roles = {'name': 'root'}


class CassandraRootUser(CassandraUser):
    """Represents the Cassandra default superuser."""

    def __init__(self, password=None, *args, **kwargs):
        if password is None:
            password = utils.generate_random_password()
        super(CassandraRootUser, self).__init__("cassandra", password=password,
                                                *args, **kwargs)
        self.roles = {'name': 'root'}


class CouchbaseRootUser(CouchbaseUser):
    """Represents the Couchbase default superuser."""

    def __init__(self, password=None, *args, **kwargs):
        if password is None:
            pwd_len = min(self.MAX_PASSWORD_LEN, CONF.default_password_length)
            password = utils.generate_random_password(pwd_len)

        # TODO(pmalik): Name should really be 'Administrator' instead.
        super(CouchbaseRootUser, self).__init__("root", password=password,
                                                *args, **kwargs)
        self.roles = {'name': 'root'}
