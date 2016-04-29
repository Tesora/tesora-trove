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
"""I totally stole most of this from melange, thx guys!!!"""

import collections
import datetime
import inspect
import os
import random
import shutil
import string
import time
import types
import uuid

from eventlet.timeout import Timeout
import jinja2
from oslo_concurrency import processutils
from oslo_log import log as logging
from oslo_service import loopingcall
from oslo_utils import importutils
from oslo_utils import strutils
from oslo_utils import timeutils
from passlib import utils as passlib_utils
import six.moves.urllib.parse as urlparse

from trove.common import cfg
from trove.common import exception
from trove.common.i18n import _


CONF = cfg.CONF
LOG = logging.getLogger(__name__)
import_class = importutils.import_class
import_object = importutils.import_object
import_module = importutils.import_module
bool_from_string = strutils.bool_from_string
execute = processutils.execute
isotime = timeutils.isotime


def build_jinja_environment():
    env = jinja2.Environment(loader=jinja2.ChoiceLoader([
        jinja2.FileSystemLoader(CONF.template_path),
        jinja2.PackageLoader("trove", "templates")
    ]))
    # Add some basic operation not built-in.
    env.globals['max'] = max
    env.globals['min'] = min
    return env

ENV = build_jinja_environment()


def create_method_args_string(*args, **kwargs):
    """Returns a string representation of args and keyword args.

    I.e. for args=1,2,3 and kwargs={'a':4, 'b':5} you'd get: "1,2,3,a=4,b=5"
    """
    # While %s turns a var into a string but in some rare cases explicit
    # repr() is less likely to raise an exception.
    arg_strs = [repr(arg) for arg in args]
    arg_strs += ['%s=%s' % (repr(key), repr(value))
                 for (key, value) in kwargs.items()]
    return ', '.join(arg_strs)


def stringify_keys(dictionary):
    if dictionary is None:
        return None
    return {str(key): value for key, value in dictionary.iteritems()}


def exclude(key_values, *exclude_keys):
    if key_values is None:
        return None
    return {key: value for key, value in key_values.iteritems()
            if key not in exclude_keys}


def generate_uuid():
    return str(uuid.uuid4())


def utcnow():
    return datetime.datetime.utcnow()


def raise_if_process_errored(process, exception):
    try:
        err = process.stderr.read()
        if err:
            raise exception(err)
    except OSError:
        pass


def clean_out(folder):
    for root, dirs, files in os.walk(folder):
        for f in files:
            os.unlink(os.path.join(root, f))
        for d in dirs:
            shutil.rmtree(os.path.join(root, d))


class cached_property(object):
    """A decorator that converts a function into a lazy property.

    Taken from : https://github.com/nshah/python-memoize
    The function wrapped is called the first time to retrieve the result
    and than that calculated result is used the next time you access
    the value:

        class Foo(object):

            @cached_property
            def bar(self):
                # calculate something important here
                return 42

    """

    def __init__(self, func, name=None, doc=None):
        self.func = func
        self.__name__ = name or func.__name__
        self.__doc__ = doc or func.__doc__

    def __get__(self, obj, type=None):
        if obj is None:
            return self
        value = self.func(obj)
        setattr(obj, self.__name__, value)
        return value


class MethodInspector(object):

    def __init__(self, func):
        self._func = func

    @cached_property
    def required_args(self):
        return self.args[0:self.required_args_count]

    @cached_property
    def optional_args(self):
        keys = self.args[self.required_args_count: len(self.args)]
        return zip(keys, self.defaults)

    @cached_property
    def defaults(self):
        return self.argspec.defaults or ()

    @cached_property
    def required_args_count(self):
        return len(self.args) - len(self.defaults)

    @cached_property
    def args(self):
        args = self.argspec.args
        if inspect.ismethod(self._func):
            args.pop(0)
        return args

    @cached_property
    def argspec(self):
        return inspect.getargspec(self._func)

    def __str__(self):
        optionals = ["[{0}=<{0}>]".format(k) for k, v in self.optional_args]
        required = ["{0}=<{0}>".format(arg) for arg in self.required_args]
        args_str = ' '.join(required + optionals)
        return "%s %s" % (self._func.__name__, args_str)


def build_polling_task(retriever, condition=lambda value: value,
                       sleep_time=1, time_out=None):
    start_time = time.time()

    def poll_and_check():
        obj = retriever()
        if condition(obj):
            raise loopingcall.LoopingCallDone(retvalue=obj)
        if time_out is not None and time.time() - start_time > time_out:
            raise exception.PollTimeOut

    return loopingcall.FixedIntervalLoopingCall(
        f=poll_and_check).start(sleep_time, True)


def poll_until(retriever, condition=lambda value: value,
               sleep_time=1, time_out=None):
    """Retrieves object until it passes condition, then returns it.

    If time_out_limit is passed in, PollTimeOut will be raised once that
    amount of time is eclipsed.

    """

    return build_polling_task(retriever, condition=condition,
                              sleep_time=sleep_time, time_out=time_out).wait()


# Copied from nova.api.openstack.common in the old code.
def get_id_from_href(href):
    """Return the id or uuid portion of a url.

    Given: 'http://www.foo.com/bar/123?q=4'
    Returns: '123'

    Given: 'http://www.foo.com/bar/abc123?q=4'
    Returns: 'abc123'

    """
    return urlparse.urlsplit("%s" % href).path.split('/')[-1]


def execute_with_timeout(*args, **kwargs):
    time = kwargs.pop('timeout', 30)
    log_output_on_error = kwargs.pop('log_output_on_error', False)

    timeout = Timeout(time)
    try:
        return execute(*args, **kwargs)
    except exception.ProcessExecutionError as e:
        if log_output_on_error:
            LOG.error(
                _("Command '%(cmd)s' failed. %(description)s "
                  "Exit code: %(exit_code)s\nstderr: %(stderr)s\n"
                  "stdout: %(stdout)s") %
                {'cmd': e.cmd, 'description': e.description or '',
                 'exit_code': e.exit_code, 'stderr': e.stderr,
                 'stdout': e.stdout})
        raise
    except Timeout as t:
        if t is not timeout:
            LOG.error(_("Got a timeout but not the one expected."))
            raise
        else:
            msg = (_("Time out after waiting "
                     "%(time)s seconds when running proc: %(args)s"
                     " %(kwargs)s.") % {'time': time, 'args': args,
                                        'kwargs': kwargs})
            LOG.error(msg)
            raise exception.ProcessExecutionError(msg)
    finally:
        timeout.cancel()


def correct_id_with_req(id, request):
    # Due to a shortcoming with the way Trove uses routes.mapper,
    # URL entities right of the last slash that contain at least
    # one . are routed to our service without that suffix, as
    # it was interpreted as a filetype This method looks at the
    # request, and if applicable, reattaches the suffix to the id.
    routing_args = request.environ.get('wsgiorg.routing_args', [])
    for routing_arg in routing_args:
        try:
            found = routing_arg.get('format', '')
            if found and found not in CONF.expected_filetype_suffixes:
                return "%s.%s" % (id, found)
        except (AttributeError, KeyError):
            # Not the relevant routing_args entry.
            pass
    return id


def generate_random_password(password_length=None, datastore=None,
                             alpha_first=True):
    """
    Generate and return a random password string.

    :param password_length: Length of password to create. If value is None,
    the default_password_length set in the configuration will be used.
    :param datastore: Datastore name to generate random password for. If
    value is None, default values set in the configuration will be used.
    :param alpha_first: Specify whether the generated password should begin
    with an alphabet.
    :return: A randomly generated password string
    """
    lower_case = 'abcdefghjkmnpqrstuvwxyz'
    upper_case = 'ABCDEFGHJKMNPQRTUVWXYZ'
    numbers = '2346789'
    min_lower_case = cfg.get_configuration_property(
        'password_min_lower_case', datastore)
    min_upper_case = cfg.get_configuration_property(
        'password_min_upper_case', datastore)
    min_numbers = cfg.get_configuration_property(
        'password_min_numbers', datastore)
    min_special_chars = cfg.get_configuration_property(
        'password_min_special_chars', datastore)
    special_chars = cfg.get_configuration_property(
        'password_special_charset', datastore)
    password_length = (
        password_length or
        cfg.get_configuration_property('default_password_length')
    )
    choices = [lower_case, upper_case, numbers, special_chars]
    mins = [min_lower_case, min_upper_case, min_numbers, min_special_chars]
    all_choices = (lower_case + upper_case + numbers + special_chars)

    password = bytearray()
    if password_length < 1:
        raise RuntimeError("Length cannot be less than 1")
    total_min = 0
    for index, value in enumerate(mins):
        total_min += value
        if value:
            password.extend(passlib_utils.generate_password(
                size=value, charset=choices[index]).encode('utf-8'))
        if index == 1:
            random.shuffle(password)
    remainder = password_length - total_min
    if total_min > password_length:
        raise RuntimeError("Length cannot be less than %d" % total_min)
    if remainder > 0:
        password.extend(passlib_utils.generate_password(
            size=password_length - total_min, charset=all_choices)
            .encode('utf-8'))
    if alpha_first:
        last_part = bytearray(password[1:])
        random.shuffle(last_part)
        password = password[:1]
        password.extend(last_part)
    else:
        random.shuffle(password)

    try:
        return password.decode('utf-8')
    except AttributeError:
        return str(password)


def generate_random_string(length,
                           chars=string.ascii_uppercase + string.digits):
    return ''.join(random.choice(chars) for _ in range(length))


def try_recover(func):
    def _decorator(*args, **kwargs):
        recover_func = kwargs.pop("recover_func", None)
        try:
            func(*args, **kwargs)
        except Exception:
            if recover_func is not None:
                recover_func(func)
            else:
                LOG.debug("No recovery method defined for %(func)s" % {
                          'func': func.__name__})
            raise
    return _decorator


def gen_ports(portstr):
    from_port, sep, to_port = portstr.partition('-')
    if not (to_port and from_port):
        if not sep:
            to_port = from_port
    if int(from_port) > int(to_port):
        raise ValueError
    return from_port, to_port


def unpack_singleton(container):
    """Unpack singleton collections.

    Check whether a given collection is a singleton (has exactly one element)
    and unpack it if that is the case.
    Return the original collection otherwise.
    """
    if is_collection(container) and len(container) == 1:
        return unpack_singleton(container[0])

    return container


def is_collection(item):
    """Return True is a given item is an iterable collection, but not a string.
    """
    return (isinstance(item, collections.Iterable) and
            not isinstance(item, types.StringTypes))
