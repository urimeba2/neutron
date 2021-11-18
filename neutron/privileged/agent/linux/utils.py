# Copyright 2020 Red Hat, Inc.
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

import os
import re
from sys import stderr

from eventlet.green import subprocess
from neutron_lib.utils import helpers
from oslo_concurrency import processutils
from oslo_utils import fileutils

from neutron import privileged
from neutron.agent.linux.ip_lib import LOG


NETSTAT_PIDS_REGEX = re.compile(r'.* (?P<pid>\d{2,6})/.*')


@privileged.default.entrypoint
def find_listen_pids_namespace(namespace):
    return _find_listen_pids_namespace(namespace)


def _find_listen_pids_namespace(namespace):
    """Retrieve a list of pids of listening processes within the given netns

    This method is implemented separately to allow unit testing.
    """
    pids = set()
    cmd = ['ip', 'netns', 'exec', namespace, 'netstat', '-nlp']
    output = processutils.execute(*cmd)
    for line in output[0].splitlines():
        m = NETSTAT_PIDS_REGEX.match(line)
        if m:
            pids.add(m.group('pid'))
    return list(pids)


@privileged.default.entrypoint
def delete_if_exists(path, remove=os.unlink):
    fileutils.delete_if_exists(path, remove=remove)


@privileged.default.entrypoint
def execute_process(cmd, _process_input, addl_env):
    LOG.debug('Inside execute_process with cmd: {cmd} , _process_input: {process_input} and addl_env: {addl_env}'.format(
        cmd=cmd,
        process_input=_process_input,
        addl_env=addl_env
    ))
    obj, cmd = _create_process(cmd, addl_env=addl_env)
    LOG.debug('Creating process inside execute_process with cmd: {cmd} and object: {obj}'.format(
        cmd=cmd,
        obj=obj
    ))
    _stdout, _stderr = obj.communicate(_process_input)
    
    returncode = obj.returncode

    LOG.debug('Creating process inside execute_process with _stdout: {stdout} and _stderr: {stderr} and returncode: {returncode}'.format(
        stdout=_stdout,
        stderr=_stderr, 
        returncode=returncode
    ))
    obj.stdin.close()
    _stdout = helpers.safe_decode_utf8(_stdout)
    _stderr = helpers.safe_decode_utf8(_stderr)
    return _stdout, _stderr, returncode


def _addl_env_args(addl_env):
    """Build arguments for adding additional environment vars with env"""

    # NOTE (twilson) If using rootwrap, an EnvFilter should be set up for the
    # command instead of a CommandFilter.
    if addl_env is None:
        return []
    return ['env'] + ['%s=%s' % pair for pair in addl_env.items()]


def _create_process(cmd, addl_env=None):
    """Create a process object for the given command.

    The return value will be a tuple of the process object and the
    list of command arguments used to create it.
    """
    cmd = list(map(str, _addl_env_args(addl_env) + list(cmd)))
    obj = subprocess.Popen(cmd, shell=False, stdin=subprocess.PIPE,
                           stdout=subprocess.PIPE, stderr=subprocess.PIPE)

    
    return obj, cmd
