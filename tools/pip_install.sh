#!/bin/sh

BRANCH_NAME=${BRANCH_NAME:-master}
BRANCH_NAME=${ZUUL_BRANCH:-$BRANCH_NAME}

set -e

install_cmd="pip install"
uninstall_cmd="pip uninstall -y"

# install all pip libraries from pypi first
$install_cmd -U $*

# remove the python-troveclient from pypi
$uninstall_cmd python-troveclient

# install python-troveclient from source
PYTHON_TROVECLIENT_PIP_LOCATION="git://github.com/Tesora/tesora-python-troveclient.git@$BRANCH_NAME#egg=python-troveclient"
$install_cmd -U -e ${PYTHON_TROVECLIENT_PIP_LOCATION}

exit $?
