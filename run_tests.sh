#!/bin/bash

# Copyright 2017 Verily Life Sciences Inc. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

set -o nounset
set -o errexit

# Check that required variables are explicitly set.
GOOGLE_APPLICATION_CREDENTIALS="$GOOGLE_APPLICATION_CREDENTIALS"
TEST_PROJECT="$TEST_PROJECT"

set -o xtrace

virtualenv --system-site-packages virtualTestEnv
# Work around virtual env error 'PS1: unbound variable'
set +o nounset
source virtualTestEnv/bin/activate
set -o nounset

pip install --upgrade pip
pip install --upgrade setuptools
python setup.py install
# Work around 'error: grpcio 1.7.0 is installed but grpcio<1.6dev,>=1.0.2 is required by set(['google-gax'])'
# pip knows how to install google-cloud properly but python setup.py install does not.
pip install --upgrade google-cloud==0.27.0 --ignore-installed

# Check the version of sqlite3 installed.
python -c "import sqlite3; print(sqlite3.sqlite_version)"

python -m verily.bigquery_wrapper.bq_test
python -m verily.bigquery_wrapper.mock_bq_test
