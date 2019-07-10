#!/bin/bash

# Copyright 2019 Verily Life Sciences Inc. All Rights Reserved.
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
set -o xtrace

for version in 2 3; do

  virtualenv --system-site-packages -p python$version virtualTestEnv
  # Work around virtual env error 'PS1: unbound variable'
  set +o nounset
  source virtualTestEnv/bin/activate
  set -o nounset

  pip$version install --upgrade pip
  pip$version install --upgrade setuptools
  pip$version install .

  # Check the version of sqlite3 installed.
  if [ "$version" = "2" ]; then
    python$version -c "from pysqlite2 import dbapi2 as sqlite3; print(sqlite3.sqlite_version)"
  else
    python$version -c "import sqlite3; print(sqlite3.sqlite_version)"
  fi


  if [[ -v GOOGLE_APPLICATION_CREDENTIALS ]] && [[ -v TEST_PROJECT ]];
  then python$version -m verily.bigquery_wrapper.bq_test
  else echo "Skipping Bigquery tests. To run, set GOOGLE_APPLICATION_CREDENTIALS and TEST_PROJECT"
  fi

  python$version -m verily.bigquery_wrapper.mock_bq_test
  python$version -m verily.query_kit.template_test
  python$version -m verily.query_kit.features_test

  deactivate
done
