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
set -o xtrace

virtualenv --system-site-packages virtualTestEnv
# Work around virtual env error 'PS1: unbound variable'
set +o nounset
source virtualTestEnv/bin/activate
set -o nounset

pip install --upgrade pip
pip install --upgrade setuptools
pip install .

# Check the version of sqlite3 installed.
python -c "import sqlite3; print(sqlite3.sqlite_version)"

if [[ -v GOOGLE_APPLICATION_CREDENTIALS ]] && [[ -v TEST_PROJECT ]];
then python -m verily.bigquery_wrapper.bq_test
else echo "Skipping Bigquery tests. To run, set GOOGLE_APPLICATION_CREDENTIALS and TEST_PROJECT"
fi

python -m verily.bigquery_wrapper.mock_bq_test
python -m verily.query_kit.template_test
python -m verily.query_kit.features_test
