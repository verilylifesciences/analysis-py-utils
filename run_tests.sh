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
pip install .

# Check the version of sqlite3 installed.
python -c "import sqlite3; print(sqlite3.sqlite_version)"

# Install pyspark
CUR_DIR=$(pwd)
cd /opt
curl http://archive.apache.org/dist/spark/spark-2.2.0/spark-2.2.0-bin-hadoop2.7.tgz | tar zxf -
# According to pip, the pyspark version list is (2.1.2, 2.2.0.post0, 2.2.1, 2.3.0). No idea why the
# post0 at the end of 2.2.0, but it appears to be the right version.
pip install pyspark==2.2.0.post0
export SPARK_HOME=/opt/spark-2.2.0-bin-hadoop2.7
export PYSPARK_PYTHON=/usr/bin/python
cd $CUR_DIR

# Check the version of pyspark installed
spark-submit --version

python -m verily.bigquery_wrapper.bq_test
python -m verily.bigquery_wrapper.mock_bq_test
python -m verily.bigquery_wrapper.mock_bq_sqlite_test