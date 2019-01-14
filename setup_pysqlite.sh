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

# This script installs pysqlite using a specific underlying sqlite version.
#
# The pysqlite library is a python wrapper around the sqlite library, which is written in C. By
# default, pysqlite uses an old sqlite version. According to the owner of the pysqlite library, this
# is the canonical way to upgrade the sqlite version (see
# https://github.com/ghaering/pysqlite/issues/95).

# Clone pysqlite from github. This creates a directory inside the current working directory called
# "pysqlite".
git clone https://github.com/ghaering/pysqlite

# Download the sqlite library, version 3.25.0, into this directory.
cd pysqlite
wget http://sqlite.org/2018/sqlite-amalgamation-3250000.zip
unzip sqlite-amalgamation-3250000.zip
mv -i sqlite-amalgamation-3250000/* .

# Install pysqlite from the cloned repository, using the downloaded sqlite version. This will
# install pysqlite2 into /usr/local/lib/python2.7/dist_packages.
python2.7 setup.py build_static install

# Clean up the cloned repository.
rm -rf pysqlite
