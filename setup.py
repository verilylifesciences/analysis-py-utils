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
"""Package configuration."""

from setuptools import find_packages
from setuptools import setup

REQUIRED_PACKAGES = ['pandas',
                     'google-api-core==1.5.1',
                     'google-auth==1.4.1',
                     'google-cloud-bigquery==1.7.0',
                     'google-cloud-storage==1.10.0',
                     'pysqlite>=2.8.3',
                     'ddt',
                     'typing']

setup(
    name='analysis-py-utils',
    version='0.4.0',
    license='Apache 2.0',
    author='Verily Life Sciences',
    url='https://github.com/verilylifesciences/analysis-py-utils',
    install_requires=REQUIRED_PACKAGES,
    packages=find_packages(),
    include_package_data=True,
    description='Python utilities for data analysis.',
    requires=[])
