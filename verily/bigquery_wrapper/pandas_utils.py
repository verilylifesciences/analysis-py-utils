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
"""This file contains utilities that are useful for working with pandas.
"""

import copy
import numpy as np
import pandas as pd


def safe_read_csv(data_file, dtype_map):
    """ This reads in a csv and handles the case of "NAN" ints by leaving the bad rows as NAN

        Args:
            data_file: A string describing the file with the data to use in testing
            dtype_map: The type map for the file, same as read_csv's dtype.
        Returns:
            A pandas dataframe
    """
    try:
        df = pd.read_csv(data_file, index_col=None, dtype=dtype_map)
    except ValueError:
        dtype_map_tmp = copy.deepcopy(dtype_map)
        types_to_change = []
        # Ints don't support nulls, so load using float64, and cast the rows that
        # aren't NAN as ints.
        for key in dtype_map_tmp:
            if dtype_map_tmp[key] == np.int64:
                dtype_map_tmp[key] = np.float64
                types_to_change.append(key)
        df = pd.read_csv(data_file, index_col=None, dtype=dtype_map_tmp)
        for col in types_to_change:
            # The dtype map can include columns that are not found in the file, this should not
            # cause this to fail.
            if col in set(list(df)):
                df['unsafe'] = pd.isnull(df[col])
                df[col] = df.apply(
                    lambda r: r[col] if r['unsafe'] else int(r[col]), axis=1)
                df = df.drop('unsafe', 1)
    return df
