# Copyright 2020 ABSA Group Limited
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

import pandas as pd
import numpy as np


def main():
    df1 = pd.DataFrame(np.array([["Alice", 29], ["Bob", 31], ["Cathy", 22], ["David", 35]]), columns=["name", "age"])
    df2 = pd.DataFrame(np.array([["Alice", 40], ["Bob", 31], ["Catherine", 22], ["David", 35]]), columns=["name", "age"])
    common = df1.merge(df2, on=["name", "age"])
    print(df1[(~df1.name.isin(common.name)) & (~df1.age.isin(common.age))])


if __name__ == "__main__":
    main()
