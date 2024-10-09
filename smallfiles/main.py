import pandas as pd
import numpy as np


def main():
    df1 = pd.DataFrame(np.array([["Alice", 29], ["Bob", 31], ["Cathy", 22], ["David", 35]]),
                       columns=['name', 'age'])
    df2 = pd.DataFrame(np.array([["Alice", 40], ["Bob", 31], ["Catherine", 22], ["David", 35]]),
                       columns=['name', 'age'])
    common = df1.merge(df2,on=['name', 'age'])
    print(df1[(~df1.name.isin(common.name))&(~df1.age.isin(common.age))])

if __name__ == "__main__":
    main()
