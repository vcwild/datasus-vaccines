import pandas as pd
from os import listdir


def filter_df(data, iterator, year=None):
  """Create a Pipeline filter for the selected Datasets

  Args:
      data (list): Iterates over a list of Pandas DataFrames
      iterator (int): Index of the iterated Dataframe
      year (int, optional): Creates a new feature for the iterated Dataset. Defaults to None.

  Returns:
      pd.Dataframe: Returns the unpacked Pandas DataFrame.
  """
  df = data[iterator]
  df.columns = df.iloc[0]
  df = df.iloc[1:]
  df = df.reset_index()
  df = df.rename(columns={"index":"Estado"})
  if year is not None:
    df['Ano'] = year

  return df

filepath = './datasets/'

datasets = sorted([x for x in listdir('./datasets')])

df = [pd.read_csv(filepath + dataset, encoding='utf8', sep=';', decimal=',') for dataset in datasets]
df_t = [x.transpose() for x in df]

imuno = filter_df(df_t, 0)
imuno = imuno.rename(columns={"Estado": "Ano"})
df_2015 = filter_df(df_t, 1, 2015)
df_2016 = filter_df(df_t, 2, 2016)
df_2017 = filter_df(df_t, 3, 2017)
df_2018 = filter_df(df_t, 4, 2018)
df_2019 = filter_df(df_t, 5, 2019)

bases = [df_2015, df_2016, df_2017, df_2018, df_2019]
result = pd.concat(bases)

print(result.groupby(['Estado', 'Ano']).sum())