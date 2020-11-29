from os import listdir
from pipeline import filter_df
import pandas as pd

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