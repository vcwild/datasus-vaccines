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

__name__ = "__main__"
  