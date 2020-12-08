# DataSUS Vaccines Pipeline Cleansing


```python
import pandas as pd
from os import listdir
```


```python
datasets = sorted([x for x in listdir('./datasets')])
datasets
```




    ['cv_ano_imuno.csv',
     'cv_uf_2015.csv',
     'cv_uf_2016.csv',
     'cv_uf_2017.csv',
     'cv_uf_2018.csv',
     'cv_uf_2019.csv']




```python
def filter_df(data, iterator, year=None):
    df = data[iterator]
    df.columns = df.iloc[0]
    df = df.iloc[1:]
    df = df.reset_index()
    df = df.rename(columns={"index":"Estado"})
    if year is not None:
        df['Ano'] = year

    return df
```


```python
filepath = './datasets/'

df = [pd.read_csv(filepath + dataset, encoding='utf8', sep=';', decimal=',') for dataset in datasets]

df_t = [x.transpose() for x in df]

imuno = filter_df(df_t, 0)
imuno = imuno.rename(columns={"Estado": "Ano"})
df_2015 = filter_df(df_t, 1, 2015)
df_2016 = filter_df(df_t, 2, 2016)
df_2017 = filter_df(df_t, 3, 2017)
df_2018 = filter_df(df_t, 4, 2018)
df_2019 = filter_df(df_t, 5, 2019)

```


```python
df_2015.head(2)
```




<div>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th>Imuno</th>
      <th>Estado</th>
      <th>BCG</th>
      <th>Hepatite B  em crianças até 30 dias</th>
      <th>Rotavírus Humano</th>
      <th>Meningococo C</th>
      <th>Hepatite B</th>
      <th>Penta</th>
      <th>Pneumocócica</th>
      <th>Poliomielite</th>
      <th>Febre Amarela</th>
      <th>...</th>
      <th>Tríplice Viral  D2</th>
      <th>Tetra Viral(SRC+VZ)</th>
      <th>DTP</th>
      <th>Tríplice Bacteriana(DTP)(1º ref)</th>
      <th>Dupla adulto e tríplice acelular gestante</th>
      <th>dTpa gestante</th>
      <th>Tetravalente (DTP/Hib) (TETRA)</th>
      <th>Ignorado</th>
      <th>Total</th>
      <th>Ano</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>RO</td>
      <td>110.77</td>
      <td>104.78</td>
      <td>103.95</td>
      <td>104.02</td>
      <td>106.35</td>
      <td>104.54</td>
      <td>104.68</td>
      <td>105.44</td>
      <td>106.06</td>
      <td>...</td>
      <td>94.61</td>
      <td>94.63</td>
      <td>104.63</td>
      <td>95.82</td>
      <td>73.92</td>
      <td>64.65</td>
      <td>103.36</td>
      <td>229.39</td>
      <td>111.27</td>
      <td>2015</td>
    </tr>
    <tr>
      <th>1</th>
      <td>AC</td>
      <td>105.9</td>
      <td>69.63</td>
      <td>82.5</td>
      <td>88.64</td>
      <td>82.62</td>
      <td>81.24</td>
      <td>72.48</td>
      <td>82.74</td>
      <td>66.67</td>
      <td>...</td>
      <td>51.69</td>
      <td>49.3</td>
      <td>81.3</td>
      <td>62.7</td>
      <td>17.13</td>
      <td>12.08</td>
      <td>83.71</td>
      <td>158.09</td>
      <td>75.54</td>
      <td>2015</td>
    </tr>
  </tbody>
</table>
<p>2 rows × 25 columns</p>
</div>




```python
bases = [df_2015, df_2016, df_2017, df_2018, df_2019]
result = pd.concat(bases)
result.columns
```




    Index(['Estado', 'BCG', 'Hepatite B  em crianças até 30 dias',
           'Rotavírus Humano', 'Meningococo C', 'Hepatite B', 'Penta',
           'Pneumocócica', 'Poliomielite', 'Febre Amarela', 'Hepatite A',
           'Pneumocócica(1º ref)', 'Meningococo C (1º ref)',
           'Poliomielite(1º ref)', 'Tríplice Viral  D1', 'Tríplice Viral  D2',
           'Tetra Viral(SRC+VZ)', 'DTP', 'Tríplice Bacteriana(DTP)(1º ref)',
           'Dupla adulto e tríplice acelular gestante', 'dTpa gestante',
           'Tetravalente (DTP/Hib) (TETRA)', 'Ignorado', 'Total', 'Ano',
           'DTP REF (4 e 6 anos)', 'Poliomielite 4 anos'],
          dtype='object')




```python
result.groupby(['Estado', 'Ano']).sum()
```




<div>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th></th>
      <th>BCG</th>
      <th>Hepatite B  em crianças até 30 dias</th>
      <th>Rotavírus Humano</th>
      <th>Meningococo C</th>
      <th>Hepatite B</th>
      <th>Penta</th>
      <th>Pneumocócica</th>
      <th>Poliomielite</th>
      <th>Febre Amarela</th>
      <th>Hepatite A</th>
      <th>...</th>
      <th>Tetra Viral(SRC+VZ)</th>
      <th>DTP</th>
      <th>Tríplice Bacteriana(DTP)(1º ref)</th>
      <th>Dupla adulto e tríplice acelular gestante</th>
      <th>dTpa gestante</th>
      <th>Tetravalente (DTP/Hib) (TETRA)</th>
      <th>Ignorado</th>
      <th>Total</th>
      <th>DTP REF (4 e 6 anos)</th>
      <th>Poliomielite 4 anos</th>
    </tr>
    <tr>
      <th>Estado</th>
      <th>Ano</th>
      <th></th>
      <th></th>
      <th></th>
      <th></th>
      <th></th>
      <th></th>
      <th></th>
      <th></th>
      <th></th>
      <th></th>
      <th></th>
      <th></th>
      <th></th>
      <th></th>
      <th></th>
      <th></th>
      <th></th>
      <th></th>
      <th></th>
      <th></th>
      <th></th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th rowspan="5" valign="top">Total</th>
      <th>2015</th>
      <td>105.08</td>
      <td>90.93</td>
      <td>95.35</td>
      <td>98.19</td>
      <td>97.74</td>
      <td>96.30</td>
      <td>94.23</td>
      <td>98.29</td>
      <td>46.31</td>
      <td>97.07</td>
      <td>...</td>
      <td>77.37</td>
      <td>96.90</td>
      <td>85.78</td>
      <td>45.57</td>
      <td>44.97</td>
      <td>95.49</td>
      <td>196.58</td>
      <td>95.07</td>
      <td>0.00</td>
      <td>0.00</td>
    </tr>
    <tr>
      <th>2016</th>
      <td>95.55</td>
      <td>81.75</td>
      <td>88.98</td>
      <td>91.68</td>
      <td>105.19</td>
      <td>89.27</td>
      <td>95.00</td>
      <td>84.43</td>
      <td>44.59</td>
      <td>71.58</td>
      <td>...</td>
      <td>79.04</td>
      <td>89.53</td>
      <td>64.28</td>
      <td>31.53</td>
      <td>33.81</td>
      <td>5.21</td>
      <td>16.44</td>
      <td>50.44</td>
      <td>2.73</td>
      <td>0.00</td>
    </tr>
    <tr>
      <th>2017</th>
      <td>97.98</td>
      <td>85.88</td>
      <td>85.12</td>
      <td>87.44</td>
      <td>84.40</td>
      <td>84.24</td>
      <td>92.15</td>
      <td>84.74</td>
      <td>47.37</td>
      <td>78.94</td>
      <td>...</td>
      <td>35.44</td>
      <td>0.00</td>
      <td>72.40</td>
      <td>34.73</td>
      <td>42.40</td>
      <td>0.00</td>
      <td>0.00</td>
      <td>72.93</td>
      <td>66.08</td>
      <td>62.26</td>
    </tr>
    <tr>
      <th>2018</th>
      <td>99.72</td>
      <td>88.40</td>
      <td>91.33</td>
      <td>88.49</td>
      <td>88.53</td>
      <td>88.49</td>
      <td>95.25</td>
      <td>89.54</td>
      <td>59.50</td>
      <td>82.69</td>
      <td>...</td>
      <td>33.26</td>
      <td>0.00</td>
      <td>73.27</td>
      <td>44.99</td>
      <td>60.23</td>
      <td>0.00</td>
      <td>0.00</td>
      <td>77.13</td>
      <td>68.52</td>
      <td>63.62</td>
    </tr>
    <tr>
      <th>2019</th>
      <td>86.67</td>
      <td>78.57</td>
      <td>85.40</td>
      <td>87.41</td>
      <td>70.77</td>
      <td>70.76</td>
      <td>89.07</td>
      <td>84.19</td>
      <td>62.41</td>
      <td>85.02</td>
      <td>...</td>
      <td>34.24</td>
      <td>0.00</td>
      <td>57.08</td>
      <td>45.02</td>
      <td>63.23</td>
      <td>0.00</td>
      <td>0.00</td>
      <td>73.44</td>
      <td>53.74</td>
      <td>68.45</td>
    </tr>
    <tr>
      <th>...</th>
      <th>...</th>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
    </tr>
    <tr>
      <th rowspan="5" valign="top">TO</th>
      <th>2015</th>
      <td>107.48</td>
      <td>94.35</td>
      <td>97.73</td>
      <td>98.73</td>
      <td>100.00</td>
      <td>98.70</td>
      <td>93.92</td>
      <td>97.22</td>
      <td>83.28</td>
      <td>94.52</td>
      <td>...</td>
      <td>52.17</td>
      <td>98.76</td>
      <td>83.28</td>
      <td>15.24</td>
      <td>16.55</td>
      <td>96.74</td>
      <td>231.40</td>
      <td>92.37</td>
      <td>0.00</td>
      <td>0.00</td>
    </tr>
    <tr>
      <th>2016</th>
      <td>99.23</td>
      <td>77.44</td>
      <td>91.00</td>
      <td>92.43</td>
      <td>106.61</td>
      <td>88.14</td>
      <td>97.31</td>
      <td>84.77</td>
      <td>77.26</td>
      <td>75.63</td>
      <td>...</td>
      <td>88.32</td>
      <td>88.14</td>
      <td>69.89</td>
      <td>11.52</td>
      <td>19.23</td>
      <td>5.49</td>
      <td>31.04</td>
      <td>60.94</td>
      <td>2.28</td>
      <td>0.00</td>
    </tr>
    <tr>
      <th>2017</th>
      <td>104.75</td>
      <td>91.32</td>
      <td>89.09</td>
      <td>90.01</td>
      <td>85.77</td>
      <td>85.76</td>
      <td>93.17</td>
      <td>86.05</td>
      <td>73.28</td>
      <td>81.50</td>
      <td>...</td>
      <td>60.91</td>
      <td>0.00</td>
      <td>75.43</td>
      <td>34.82</td>
      <td>53.33</td>
      <td>0.00</td>
      <td>0.00</td>
      <td>77.95</td>
      <td>72.35</td>
      <td>66.53</td>
    </tr>
    <tr>
      <th>2018</th>
      <td>104.06</td>
      <td>100.67</td>
      <td>93.40</td>
      <td>79.72</td>
      <td>90.04</td>
      <td>90.04</td>
      <td>100.71</td>
      <td>91.68</td>
      <td>81.08</td>
      <td>84.08</td>
      <td>...</td>
      <td>65.92</td>
      <td>0.00</td>
      <td>74.54</td>
      <td>45.80</td>
      <td>70.06</td>
      <td>0.00</td>
      <td>0.00</td>
      <td>81.12</td>
      <td>65.28</td>
      <td>62.70</td>
    </tr>
    <tr>
      <th>2019</th>
      <td>112.41</td>
      <td>111.88</td>
      <td>88.73</td>
      <td>93.52</td>
      <td>76.38</td>
      <td>76.38</td>
      <td>94.02</td>
      <td>88.17</td>
      <td>76.47</td>
      <td>87.09</td>
      <td>...</td>
      <td>81.35</td>
      <td>0.00</td>
      <td>55.40</td>
      <td>51.01</td>
      <td>75.97</td>
      <td>0.00</td>
      <td>0.00</td>
      <td>82.12</td>
      <td>53.13</td>
      <td>65.91</td>
    </tr>
  </tbody>
</table>
<p>140 rows × 25 columns</p>
</div>




```python
result.to_csv('cv_uf.csv', encoding='utf8', sep=',', decimal='.', index=False)
```


```python
# !jupyter nbconvert main.ipynb --to markdown --output README.md
```


```python

```
