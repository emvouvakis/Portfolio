```python
#Import libraries

import pandas as pd
import seaborn as sns
import numpy as np

import matplotlib 
import matplotlib.pyplot as plt
plt.style.use('ggplot')
from matplotlib.pyplot import figure
%matplotlib inline
matplotlib.rcParams['figure.figsize']= (12,8)

#Read data
df = pd.read_csv(r'C:\Users\Μανώλης Βουβάκης\Desktop\movies.csv')


```


```python
#Sneak peak at the Data
df.head()
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>budget</th>
      <th>company</th>
      <th>country</th>
      <th>director</th>
      <th>genre</th>
      <th>gross</th>
      <th>name</th>
      <th>rating</th>
      <th>released</th>
      <th>runtime</th>
      <th>score</th>
      <th>star</th>
      <th>votes</th>
      <th>writer</th>
      <th>year</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>8000000.0</td>
      <td>Columbia Pictures Corporation</td>
      <td>USA</td>
      <td>Rob Reiner</td>
      <td>Adventure</td>
      <td>52287414.0</td>
      <td>Stand by Me</td>
      <td>R</td>
      <td>1986-08-22</td>
      <td>89</td>
      <td>8.1</td>
      <td>Wil Wheaton</td>
      <td>299174</td>
      <td>Stephen King</td>
      <td>1986</td>
    </tr>
    <tr>
      <th>1</th>
      <td>6000000.0</td>
      <td>Paramount Pictures</td>
      <td>USA</td>
      <td>John Hughes</td>
      <td>Comedy</td>
      <td>70136369.0</td>
      <td>Ferris Bueller's Day Off</td>
      <td>PG-13</td>
      <td>1986-06-11</td>
      <td>103</td>
      <td>7.8</td>
      <td>Matthew Broderick</td>
      <td>264740</td>
      <td>John Hughes</td>
      <td>1986</td>
    </tr>
    <tr>
      <th>2</th>
      <td>15000000.0</td>
      <td>Paramount Pictures</td>
      <td>USA</td>
      <td>Tony Scott</td>
      <td>Action</td>
      <td>179800601.0</td>
      <td>Top Gun</td>
      <td>PG</td>
      <td>1986-05-16</td>
      <td>110</td>
      <td>6.9</td>
      <td>Tom Cruise</td>
      <td>236909</td>
      <td>Jim Cash</td>
      <td>1986</td>
    </tr>
    <tr>
      <th>3</th>
      <td>18500000.0</td>
      <td>Twentieth Century Fox Film Corporation</td>
      <td>USA</td>
      <td>James Cameron</td>
      <td>Action</td>
      <td>85160248.0</td>
      <td>Aliens</td>
      <td>R</td>
      <td>1986-07-18</td>
      <td>137</td>
      <td>8.4</td>
      <td>Sigourney Weaver</td>
      <td>540152</td>
      <td>James Cameron</td>
      <td>1986</td>
    </tr>
    <tr>
      <th>4</th>
      <td>9000000.0</td>
      <td>Walt Disney Pictures</td>
      <td>USA</td>
      <td>Randal Kleiser</td>
      <td>Adventure</td>
      <td>18564613.0</td>
      <td>Flight of the Navigator</td>
      <td>PG</td>
      <td>1986-08-01</td>
      <td>90</td>
      <td>6.9</td>
      <td>Joey Cramer</td>
      <td>36636</td>
      <td>Mark H. Baker</td>
      <td>1986</td>
    </tr>
  </tbody>
</table>
</div>




```python
#Checking for missing data

for col in df.columns:
    pct_missing = np.mean(df[col].isnull())
    print('{} - {}% '.format(col,pct_missing))
    
```

    budget - 0.0% 
    company - 0.0% 
    country - 0.0% 
    director - 0.0% 
    genre - 0.0% 
    gross - 0.0% 
    name - 0.0% 
    rating - 0.0% 
    released - 0.0% 
    runtime - 0.0% 
    score - 0.0% 
    star - 0.0% 
    votes - 0.0% 
    writer - 0.0% 
    year - 0.0% 
    


```python
sns.regplot(x='gross', y='budget', data=df, scatter_kws={"color":"black"}, line_kws={"color":"red"})
```




    <AxesSubplot:xlabel='gross', ylabel='budget'>




    
![png](output_3_1.png)
    



```python
df.corr(method="pearson")
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>budget</th>
      <th>gross</th>
      <th>runtime</th>
      <th>score</th>
      <th>votes</th>
      <th>year</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>budget</th>
      <td>1.000000</td>
      <td>0.712196</td>
      <td>0.268226</td>
      <td>0.042145</td>
      <td>0.503924</td>
      <td>0.291009</td>
    </tr>
    <tr>
      <th>gross</th>
      <td>0.712196</td>
      <td>1.000000</td>
      <td>0.224579</td>
      <td>0.165693</td>
      <td>0.662457</td>
      <td>0.191548</td>
    </tr>
    <tr>
      <th>runtime</th>
      <td>0.268226</td>
      <td>0.224579</td>
      <td>1.000000</td>
      <td>0.395343</td>
      <td>0.317399</td>
      <td>0.087639</td>
    </tr>
    <tr>
      <th>score</th>
      <td>0.042145</td>
      <td>0.165693</td>
      <td>0.395343</td>
      <td>1.000000</td>
      <td>0.393607</td>
      <td>0.105276</td>
    </tr>
    <tr>
      <th>votes</th>
      <td>0.503924</td>
      <td>0.662457</td>
      <td>0.317399</td>
      <td>0.393607</td>
      <td>1.000000</td>
      <td>0.229304</td>
    </tr>
    <tr>
      <th>year</th>
      <td>0.291009</td>
      <td>0.191548</td>
      <td>0.087639</td>
      <td>0.105276</td>
      <td>0.229304</td>
      <td>1.000000</td>
    </tr>
  </tbody>
</table>
</div>




```python
sns.swarmplot(x="rating", y="gross", data=df)
```


```python
correlation_matrix = df.corr()

sns.heatmap(correlation_matrix, annot = True)

plt.title("Correlation matrix for Numeric Features")

plt.xlabel("Movie features")

plt.ylabel("Movie features")

plt.show()
```


    
![png](output_6_0.png)
    

