import os
import sys

import pandas as pd

import logging
import numpy as np
from iecasdmx.funciones import crear_mapeo_por_defecto

from iecasdmx.funciones import strip_accents

data = {'first_set': [1,2,2,4,5,np.nan,6,7,np.nan,np.nan,2,9,10,np.nan],
        'second_set': ['a','b',np.nan,np.nan,'c','d','e',np.nan,np.nan,'f','g',np.nan,'h','i']
        }

df = pd.DataFrame(data,columns=['first_set','second_set'])

print (df)

df_na = df[df['first_set'].isna()]
df_notna = df[df['first_set'].notna()]

print(df_na)
print(df_notna)
#"drop duplicates from notna"
df_notna.drop_duplicates('first_set',inplace=True)

df = pd.concat([df_na,df_notna])
print(df_notna)
print(df)