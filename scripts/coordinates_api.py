from typing import Iterator
import requests
import pandas as pd
import json
import time
import numpy as np 

#extract london postcodes
df = pd.read_csv('/home/diriei/etl_project/original_files/business_census2021.csv',encoding='utf-8')
df = df[df.posttown == 'LONDON']
df=df['postcode'].unique()
df= pd.DataFrame(df)
df.columns = ['postcode']
df=df.replace(' ','+', regex=True)


#convert postcode into longitude and latitude, https://api.getthedata.com/postcode/(ADD+POSTCODE+HERE)
def postcode_converter(x):
    longitude,latitude= None,None
    new_url='https://api.getthedata.com/postcode/'+str(x)
    new_url=new_url.strip()
    r = requests.get(new_url)
    time.sleep(0.1) 
    r= r.text
    try:
        result= json.loads(r)
        longitude=result['data']['longitude']
        latitude=result['data']['latitude']
    except Exception:
        pass
    return (longitude,latitude)

df['longitude'],df['latitude']=zip(*df['postcode'].apply(postcode_converter))


#cleaning coordinates column by dropping empty rows and changing string format
df['longitude'].replace('',np.nan,inplace=True)
df['latitude'].replace('',np.nan,inplace=True)
df.dropna(subset=['longitude'], inplace =True)
df.dropna(subset=['latitude'], inplace=True)
df['postcode'] = df['postcode'].str.replace('+','')
df.to_csv('/home/diriei/etl_project/transformed_files/cleaned_postcode_coordinates.csv', sep=',', encoding='utf-8')

