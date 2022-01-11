import pandas as pd
import numpy as np
import string

#filtering out on london postcode
df = pd.read_csv('/home/diriei/etl_project/original_files/business_census2021.csv',encoding='utf-8')
df = df[df.posttown == 'LONDON']
df.loc[df.posttown == 'LONDON']
# print(df.head())
# print(df.info())
# print(df.describe().round(2))
# print(df.isnull().sum())

#dropping columns not of interest
columns=['dissolutiondate','country','county','countryoforigin','numgenpartners','numlimpartners', 'companystatus', 'nummortcharges', 'nummortoutstanding', 'nummortpartsatisfied', 'nummortsatisfied','siccode', 'sicdescription', 'return_nextduedate','return_lastmadeupdate', 'confirmation_nextduedate','accountingrefday','accountingrefday','accountingrefmonth','account_nextduedate','account_lastmadeupdate','careof','pobox','confirmation_lastmadeupdate','accountscategory'] 
df.drop(columns, axis=1, inplace=True)
df.head()

#stripping whitespace
cols = df.select_dtypes(object).columns
df[cols] = df[cols].apply(lambda x: x.str.strip())

#cleaning postcode
df['postcode'].replace('',np.nan,inplace=True)
df.dropna(subset=['postcode'], inplace =True)
df['postcode'] = df['postcode'].str.replace(' ','')
df = df.drop_duplicates(subset='postcode', keep='first')
# prnt(df.count())

#cleaning company category
df['companycategory'].replace(to_replace=["PRI/LTD BY GUAR/NSC (Private, limited by guarantee, no share capital)","PRI/LBG/NSC (Private, Limited by guarantee, no share capital, use of 'Limited' exemption)","PRIV LTD SECT. 30 (Private limited company, section 30 of the Companies Act)"], value = "Private Limited Comapany",inplace=True)
df['companycategory'].replace(to_replace="Private Unlimited", value="Private Unlimited Company",inplace=True)
df['companycategory'].replace(to_replace='', value='', inplace=True)
df.dropna(subset=['companycategory'], inplace=True)
# print(df.count())

df["incorporationdate"] = pd.to_datetime(df["incorporationdate"])

#capitalising addresslines
df['addressline1'] = df['addressline1'].astype(str)
df['addressline2'] = df['addressline2'].astype(str)
df['addressline1'] = df[['addressline1']].applymap(string.capwords)
df['addressline2'] = df[['addressline2']].applymap(string.capwords)
print(df.count())


#merging coordinate columns with dataframe
coordinates = pd.read_csv('/home/diriei/etl_project/transformed_files/cleaned_postcode_coordinates_1.csv')
new_df=pd.merge(df, coordinates, on='postcode', how='left')
new_df.drop('Unnamed: 0', axis=1, inplace=True)

#extracting borough code and merging 
borough_code = pd.read_csv('/Users/diriei/Desktop/Training/pipeline_project/postcodes_directory.csv')
borough_code['pcd'] = borough_code['pcd'].str.replace(' ','')
borough_code = borough_code[['pcd','oslaua']]
borough_code = borough_code.rename(columns = {'oslaua':'borough_code'})
borough_code = borough_code.rename(columns = {'pcd':'postcode'})
final_df=pd.merge(new_df, borough_code, on='postcode', how='inner')

#changing order of columnms
new_df= new_df[['companynumber','incorporationdate','addressline1','addressline2','posttown','postcode','latitude','longitude','borough_code']]
new_df.head()

new_df.to_csv('/home/diriei/etl_project/transformed_files/cleaned_business_census.csv', sep='|')


#cultural infrastructure 
all_sites= pd.read_csv('/home/diriei/etl_project/original_files/all_sites.csv')
all_sites=all_sites[['Cultural Venue Type','borough_code','borough_name']]
# print(all_sites.head())
# print(all_sites.info())
# print(all_sites.describe().round(2))
# print(print(all_sites.isnull().sum()))

#dropping empty rows
all_sites['borough_code'].replace('',np.nan,inplace=True)
all_sites['borough_name'].replace('',np.nan,inplace=True)
all_sites.dropna(subset=['borough_code'], inplace=True)
all_sites.dropna(subset=['borough_name'], inplace=True)

#removing whitespace
all_sites['borough_name']=all_sites['borough_name'].astype(str)
all_sites['borough_code']=all_sites['borough_code'].astype(str)
all_sites['Cultural Venue Type']=all_sites['Cultural Venue Type'].astype(str)
cols = all_sites.select_dtypes(object).columns
all_sites[cols] = all_sites[cols].apply(lambda x: x.str.strip())

#Capitalising borough names
all_sites['borough_name']=all_sites['borough_name'].astype(str)
all_sites['borough_name']=all_sites[['borough_name']].applymap(str.title)

#cleaning borough name values
all_sites.replace(to_replace =['City And County Of Th','City And County Of The Cit','City And County Of The City Of London'], value ='City Of London',inplace=True)
all_sites.replace(to_replace =['Hammersmith & Fulham','Hammersmith And Fulha'], value ='Hammersmith And Fulham',inplace=True)
all_sites.replace(to_replace =['Kensington & Chelsea','Kensington And Chelse'], value ='Kensington And Chelsea',inplace=True)
all_sites.replace(to_replace ='Westminster', value ='City Of Westminster',inplace=True)
all_sites.replace(to_replace='Barking', value='Barking And Dagenham',inplace=True)

all_sites.to_csv('/home/diriei/etl_project/transformed_files/final_cultural_venue_data_check.csv')

#borough mapping
borough_df= pd.read_csv('/home/diriei/etl_project/original_files/all_sites.csv')
borough_df=borough_df[['borough_code','borough_name']]
print(borough_df.count())
print(borough_df.info())
print(print(borough_df.isnull().sum()))

#dropping duplicates
borough_df=borough_df.drop_duplicates(subset='borough_name', keep='last')
borough_df=borough_df.drop_duplicates(subset='borough_code', keep='last')
borough_df.count()

#stripping whitespace
cols = borough_df.select_dtypes(object).columns
borough_df[cols] = borough_df[cols].apply(lambda x: x.str.strip())

#capitalising borough name
borough_df['borough_name']=borough_df['borough_name'].astype(str)
borough_df['borough_name']=borough_df[['borough_name']].applymap(str.title)

#cleaning borough name strings
borough_df.replace(to_replace = ['City And County Of Th','City And County Of The Cit','City And County Of The City Of London'], value = 'City Of London',regex=True)
borough_df.replace(to_replace = ['Hammersmith & Fulham','Hammersmith And Fulha'], value = 'Hammersmith And Fulham',regex=True)
borough_df.replace(to_replace = ['Kensington & Chelsea','Kensington And Chelse'], value = 'Kensington And Chelsea',regex=True)
borough_df.replace(to_replace = 'Barking', value='Barking And Dagenham',regex=True)
borough_df.replace(to_replace = 'Westminster', value ='City Of Westminster',regex=True)
# print(borough_df.count())

#dropping empty values
borough_df.replace(to_replace = '', value = np.nan)
borough_df.dropna(subset=['borough_code'], inplace=True)
borough_df.dropna(subset=['borough_name'], inplace=True)
# print(borough_df.count())

borough_df.to_csv('/home/diriei/etl_project/transformed_files/mapping_borough.csv')
