#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pandas as pd
import psycopg2
import sys
import pyathena
import pyodbc
import pandasql as pdsql
pysql = lambda q: pdsql.sqldf(q, globals())
import win32com.client
import os

Masked_DataFrame_Temp = pd.DataFrame()
Masked_DataFrame = pd.DataFrame()

Client_Code = 'pcvsys1'
start_date = '2020-01-08'
end_date = '2020-02-08'
Publisher_Lookup = 'LKP_CUSTOM_DIM_DETAILS'
Client_CDS_Env = 'US'

Metadata_File_Dict = {
    "CCTSYS1": "C:\\Users\\Alen.James\\PythonProjects\\MetaData\\ConstantContact_MetaData.xlsx",
    "CARSYS2": "C:\\Users\\Alen.James\\PythonProjects\\MetaData\\Cars_MetaData.xlsx",
    "NFCSYS1": "C:\\Users\\Alen.James\\PythonProjects\\MetaData\\NFCU_MetaData.xlsx",
    "JBESYS1": "C:\\Users\\Alen.James\\PythonProjects\\MetaData\\JetBlue Metadata.xlsx",
    "ELCSYS1": "C:\\Users\\Mredul.Mohan\\Python Tutorial\\Express_MetaData.xlsx",
  
    
}

Netezza_Server_Lookup = {
    1: 'xx.xx.xx.xx',
    2: 'xx.xx.xx.xx',
    3: 'xx.xx.xx.xx',
    4: 'xx.xx.xx.xx'
}


# In[2]:


pg_conn = psycopg2.connect(database="", user="", password="", host="",
                           port="").cursor()
pg_conn.execute(
    "SELECT A.ID,A.CLIENT_NAME, C.ID FROM ZE_CLIENT A JOIN ZE_CLIENT_INFRA_CONF B ON A.ID=B.CLIENT_ID JOIN ZE_INF_SERVER C ON B.NZ_SERVER_ID=C.ID where lower(a.client_code) = '" + Client_Code.lower() + "'")
Query_Result = pg_conn.fetchall()
pg_conn.close()

Client_ID, Client_Name, Server_ID = Query_Result[0]
Server_IP = Netezza_Server_Lookup[Server_ID]


# In[3]:


Netezza_Connection_String = "DRIVER={NetezzaSQL}; SERVER=" + Server_IP + "; PORT=5480; DATABASE=" + Client_Code + "_SUGAR_COOKIE; UID=; PWD=;"
Netezza_Query_String = "SELECT DISTINCT SITE_ID, SITE FROM " + Client_Code + "_SUGAR_COOKIE.." + Publisher_Lookup + ";"
print(Netezza_Query_String)
Netezza_conn = pyodbc.connect(Netezza_Connection_String)
Site_Lookup = pd.read_sql(Netezza_Query_String,Netezza_conn)

Site_Lookup.head(2)


# In[4]:


Metadata_File = pd.ExcelFile(Metadata_File_Dict[Client_Code.upper()])

Metadata_DataFrame = Metadata_File.parse('Filters', skiprows=1)
Metadata_DataFrame.rename(
    columns={'Filter.1': 'DCM_Filter', 'Table.1': 'DCM_Table', 'Filter': 'VIQ_Filter', 'Table': 'VIQ_Table'},
    inplace=True)
Metadata_DataFrame.head(30)


# In[5]:


if(Client_CDS_Env == 'US'):
    Ath_conn = pyathena.connect(s3_staging_dir='s3://aws-athena-query-results-634335036558-us-east-1/',
                      aws_access_key_id='',
                      aws_secret_access_key='',
                      region_name='us-east-1'
                      ).cursor()
elif(Client_CDS_Env == 'EMEA'):
    Ath_conn = pyathena.connect(s3_staging_dir='s3://aws-athena-query-results-634335036558-eu-west-1/',
                      aws_access_key_id='',
                      aws_secret_access_key='',
                      region_name='eu-west-1'
                      ).cursor()


# In[6]:


def DF_Query_Execute(QueryString):
    TempDataFrame = pd.DataFrame()
    for attempt in range(5):
        try:
            Ath_conn.execute(QueryString)
            TempDataFrame = pd.DataFrame(Ath_conn.fetchall(),
                                         columns=['Pixel_Type', 'Channel_Type', 'Channel', 'VIQ_Table', 'Dimension ID',
                                                  'Total', 'Masked', 'Safari', 'Safari_Masked', 'Chrome',
                                                  'Chrome_Masked', 'Firefox', 'Firefox_Masked', 'MicrosoftIE',
                                                  'MicrosoftIE_Masked', 'Opera', 'Opera_Masked', 'Yandex',
                                                  'Yandex_Masked', 'Android', 'Android_Masked', 'Others',
                                                  'Others_Masked', 'No_Browser', 'No_Browser_Masked'])
        except Exception as e:
            print('Error on line {}'.format(sys.exc_info()[-1].tb_lineno), "\nError Type :", type(e).__name__,
                  "\nError:", e)
            print("Attempt #:" + str(attempt + 1))
            continue
        else:
            break
    else:
        print("Tried reconnecting 5 times. Aborting the run.")
    return TempDataFrame


# In[7]:


for row in Metadata_DataFrame.itertuples():
    if(pd.notnull(row.VIQ_Filter) or pd.notnull(row.VIQ_Table)):
        print("Execution of Query for Channel Type:" + row.Channel_Type + "- Channel:" + row.Channel + "- Table:" + row.VIQ_Table + " is in Progress...")
        if row.Channel_Type == 'Display':
            Query = "select '" + row.Pixel_Type + "', '" + row.Channel_Type + "', '" + row.Channel + "', '" + row.VIQ_Table + "', " + " site_id , count(*) Total , sum(case when user_id like '0-%' then 1 else 0 end) Masked , sum(case when (lower(browser_version) like '%safari%' or lower(browser_version) like '%ios%') then 1 else 0 end) Safari , sum(case when (lower(browser_version) like '%safari%' or lower(browser_version) like '%ios%') and user_id like '0-%' then 1 else 0 end) Safari_Masked , sum(case when (lower(browser_version) like '%chrome%' or lower(browser_version) like '%google%') then 1 else 0 end) Chrome , sum(case when (lower(browser_version) like '%chrome%' or lower(browser_version) like '%google%') and user_id like '0-%' then 1 else 0 end) Chrome_Masked , sum(case when (lower(browser_version) like '%mozilla%' or lower(browser_version) like '%firefox%') then 1 else 0 end) Firefox , sum(case when (lower(browser_version) like '%mozilla%' or lower(browser_version) like '%firefox%') and user_id like '0-%' then 1 else 0 end) Firefox_Masked , sum(case when (lower(browser_version) like '%edge%' or lower(browser_version) like '%msie%') then 1 else 0 end) MicrosoftIE , sum(case when (lower(browser_version) like '%edge%' or lower(browser_version) like '%msie%') and user_id like '0-%' then 1 else 0 end) MicrosoftIE_Masked , sum(case when lower(browser_version) like '%opera%' then 1 else 0 end) Opera , sum(case when lower(browser_version) like '%opera%' and user_id like '0-%' then 1 else 0 end) Opera_Masked , sum(case when lower(browser_version) like '%yandex%' then 1 else 0 end) Yandex , sum(case when lower(browser_version) like '%yandex%' and user_id like '0-%' then 1 else 0 end) Yandex_Masked , sum(case when lower(browser_version) like '%android%' then 1 else 0 end) Android , sum(case when lower(browser_version) like '%android%' and user_id like '0-%' then 1 else 0 end) Android_Masked , sum(case when lower(browser_version) not like '%safari%' and lower(browser_version) not like '%ios%' and lower(browser_version) not like '%chrome%' and lower(browser_version) not like '%google%' and lower(browser_version) not like '%mozilla%' and lower(browser_version) not like '%firefox%' and lower(browser_version) not like '%edge%' and lower(browser_version) not like '%msie%' and lower(browser_version) not like '%opera%' and lower(browser_version) not like '%yandex%' and lower(browser_version) not like '%android%' then 1 else 0 end) Others , sum(case when lower(browser_version) not like '%safari%' and lower(browser_version) not like '%ios%' and lower(browser_version) not like '%chrome%' and lower(browser_version) not like '%google%' and lower(browser_version) not like '%mozilla%' and lower(browser_version) not like '%firefox%' and lower(browser_version) not like '%edge%' and lower(browser_version) not like '%msie%' and lower(browser_version) not like '%opera%' and lower(browser_version) not like '%yandex%' and lower(browser_version) not like '%android%' and user_id like '0-%' then 1 else 0 end) Others_Masked , sum(case when (browser_version is null or length(browser_version)=0) then 1 else 0 end) No_Browser , sum(case when (browser_version is null or length(browser_version)=0) and user_id like '0-%' then 1 else 0 end) No_Browser_Masked from " + Client_Code + "_core_data_db." + row.VIQ_Table + " where date(time) between date('" + start_date + "') and date('" + end_date + "') and " + str(row.VIQ_Filter) + " group by 1,2,3,4,5;"
        elif row.Channel_Type == 'Paid Search':
            Query = "select '" + row.Pixel_Type + "', '" + row.Channel_Type + "', '" + row.Channel + "', '" + row.VIQ_Table + "', " + " creative_size_id , count(*) Total , sum(case when user_id like '0-%' then 1 else 0 end) Masked , sum(case when (lower(browser_version) like '%safari%' or lower(browser_version) like '%ios%') then 1 else 0 end) Safari , sum(case when (lower(browser_version) like '%safari%' or lower(browser_version) like '%ios%') and user_id like '0-%' then 1 else 0 end) Safari_Masked , sum(case when (lower(browser_version) like '%chrome%' or lower(browser_version) like '%google%') then 1 else 0 end) Chrome , sum(case when (lower(browser_version) like '%chrome%' or lower(browser_version) like '%google%') and user_id like '0-%' then 1 else 0 end) Chrome_Masked , sum(case when (lower(browser_version) like '%mozilla%' or lower(browser_version) like '%firefox%') then 1 else 0 end) Firefox , sum(case when (lower(browser_version) like '%mozilla%' or lower(browser_version) like '%firefox%') and user_id like '0-%' then 1 else 0 end) Firefox_Masked , sum(case when (lower(browser_version) like '%edge%' or lower(browser_version) like '%msie%') then 1 else 0 end) MicrosoftIE , sum(case when (lower(browser_version) like '%edge%' or lower(browser_version) like '%msie%') and user_id like '0-%' then 1 else 0 end) MicrosoftIE_Masked , sum(case when lower(browser_version) like '%opera%' then 1 else 0 end) Opera , sum(case when lower(browser_version) like '%opera%' and user_id like '0-%' then 1 else 0 end) Opera_Masked , sum(case when lower(browser_version) like '%yandex%' then 1 else 0 end) Yandex , sum(case when lower(browser_version) like '%yandex%' and user_id like '0-%' then 1 else 0 end) Yandex_Masked , sum(case when lower(browser_version) like '%android%' then 1 else 0 end) Android , sum(case when lower(browser_version) like '%android%' and user_id like '0-%' then 1 else 0 end) Android_Masked , sum(case when lower(browser_version) not like '%safari%' and lower(browser_version) not like '%ios%' and lower(browser_version) not like '%chrome%' and lower(browser_version) not like '%google%' and lower(browser_version) not like '%mozilla%' and lower(browser_version) not like '%firefox%' and lower(browser_version) not like '%edge%' and lower(browser_version) not like '%msie%' and lower(browser_version) not like '%opera%' and lower(browser_version) not like '%yandex%' and lower(browser_version) not like '%android%' then 1 else 0 end) Others , sum(case when lower(browser_version) not like '%safari%' and lower(browser_version) not like '%ios%' and lower(browser_version) not like '%chrome%' and lower(browser_version) not like '%google%' and lower(browser_version) not like '%mozilla%' and lower(browser_version) not like '%firefox%' and lower(browser_version) not like '%edge%' and lower(browser_version) not like '%msie%' and lower(browser_version) not like '%opera%' and lower(browser_version) not like '%yandex%' and lower(browser_version) not like '%android%' and user_id like '0-%' then 1 else 0 end) Others_Masked , sum(case when (browser_version is null or length(browser_version)=0) then 1 else 0 end) No_Browser , sum(case when (browser_version is null or length(browser_version)=0) and user_id like '0-%' then 1 else 0 end) No_Browser_Masked from " + Client_Code + "_core_data_db." + row.VIQ_Table + " where date(time) between date('" + start_date + "') and date('" + end_date + "') and " + str(row.VIQ_Filter) + " group by 1,2,3,4,5;"
        elif row.Channel_Type == 'Hosted Tag':
            Query = "select '" + row.Pixel_Type + "', '" + row.Channel_Type + "', '" + row.Channel + "', '" + row.VIQ_Table + "', " + " page_id , count(*) Total , sum(case when user_id like '0-%' then 1 else 0 end) Masked , sum(case when (lower(browser_version) like '%safari%' or lower(browser_version) like '%ios%') then 1 else 0 end) Safari , sum(case when (lower(browser_version) like '%safari%' or lower(browser_version) like '%ios%') and user_id like '0-%' then 1 else 0 end) Safari_Masked , sum(case when (lower(browser_version) like '%chrome%' or lower(browser_version) like '%google%') then 1 else 0 end) Chrome , sum(case when (lower(browser_version) like '%chrome%' or lower(browser_version) like '%google%') and user_id like '0-%' then 1 else 0 end) Chrome_Masked , sum(case when (lower(browser_version) like '%mozilla%' or lower(browser_version) like '%firefox%') then 1 else 0 end) Firefox , sum(case when (lower(browser_version) like '%mozilla%' or lower(browser_version) like '%firefox%') and user_id like '0-%' then 1 else 0 end) Firefox_Masked , sum(case when (lower(browser_version) like '%edge%' or lower(browser_version) like '%msie%') then 1 else 0 end) MicrosoftIE , sum(case when (lower(browser_version) like '%edge%' or lower(browser_version) like '%msie%') and user_id like '0-%' then 1 else 0 end) MicrosoftIE_Masked , sum(case when lower(browser_version) like '%opera%' then 1 else 0 end) Opera , sum(case when lower(browser_version) like '%opera%' and user_id like '0-%' then 1 else 0 end) Opera_Masked , sum(case when lower(browser_version) like '%yandex%' then 1 else 0 end) Yandex , sum(case when lower(browser_version) like '%yandex%' and user_id like '0-%' then 1 else 0 end) Yandex_Masked , sum(case when lower(browser_version) like '%android%' then 1 else 0 end) Android , sum(case when lower(browser_version) like '%android%' and user_id like '0-%' then 1 else 0 end) Android_Masked , sum(case when lower(browser_version) not like '%safari%' and lower(browser_version) not like '%ios%' and lower(browser_version) not like '%chrome%' and lower(browser_version) not like '%google%' and lower(browser_version) not like '%mozilla%' and lower(browser_version) not like '%firefox%' and lower(browser_version) not like '%edge%' and lower(browser_version) not like '%msie%' and lower(browser_version) not like '%opera%' and lower(browser_version) not like '%yandex%' and lower(browser_version) not like '%android%' then 1 else 0 end) Others , sum(case when lower(browser_version) not like '%safari%' and lower(browser_version) not like '%ios%' and lower(browser_version) not like '%chrome%' and lower(browser_version) not like '%google%' and lower(browser_version) not like '%mozilla%' and lower(browser_version) not like '%firefox%' and lower(browser_version) not like '%edge%' and lower(browser_version) not like '%msie%' and lower(browser_version) not like '%opera%' and lower(browser_version) not like '%yandex%' and lower(browser_version) not like '%android%' and user_id like '0-%' then 1 else 0 end) Others_Masked , sum(case when (browser_version is null or length(browser_version)=0) then 1 else 0 end) No_Browser , sum(case when (browser_version is null or length(browser_version)=0) and user_id like '0-%' then 1 else 0 end) No_Browser_Masked from " + Client_Code + "_core_data_db." + row.VIQ_Table + " where date(time) between date('" + start_date + "') and date('" + end_date + "') and " + str(row.VIQ_Filter) + " group by 1,2,3,4,5;"
        elif row.Channel_Type == 'Conversions':
            Query = "select '" + row.Pixel_Type + "', '" + row.Channel_Type + "', '" + row.Channel + "', '" + row.VIQ_Table + "', " + " activity_type , count(*) Total , sum(case when user_id like '0-%' then 1 else 0 end) Masked , sum(case when (lower(browser_version) like '%safari%' or lower(browser_version) like '%ios%') then 1 else 0 end) Safari , sum(case when (lower(browser_version) like '%safari%' or lower(browser_version) like '%ios%') and user_id like '0-%' then 1 else 0 end) Safari_Masked , sum(case when (lower(browser_version) like '%chrome%' or lower(browser_version) like '%google%') then 1 else 0 end) Chrome , sum(case when (lower(browser_version) like '%chrome%' or lower(browser_version) like '%google%') and user_id like '0-%' then 1 else 0 end) Chrome_Masked , sum(case when (lower(browser_version) like '%mozilla%' or lower(browser_version) like '%firefox%') then 1 else 0 end) Firefox , sum(case when (lower(browser_version) like '%mozilla%' or lower(browser_version) like '%firefox%') and user_id like '0-%' then 1 else 0 end) Firefox_Masked , sum(case when (lower(browser_version) like '%edge%' or lower(browser_version) like '%msie%') then 1 else 0 end) MicrosoftIE , sum(case when (lower(browser_version) like '%edge%' or lower(browser_version) like '%msie%') and user_id like '0-%' then 1 else 0 end) MicrosoftIE_Masked , sum(case when lower(browser_version) like '%opera%' then 1 else 0 end) Opera , sum(case when lower(browser_version) like '%opera%' and user_id like '0-%' then 1 else 0 end) Opera_Masked , sum(case when lower(browser_version) like '%yandex%' then 1 else 0 end) Yandex , sum(case when lower(browser_version) like '%yandex%' and user_id like '0-%' then 1 else 0 end) Yandex_Masked , sum(case when lower(browser_version) like '%android%' then 1 else 0 end) Android , sum(case when lower(browser_version) like '%android%' and user_id like '0-%' then 1 else 0 end) Android_Masked , sum(case when lower(browser_version) not like '%safari%' and lower(browser_version) not like '%ios%' and lower(browser_version) not like '%chrome%' and lower(browser_version) not like '%google%' and lower(browser_version) not like '%mozilla%' and lower(browser_version) not like '%firefox%' and lower(browser_version) not like '%edge%' and lower(browser_version) not like '%msie%' and lower(browser_version) not like '%opera%' and lower(browser_version) not like '%yandex%' and lower(browser_version) not like '%android%' then 1 else 0 end) Others , sum(case when lower(browser_version) not like '%safari%' and lower(browser_version) not like '%ios%' and lower(browser_version) not like '%chrome%' and lower(browser_version) not like '%google%' and lower(browser_version) not like '%mozilla%' and lower(browser_version) not like '%firefox%' and lower(browser_version) not like '%edge%' and lower(browser_version) not like '%msie%' and lower(browser_version) not like '%opera%' and lower(browser_version) not like '%yandex%' and lower(browser_version) not like '%android%' and user_id like '0-%' then 1 else 0 end) Others_Masked , sum(case when (browser_version is null or length(browser_version)=0) then 1 else 0 end) No_Browser , sum(case when (browser_version is null or length(browser_version)=0) and user_id like '0-%' then 1 else 0 end) No_Browser_Masked from " + Client_Code + "_core_data_db." + row.VIQ_Table + " where date(time) between date('" + start_date + "') and date('" + end_date + "') and " + str(row.VIQ_Filter) + " group by 1,2,3,4,5;"
        else:
            Query = "A new Channel is present. Update the Code"
        Masked_DataFrame_Temp = DF_Query_Execute(Query)
        Masked_DataFrame = Masked_DataFrame.append(Masked_DataFrame_Temp, sort='False')
        print("Execution of Query for Channel Type:" + row.Channel_Type + "- Channel:" + row.Channel + "- Table:" + row.VIQ_Table + " is Completed")
Ath_conn.close()


# In[8]:


Masked_DataFrame.rename(columns={'Table' : 'VIQ_Table'}, inplace = True)


# In[9]:


# Overview Tab
Overview_DF = pysql("select VIQ_Table, sum(Total), sum(Masked)*1.0/sum(Total), sum(Safari)*1.0/sum(Total), sum(Safari_Masked)*1.0/sum(Safari), sum(Chrome)*1.0/sum(Total), sum(Chrome_Masked)*1.0/sum(Chrome), sum(Firefox)*1.0/sum(Total), sum(Firefox_Masked)*1.0/sum(Firefox), sum(MicrosoftIE)*1.0/sum(Total), sum(MicrosoftIE_Masked)*1.0/sum(MicrosoftIE), sum(Opera)*1.0/sum(Total), sum(Opera_Masked)*1.0/sum(Opera), sum(Yandex)*1.0/sum(Total), sum(Yandex_Masked)*1.0/sum(Yandex), sum(Android)*1.0/sum(Total), sum(Android_Masked)*1.0/sum(Android), sum(Others)*1.0/sum(Total), sum(Others_Masked)*1.0/sum(Others), sum(No_Browser)*1.0/sum(Total), sum(No_Browser_Masked)*1.0/sum(No_Browser) from Masked_DataFrame group by 1 order by VIQ_Table desc")
Overview_DF = Overview_DF.fillna(0.0)
# Pixel wise Report
DF_Tot_by_Table = pysql("select VIQ_Table, sum(Total) Tot_by_Table from Masked_DataFrame group by 1")
Channel_Overview_DF = pysql("select A.VIQ_Table, A.Channel, sum(Total), sum(Total)*1.0/Tot_by_Table, sum(Masked)*1.0/sum(Total), sum(Masked)*1.0/Tot_by_Table, sum(Safari)*1.0/sum(Total), sum(Safari_Masked)*1.0/sum(Safari), sum(Chrome)*1.0/sum(Total), sum(Chrome_Masked)*1.0/sum(Chrome), sum(Firefox)*1.0/sum(Total), sum(Firefox_Masked)*1.0/sum(Firefox), sum(MicrosoftIE)*1.0/sum(Total), sum(MicrosoftIE_Masked)*1.0/sum(MicrosoftIE), sum(Opera)*1.0/sum(Total), sum(Opera_Masked)*1.0/sum(Opera), sum(Yandex)*1.0/sum(Total), sum(Yandex_Masked)*1.0/sum(Yandex), sum(Android)*1.0/sum(Total), sum(Android_Masked)*1.0/sum(Android), sum(Others)*1.0/sum(Total), sum(Others_Masked)*1.0/sum(Others), sum(No_Browser)*1.0/sum(Total), sum(No_Browser_Masked)*1.0/sum(No_Browser) from Masked_DataFrame A join DF_Tot_by_Table B on A.VIQ_Table = B.VIQ_Table group by 1,2 order by 1 desc, 3 desc")
Channel_Overview_DF = Channel_Overview_DF.fillna(0.0)
# DCM Impressions 
DF_Tot_by_Table_Display_Imp = pysql("select VIQ_Table, sum(Total) Tot_by_Table from Masked_DataFrame where Channel_Type = 'Display' group by 1")
DCM_Impressions_DF = pysql("select case when site is null then 'Unknown' else site end Site, sum(Total), sum(Total)*1.0/Tot_by_Table, sum(Masked)*1.0/sum(Total), sum(Masked)*1.0/Tot_by_Table, sum(Safari)*1.0/sum(Total), sum(Safari_Masked)*1.0/sum(Safari), sum(Chrome)*1.0/sum(Total), sum(Chrome_Masked)*1.0/sum(Chrome), sum(Firefox)*1.0/sum(Total), sum(Firefox_Masked)*1.0/sum(Firefox), sum(MicrosoftIE)*1.0/sum(Total), sum(MicrosoftIE_Masked)*1.0/sum(MicrosoftIE), sum(Opera)*1.0/sum(Total), sum(Opera_Masked)*1.0/sum(Opera), sum(Yandex)*1.0/sum(Total), sum(Yandex_Masked)*1.0/sum(Yandex), sum(Android)*1.0/sum(Total), sum(Android_Masked)*1.0/sum(Android), sum(Others)*1.0/sum(Total), sum(Others_Masked)*1.0/sum(Others), sum(No_Browser)*1.0/sum(Total), sum(No_Browser_Masked)*1.0/sum(No_Browser) from Masked_DataFrame A join DF_Tot_by_Table_Display_Imp B on A.VIQ_Table = B.VIQ_Table  left join Site_Lookup C on A.\"Dimension ID\" = C.site_id where lower(A.VIQ_Table) ='impression' and Channel_Type = 'Display' group by 1 order by 3 desc")
DCM_Impressions_DF = DCM_Impressions_DF.fillna(0.0)
# DCM Clicks 
DCM_Clicks_DF = pysql("select case when site is null then 'Unknown' else site end Site, sum(Total), sum(Total)*1.0/Tot_by_Table, sum(Masked)*1.0/sum(Total), sum(Masked)*1.0/Tot_by_Table, sum(Safari)*1.0/sum(Total), sum(Safari_Masked)*1.0/sum(Safari), sum(Chrome)*1.0/sum(Total), sum(Chrome_Masked)*1.0/sum(Chrome), sum(Firefox)*1.0/sum(Total), sum(Firefox_Masked)*1.0/sum(Firefox), sum(MicrosoftIE)*1.0/sum(Total), sum(MicrosoftIE_Masked)*1.0/sum(MicrosoftIE), sum(Opera)*1.0/sum(Total), sum(Opera_Masked)*1.0/sum(Opera), sum(Yandex)*1.0/sum(Total), sum(Yandex_Masked)*1.0/sum(Yandex), sum(Android)*1.0/sum(Total), sum(Android_Masked)*1.0/sum(Android), sum(Others)*1.0/sum(Total), sum(Others_Masked)*1.0/sum(Others), sum(No_Browser)*1.0/sum(Total), sum(No_Browser_Masked)*1.0/sum(No_Browser) from Masked_DataFrame A join DF_Tot_by_Table_Display_Imp B on A.VIQ_Table = B.VIQ_Table  left join Site_Lookup C on A.\"Dimension ID\" = C.site_id where lower(A.VIQ_Table) ='click' and Channel_Type = 'Display' group by 1 order by 3 desc")
DCM_Clicks_DF = DCM_Clicks_DF.fillna(0.0)


# In[10]:


FileName = "MaskedUserID_Report_VIQ_" + Client_Name + "_" + start_date + "-" + end_date + ".xlsx"

writer = pd.ExcelWriter(FileName, engine='xlsxwriter')
workbook  = writer.book


# In[11]:


Header_Format = workbook.add_format({
    'bold': 1,
    'border': 1,
    'align': 'center',
    'valign': 'vcenter'})
Decimal_Format = workbook.add_format({'num_format': '_(* #,##0.00_);_(* (#,##0.00);_(* "-"??_);_(@_)'})
Number_Format = workbook.add_format({'num_format': '_(* #,##0_);_(* (#,##0);_(* "-"??_);_(@_)'})
Percentage_Format_Decimal = workbook.add_format({'num_format': '0.00%'})
Percentage_Format_NoDecimal = workbook.add_format({'num_format': '0%'})
Red_Color_Format = workbook.add_format({'bg_color': '#FF7171'})
Amber_Color_Format = workbook.add_format({'bg_color': '#FFEF86'})
Green_Color_Format = workbook.add_format({'bg_color': '#51A75C'})


# In[12]:


def Masked_Excel_Report_Creation(SheetName, DataFrame, TopHeader, BottomHeader, PublisherShare_Bool, TopHeaderStart):
    Number_Column_Index = TopHeaderStart
    BottomHeader.to_excel(writer, sheet_name=SheetName, startrow = 1,  startcol = 0,index = None)
    DataFrame.to_excel(writer, sheet_name=SheetName, startrow = 2,  startcol = 0,index = None, header=False)
    Worksheet = writer.sheets[SheetName]
    for browser in TopHeader:
        if(browser == 'All Browsers'):
            if PublisherShare_Bool:
                Worksheet.merge_range(0, TopHeaderStart, 0, TopHeaderStart+3, browser, Header_Format)
                TopHeaderStart+=4
            else:
                Worksheet.merge_range(0, TopHeaderStart, 0, TopHeaderStart+1, browser, Header_Format)
                TopHeaderStart+=2
        else:
            Worksheet.merge_range(0, TopHeaderStart, 0, TopHeaderStart+1, browser, Header_Format)
            TopHeaderStart+=2    
    Worksheet.set_column(Number_Column_Index,Number_Column_Index, None,Number_Format)
    if PublisherShare_Bool:
        Worksheet.set_column(Number_Column_Index+1,Number_Column_Index+21, None,Percentage_Format_NoDecimal)
    else:
        Worksheet.set_column(Number_Column_Index+1,Number_Column_Index+19, None,Percentage_Format_NoDecimal)


# In[13]:


Display_Header = ['Site', 'Impressions', 'Publisher Share%', 'Masked %', 'Masked Share%', 'Total%', 'Masked%', 'Total%', 'Masked%', 'Total%', 'Masked%', 'Total%', 'Masked%', 'Total%', 'Masked%', 'Total%', 'Masked%', 'Total%', 'Masked%', 'Total%', 'Masked%', 'Total%', 'Masked%']
Display_Header_DF = pd.DataFrame(columns=Display_Header)
Overview_Header = ['Metric', 'Total Count', 'Masked %', 'Total%', 'Masked%', 'Total%', 'Masked%', 'Total%', 'Masked%', 'Total%', 'Masked%', 'Total%', 'Masked%', 'Total%', 'Masked%', 'Total%', 'Masked%', 'Total%', 'Masked%', 'Total%', 'Masked%']
Overview_Header_DF = pd.DataFrame(columns=Overview_Header)
Channel_Overview_Header = ['Impressions','Channel', 'Total Count', 'Channel Share%', 'Masked %', 'Masked Share%', 'Total%', 'Masked%', 'Total%', 'Masked%', 'Total%', 'Masked%', 'Total%', 'Masked%', 'Total%', 'Masked%', 'Total%', 'Masked%', 'Total%', 'Masked%', 'Total%', 'Masked%', 'Total%', 'Masked%']
Channel_Overview_Header_DF = pd.DataFrame(columns=Channel_Overview_Header)
Browser_Header = ['All Browsers', 'Safari', 'Chrome', 'Firefox', 'Microsoft IE', 'Opera', 'Yandex', 'Android', 'Others', 'No Browser Info']

Masked_Excel_Report_Creation('Overview', Overview_DF, Browser_Header, Overview_Header_DF, False, 1)
Masked_Excel_Report_Creation('Channel Overview', Channel_Overview_DF, Browser_Header, Channel_Overview_Header_DF, True, 2)
Masked_Excel_Report_Creation('DCM Impressions', DCM_Impressions_DF, Browser_Header, Display_Header_DF, True, 1)
Masked_Excel_Report_Creation('DCM Clicks', DCM_Clicks_DF, Browser_Header, Display_Header_DF, True, 1)

writer.save()


# In[14]:


xl = pd.ExcelFile(FileName)
ExcelSheets = xl.sheet_names
print(ExcelSheets)

xlsx = win32com.client.Dispatch("Excel.Application")
viqwb = xlsx.Workbooks.Open(os.getcwd() + "\\" + FileName)
for sheet in ExcelSheets:
    ws = viqwb.Worksheets(sheet)
    ws.Columns.AutoFit()
viqwb.Save()
viqwb.Close()


# In[15]:


DCM_Impressions_DF = DCM_Impressions_DF.fillna(0.0)


# In[16]:


DCM_Impressions_DF








