# -*- coding: utf-8 -*-
"""
Created on Fri Apr 29 10:13:49 2022

@author: chuang
"""

import pandas as pd

### analysis utils 
def process_year(y):
    try:
        year = int(str(y)[:4])
        return year
    except:
        return None    
    
def consolidate_country(df):
    ## in the end, we sue assignee country as baseline 
    df['bio_assignee_country'].fillna(df['bio_applicant_country'],inplace=True)
    df['bio_assignee_country'].fillna(df['bio_country'],inplace=True)
    df['bio_assignee_country'].fillna('US',inplace=True)
    ## transform old country code with x, remove x 
    df['bio_assignee_country']=df['bio_assignee_country'].map(lambda x: x[:2] if len(x)==3 else x)
    return df

def merge_country_name(df,country_map,year):
    if year<1979: ## if before version change, use 2digit old version; else new version 
        c_map = country_map[['2_Code_old','3_Code','Country_Name']]
        c_map.columns = ['2_Code','3_Code','Country_Name'] ## rename old verstion as mapping id
    else:
        c_map = country_map[['2_Code','3_Code','Country_Name']]
        
    merged_df = df.merge(c_map,
                            left_on='bio_assignee_country', ## switch to use assignee country ; was using bio_applicant_country
                            right_on='2_Code',how = 'left')
    
    return merged_df