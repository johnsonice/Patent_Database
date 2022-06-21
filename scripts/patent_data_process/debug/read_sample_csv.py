# -*- coding: utf-8 -*-
"""
Created on Sun Apr  3 17:23:26 2022

@author: chuang
"""
import pandas as pd
import os 

def cumulative_read_field(fp,field_name):
    res = []
    chunks = pd.read_csv(fp,chunksize = 10000, error_bad_lines=False) 
    for chunk in chunks:
        r=chunk[field_name].to_list()
        res.extend(r)
    
    return res

def check_dup_ids(f1,f2,field_name,check_num=1000):
    id_list = cumulative_read_field(f1,field_name)
    id_list2 = cumulative_read_field(f2,field_name)
    res = []
    for i in id_list:
        res.append(i in id_list2)
        
    return res,id_list,id_list2

def transform_docid(doc_id):
    try:
        new_id = doc_id.lstrip('0').replace('.0','')
        return new_id
    except:
        print('error:{}'.format(doc_id))
        return 'nan'

def consolidate_country(df):
    df['bio_applicant_country'].fillna(df['bio_assignee_country'],inplace=True)
    df['bio_applicant_country'].fillna(df['bio_country'],inplace=True)
    df['bio_applicant_country'].fillna('US',inplace=True)
    ## transform old country code with x, remove x 
    df['bio_applicant_country'].map(lambda x: x[:2] if len(x)==3 else x)
    return df

def process_year(y):
    try:
        year = str(y)[:4]
        return year
    except:
        return None
    
if __name__ =="__main__":
    
    df_p = "F:/Data/USTPO/raw/2018.csv"
    #%%
    #df_p2 = "F:/Data/USTPO/raw_txt/out/2001_txt.csv"
    ## read one chunk
    chunks = pd.read_csv(df_p,chunksize = 5000, error_bad_lines=False) 
    chunk = chunks.get_chunk()
    #%%
    ## compare ids 
    #res,id_list,id_list2 = check_dup_ids(df_p,df_p2,'bio_doc-number',check_num=1000)

    #x = chunk[chunk['bio_main-classification_national'] == '[]']
    #len(x)
    #%%
    
#    #%%
#    ## check id unique
#    for year in range(1976,2021):
#        df_p = "F:/Data/USTPO/results_small/{}_res_small.xlsx".format(year)
#        df = pd.read_excel(df_p)
#        unique = len(df['bio_doc-number']) == len(df['bio_doc-number'].unique())
#        print('check if id is unique for {}:{}'.format(year,unique))

#%%
#    ## check assignee map 
#    root_path = root_path = 'F:/Data/USTPO'
#    df_p = os.path.join(root_path,'assingee','docid_assignee_map.csv')
#    df = pd.read_csv(df_p)
#    #deduplicate 
#    df = df.drop_duplicates(subset=['grant_doc_num'],keep='first')
#    ## read one sample doc 
#    df_p2 = "F:/Data/USTPO/results_small/{}_res_small.xlsx".format(2015)
#    df_small = pd.read_excel(df_p2)
#    df_small['doc_id_map'] = df_small['bio_doc-number'].astype(str)
#    df_small['doc_id_map'] = df_small['doc_id_map'].map(transform_docid)
#    merged_df = df_small.merge(df,how='left',
#                             left_on='doc_id_map',
#                             right_on='grant_doc_num',
#                             validate='one_to_many',
#                             indicator=True)
    
    #%%
#    ## check on country map
#    
#    root_path = root_path = 'F:/Data/USTPO'
#    df_p = os.path.join(root_path,'results_small','agg_small.xlsx')
#    df = pd.read_excel(df_p)
    #%%
    
