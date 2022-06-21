# -*- coding: utf-8 -*-
"""
Created on Mon Feb 28 20:34:55 2022

@author: CHuang

filter on previously summarized data; exclude API, and financial category 
also mapped cleaned companies to the final results 

"""

## map normalized companies naes and remove some categories. 
import os,sys
sys.path.insert(0,'../lib/')
import pandas as pd  
import ast
from download_util import export_to_excel
#%%

def keep_simple(df,cols,search_v):
    df = df[cols]
    df.rename(columns={'total_counts_{}'.format(search_v): 'total_counts'},inplace=True)
    df['count_binary'] = df['total_counts'] > 0 
    return df

def sumarize(df,group_cols,agg_col='total_counts'):
    agg_df = df.groupby(group_cols).agg({agg_col:['count','sum']})
    agg_df.columns = agg_df.columns.map('_'.join)
    agg_df.reset_index(inplace=True)
    #agg_df['L1_des'] = agg_df['est_description'].map(lambda x: x.split('|')[0])
    return agg_df

def _filter_key(r,k='API'):
    try:
        res = ast.literal_eval(r).get(k,0)
    except:
        print(r)
        res = 0 
    return res

def filter_by_key(agg_df):
    
    agg_df['api_count_v1'] = agg_df['keys_hit_v1'].map(_filter_key)
    agg_df['api_count_v2'] = agg_df['keys_hit_v2'].map(_filter_key)
    agg_df['total_counts_v1'] = agg_df['total_counts_v1'] - agg_df['api_count_v1']
    agg_df['total_counts_v2'] = agg_df['total_counts_v2'] - agg_df['api_count_v2']
    agg_df.drop(columns=['api_count_v1','api_count_v2'],inplace=True)
    
    return agg_df

def filter_category(agg_df,cat_key='Emission trading,  eg pollution credits'):
    agg_df['exclude']= agg_df['est_description'].map(lambda x: cat_key in x )
    agg_df = agg_df[~agg_df['exclude']]
    agg_df.drop(columns=['exclude'],inplace=True)
    return agg_df
#%%
if __name__ == '__main__':
    
    agg_ee_path = 'F:/Data/USTPO/results_small/agg_small_assignee.xlsx'
    ee_map_file = 'F:/Data/USTPO/assingee/company_names_map_final.xlsx'
    out_raw = 'F:/Data/USTPO/results_small/agg_small_assignee_cleaned_raw.xlsx'
    out_summary = 'F:/Data/USTPO/results_small/agg_small_assignee_cleaned_summary_{}.xlsx'
    
    #%%
    agg_df = pd.read_excel(agg_ee_path)
    ee_map_df = pd.read_excel(ee_map_file)
    ee_map_df = ee_map_df[['names','Final_Names']]
    ee_map_df.columns = ['names','final_ee_name']
    #%%
    agg_df = agg_df[agg_df['application_year'].notna()]
    #agg_df['year'] = agg_df['bio_date'].map(lambda x: int(str(x)[:4]))
    #agg_df.drop(columns=['_merge'],inplace=True)
    ## get length of original 
    df_len = len(agg_df)
    ########################################
    ### Map final ee names and update ######
    ########################################
    ## some checks 
    print('merge clean assignee names')
    assert len(ee_map_df['names'].unique()) == len(ee_map_df) ## make sure it is unique
    agg_df = agg_df.merge(ee_map_df,
                               how='left',
                               left_on='ee_name',
                               right_on='names',
                               validate='m:1',
                               indicator=True)
    ## after merge check 
    assert len(agg_df) == df_len
    ## populate final assignee name with rwa if not mapped 
    agg_df['final_ee_name'].fillna(agg_df['bio_assignee_name'],inplace=True)
    
    ######################################
    # filter out unwanted categories #####
    ######################################
    #agg_df = filter_by_key(agg_df) ## after keywrod update, don't need to filter anymore 
    agg_df = filter_category(agg_df)
    
    #######################################
    ## export raw data ####################
    #######################################
    ## export raw cleaned data 
    export_to_excel([agg_df],out_raw)
    
    #%%
    #####################################
    ## export summarized data by country 
    #####################################
    print('summarize by country')
    for search_v in ['v1','v2','v3','v4','v5']:
        #keep_cols = ['bio_applicant_country','bio_date','bio_doc-number','year','2_Code','3_Code','Country_Name']
        keep_cols = ['bio_application_date','bio_doc-number','application_year','2_Code','3_Code','Country_Name']
        keep_cols.append('total_counts_{}'.format(search_v))
        group_cols = ['application_year','2_Code','3_Code','Country_Name','count_binary']
        
        out_df = keep_simple(agg_df,keep_cols,search_v)
        out_df = sumarize(out_df,group_cols,agg_col='total_counts')
        export_to_excel([out_df],out_summary.format('by_country_'+search_v))
    
#%%

    ## export summarized data by companies 
    print('summarize by companies')
    for search_v in ['v1','v2','v3','v4','v5']:
        #keep_cols = ['bio_applicant_country','bio_date','bio_doc-number','year','2_Code','3_Code','Country_Name','final_ee_name']
        keep_cols = ['bio_application_date','bio_doc-number','application_year','2_Code','3_Code','Country_Name','final_ee_name']
        keep_cols.append('total_counts_{}'.format(search_v))
        group_cols = ['application_year','2_Code','3_Code','Country_Name','count_binary','final_ee_name']
        
        out_df = keep_simple(agg_df,keep_cols,search_v)
        out_df = sumarize(out_df,group_cols,agg_col='total_counts')
        export_to_excel([out_df],out_summary.format('by_company_'+search_v))
    
    
    
        