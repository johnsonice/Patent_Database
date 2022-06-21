# -*- coding: utf-8 -*-
"""
Created on Mon Jan 31 23:26:55 2022

@author: CHuang
"""

## summary raw data 
import os , sys#, re
sys.path.insert(0,'../lib/')
import pandas as pd
import glob
#import copy
from download_util import export_to_excel
#from joblib import Parallel, delayed
from analysis_utils import process_year,consolidate_country,merge_country_name
#%%

def keep_small(df,drop_cols):
    df.drop(columns=drop_cols,inplace=True)
    year = int(str(df['bio_date'][df['bio_date'].notna()].iloc[0])[:4])    
    df['year'] = year
    year = int(str(df['bio_date'][df['bio_date'].notna()].iloc[0])[:4])  
    df['application_year'] = df['bio_application_date'].map(process_year)
    ## process country code 
    df = consolidate_country(df)
    
    #df.rename(columns={'total_counts_{}'.format(search_v): 'total_counts'},inplace=True)
    #df['count_binary'] = df['total_counts'] > 0 
    return df

#%%
if __name__ == '__main__':
    
#    number_of_cpu = 4 #joblib.cpu_count() - 2 
#    parallel_pool = Parallel(n_jobs=number_of_cpu,verbose=5)
    
    root_path = 'F:/Data/USTPO'  ## i saved temp data in an external hd
    data_out_folder = os.path.join(root_path,'results_small')
    data_folder = os.path.join(root_path,'results')
    agg_out_path = os.path.join(data_out_folder,'agg_small.xlsx')
    #meta_path = os.path.join(data_path,'global_meta.csv')
    country_path = os.path.join(r'C:\Users\chuang\OneDrive - International Monetary Fund (PRD)\Climate Change Challenge\USPTO\keywords',
                                'country_map.xlsx')
    
    #%%
    agg_dfs = []
    df_country = pd.read_excel(country_path)
    
    for year in range(1976,2021): # 1976,2021
        #df_p = df_files[0]
        df_p = os.path.join(data_folder,'{}_res.xlsx'.format(year))
        out_path = os.path.join(data_out_folder,'{}_res_small.xlsx'.format(year))
        print(df_p)

        df = pd.read_excel(df_p)
        drop_cols = ['description_paras','claims_claims','abstract_paras']
        df_sm = keep_small(df,drop_cols)
#        final_res_df = df_sm.merge(df_country,
#                                left_on='bio_assignee_country', ## switch to use assignee country ; was using bio_applicant_country
#                                right_on='2_Code',how = 'left')
        final_res_df = merge_country_name(df_sm,df_country,year)
        
        export_to_excel(final_res_df,out_path)
        agg_dfs.append(final_res_df)
    
#    ## map country names 
#    print('matching country names')
    
    res_df = pd.concat(agg_dfs,ignore_index=True,axis=0)
    export_to_excel(res_df,agg_out_path)
    
#    df_country = pd.read_excel(country_path)
#    final_res_df = res_df.merge(df_country,
#                                left_on='bio_applicant_country',
#                                right_on='2_Code',how = 'left')
    
    


    
    