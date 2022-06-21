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

#%%
    
def keep_simple(df,cols,search_v):
    df = df[cols]
    year = int(str(df['bio_date'][df['bio_date'].notna()].iloc[0])[:4])    
    df['year'] = year
    df.rename(columns={'total_counts_{}'.format(search_v): 'total_counts'},inplace=True)
    df['count_binary'] = df['total_counts'] > 0 
    return df

def sumarize(df,agg_col='total_counts'):
    agg_df = df.groupby(['year','bio_applicant_country','count_binary','est_description']).agg({agg_col:['count','sum']})
    agg_df.columns = agg_df.columns.map('_'.join)
    agg_df.reset_index(inplace=True)
    
    agg_df['L1_des'] = agg_df['est_description'].map(lambda x: x.split('|')[0])
    
    return agg_df

#def process_one_file(file_path):
#    print('working on {}'.format(file_path))
#    chunks = pd.read_csv(file_path,chunksize = 20000, error_bad_lines=False)
#    for idx,chunk in enumerate(chunks):
#        agg_chunks = []
#        df = keep_simple(chunk)
#        agg_df = sumarize(df)
#        agg_chunks.append(agg_df)
#    return agg_chunks

def summarize_global_meta(g_path):
    df = pd.read_csv(g_path,header=None)
    df.columns = ['year','count']
    df.sort_values(by='year',inplace=True)
    return df


#%%
if __name__ == '__main__':
    
#    number_of_cpu = 4 #joblib.cpu_count() - 2 
#    parallel_pool = Parallel(n_jobs=number_of_cpu,verbose=5)
    
    data_path = 'F:/USPTO/results'  ## i saved temp data in an external hd
    #data_path = 'G:/Chengyu/USPTO/processed_data'
    df_files = glob.glob(os.path.join(data_path,'final_search_res_*.xlsx'))
    #df_files = [d for d in df_files if "~" not in d]
    
    meta_path = os.path.join(data_path,'global_meta.csv')
    country_path = os.path.join(r'C:\Users\chuang\OneDrive - International Monetary Fund (PRD)\Climate Change Challenge\USPTO\keywords',
                                'country_map.xlsx')
    
    #search_v = 'v2'
    for search_v in ['v1','v2','v3','v4']:
        data_out_path = os.path.join(data_path,'agg_search_est_{}.xlsx'.format(search_v))
        keep_cols = ['bio_applicant_country','bio_date','bio_doc-number','Cat_name','est_description']
        keep_cols.append('total_counts_{}'.format(search_v))
        #%%
        agg_dfs = []
        for df_p in df_files:
            #df_p = df_files[0]
            print(df_p) 
            df = pd.read_excel(df_p)
            df_s = keep_simple(df,keep_cols,search_v)
            df_sm = sumarize(df_s)
            agg_dfs.append(df_sm)
        
        res_df = pd.concat(agg_dfs,ignore_index=True,axis=0)
        df_country = pd.read_excel(country_path)
        final_res_df = res_df.merge(df_country,
                                    left_on='bio_applicant_country',
                                    right_on='2_Code',how = 'left')
        
        df_meta = summarize_global_meta(meta_path)
        
        export_to_excel([final_res_df,df_meta],data_out_path)


    
    