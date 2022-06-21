# -*- coding: utf-8 -*-
"""
Created on Mon Jan 31 23:26:55 2022

@author: CHuang
"""

## summary raw data 
import os , sys, re
sys.path.insert(0,'../lib/')
import pandas as pd
import glob
import copy
from download_util import export_to_excel
from joblib import Parallel, delayed

#%%
    
def keep_simple(df):
    df = df[['bio_applicant_country','bio_date','bio_doc-number','Cat_name','est_description']]
    year = int(str(df['bio_date'][df['bio_date'].notna()].iloc[0])[:4])    
    df['year'] = year
    return df

def sumarize(df,agg_col='est_description',agg_method='count'):
    agg_df = df.groupby(['year','bio_applicant_country','est_description']).agg({agg_col:[agg_method]})
    agg_df.columns = agg_df.columns.map('_'.join)
    agg_df.reset_index(inplace=True)
    return agg_df

def process_one_file(file_path):
    print('working on {}'.format(file_path))
    chunks = pd.read_csv(file_path,chunksize = 20000, error_bad_lines=False)
    for idx,chunk in enumerate(chunks):
        agg_chunks = []
        df = keep_simple(chunk)
        agg_df = sumarize(df)
        agg_chunks.append(agg_df)
    return agg_chunks
#%%
if __name__ == '__main__':
    
    number_of_cpu = 4 #joblib.cpu_count() - 2 
    parallel_pool = Parallel(n_jobs=number_of_cpu,verbose=5)
    
    data_out_path = 'F:/USPTO/results'  ## i saved temp data in an external hd
    data_path = 'G:/Chengyu/USPTO/processed_data'
    df_files = glob.glob(os.path.join(data_path,'*_search_res_est.csv'))
    #df_files = [d for d in df_files if "~" not in d]
    #%%
    agg_chunks = []
    for df_p in df_files:
        #df_p = df_files[0]
        print(df_p) 
        chunks = pd.read_csv(df_p,chunksize = 80000, error_bad_lines=False)
        for idx,chunk in enumerate(chunks):
        #chunk = chunks.get_chunk()
            df = keep_simple(chunk)
            agg_df = sumarize(df)
            agg_chunks.append(agg_df)
    
#    delayed_funcs = [delayed(process_one_file)(dp) for dp in df_files]
#    agg_chunks = parallel_pool(delayed_funcs)
#%%
    res_df = pd.concat(agg_chunks,ignore_index=True,axis=0)
    res_df = sumarize(res_df,'est_description_count','sum')
    
    #%%
    df_count = res_df.pivot_table(index = ['bio_applicant_country'],columns='year',values='est_description_count_sum',aggfunc='sum')
    df_count_cat = res_df.pivot_table(index = ['est_description'],columns='year',values='est_description_count_sum',aggfunc='sum')
    export_to_excel([res_df,df_count,df_count_cat],os.path.join(data_out_path,'agg_res_est.xlsx'))
    
    