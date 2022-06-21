# -*- coding: utf-8 -*-
"""
Created on Fri Apr 29 11:19:05 2022

@author: chuang
"""
## analyze lags between application and approval
import os 
import pandas as pd
from analysis_utils import process_year
from statsmodels.stats.weightstats import DescrStatsW
#%%

def agg_by(df):
    df['application_year'] = df['bio_application_date'].apply(process_year)
    df['grant_year'] = df['bio_date'].apply(process_year)
    df['gap'] = df['grant_year']  - df['application_year']
    #df.loc[df['gap']>7,'gap'] = 10  ## just mark anything > 7 as integer 10 
    agg_df = df.groupby(['application_year','gap']).agg({'bio_doc-number':['count']})
    agg_df.columns = agg_df.columns.map('_'.join)
    agg_df.reset_index(inplace=True)
    return agg_df

def sequential_aggregate(agg_p):
    chunks = pd.read_csv(agg_p,chunksize = 50000,error_bad_lines=False)
    sum_dfs = []
    for idx,chunk in enumerate(chunks):
        sum_df_chunk = agg_by(chunk)
        sum_dfs.append(sum_df_chunk)
    
    final_agg_df = pd.concat(sum_dfs)
    final_agg_df = final_agg_df.groupby(['application_year','gap'])['bio_doc-number_count'].sum()
    
    return final_agg_df

#%%

if __name__ == '__main__':
    
    data_out_path = 'F:/Data/USTPO/filtered'  
    agg_p = os.path.join(data_out_path,'global_meta_by_application_year.csv')
    sum_out = os.path.join(data_out_path,'lag_summary_by_application_year.xlsx')
    #%%
    df = sequential_aggregate(agg_p)
    df = pd.DataFrame(df)
    df.reset_index(inplace=True)
    df = df[(df['gap']>=0) & (df['gap']<35) & (df['application_year']>1975)]
    df.rename(columns={'bio_doc-number_count':'count'},inplace=True)
    #%%
    buckets = [(1976,1978),(1979,1982),
               (1983,1986),(1987,1990),
               (1991,1994),(1995,1998),
               (1999,2002),(2003,2006),
               (2007,2010),(2011,2014),
               (2015,2018),(2019,2020)]
    
    #%%
    sum_list = []
    for b in buckets:
        #b = (1976,1978)
        s,e = b
        one_buck = df[(df['application_year']>=s)&(df['application_year']<=e)]
        summary =  pd.DataFrame(one_buck.groupby('gap')['count'].sum())
        mean = DescrStatsW(summary.index.values,summary['count']).mean
        sd = DescrStatsW(summary.index.values,summary['count']).std
        summary.reset_index(inplace=True)
        summary.loc[summary['gap']>7,'gap'] = '>7'  ## just mark anything > 7 in one group
        summary = pd.DataFrame(summary.groupby('gap')['count'].sum())
        summary['percent'] = summary['count']/summary['count'].sum()
        summary.loc['mean','percent'] = mean
        summary.loc['sd','percent'] = sd
        summary.columns = ['count','precent_{}-{}'.format(s,e)]
        summary.drop(columns=['count'],inplace=True)
        ## add to list 
        sum_list.append(summary)
    #%%
    final_summary = pd.concat(sum_list,axis=1,sort=True)
    final_summary.to_excel(sum_out,float_format = "%0.4f")
    #%%
#    chunks = pd.read_csv(agg_p,chunksize = 50000,error_bad_lines=False)
#    chunk = chunks.get_chunk()
#    #%%
#    chunk['application_year'] = chunk['bio_application_date'].apply(process_year)
#    chunk['grant_year'] = chunk['bio_date'].apply(process_year)
#    #%%
#    chunk['gap'] = chunk['grant_year']  - chunk['application_year']
#    chunk.loc[chunk['gap']>7,'gap'] = 10  ## just mark anything > 7 as integer 10 
#    agg_chunk = agg_by(chunk)