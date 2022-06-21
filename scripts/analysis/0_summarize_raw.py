# -*- coding: utf-8 -*-
"""
Created on Mon Feb  7 11:45:46 2022

@author: CHuang
"""

### import some modules we gonna use 
import os , sys, re
sys.path.insert(0,'../lib/')
#from collections import Counter
import pandas as pd
from analysis_utils import process_year, consolidate_country,merge_country_name
from download_util import export_to_excel
#import copy
import argparse
from csv import writer

#%%  
    
def filter_results(df):
    fil_df = df[['bio_application_date','bio_date','bio_doc-number','3_Code','Country_Name']]
    global_meta = {'length':len(fil_df)}
    return fil_df,global_meta

def agg_by(df,col_n='bio_application_date'):
    df['year'] = df[col_n].map(process_year)
    agg_df = df.groupby(['year','Country_Name']).agg({'bio_doc-number':['count']})
    agg_df.columns = agg_df.columns.map('_'.join)
    agg_df.reset_index(inplace=True)
    return agg_df

def sequential_aggregate(agg_p,col_n='bio_application_date'):
    
    chunks = pd.read_csv(agg_p,chunksize = 50000,error_bad_lines=False)
    sum_dfs = []
    for idx,chunk in enumerate(chunks):
        sum_df_chunk = agg_by(chunk,col_n=col_n)
        sum_dfs.append(sum_df_chunk)
    
    final_agg_df = pd.concat(sum_dfs)
    final_agg_df = final_agg_df.groupby(['year','Country_Name'])['bio_doc-number_count'].sum()
    
    return final_agg_df

def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('-s', '--start_year', type=int,action='store', dest='start_year',
                        default=1976)#'adaboost')
    parser.add_argument('-e', '--end_year', type=int,action='store', dest='end_year',
                        default=2021) # NoSignal; Selected; Signal; SignalandSelected; NoSent
    parser.add_argument('-f', '--fillter',type=str,action='store',dest='fillter',default='n')
    args = parser.parse_args()    
    return args

def write_meta_info(meta,outpath):
    with open(outpath, 'a', newline='') as f_object:  
        writer_object = writer(f_object)
        writer_object.writerow(meta)  
        f_object.close()

#%%

if __name__ == '__main__':
    
    args = parse_args()
    
    data_out_path = 'F:/Data/USTPO/filtered'  ## i saved temp data in an external hd
    data_path = 'F:/Data/USTPO/with_est'
    agg_p = os.path.join(data_out_path,'global_meta_by_application_year_v2.csv')
    country_path = os.path.join(r'C:\Users\chuang\OneDrive - International Monetary Fund (PRD)\Climate Change Challenge\USPTO\keywords',
                                'country_map.xlsx')
    df_country = pd.read_excel(country_path)
    #df_files = glob.glob(os.path.join(data_path,'*_search_res_est.csv'))        
    #%%
    if args.fillter == 'y':
        global_index = 0
        out_p = agg_p
        for year in range(args.start_year,args.end_year):
            # reset global meta info
            g_length = 0 
            #df_p = df_files[0]
            df_p = os.path.join(data_path,'{}_est.csv'.format(year))
            
            #meta_out_p = os.path.join(data_out_path,'global_meta_76-20.csv')
            print(df_p) 
            
            chunks = pd.read_csv(df_p,chunksize = 10000, error_bad_lines=False)  ## 80000
            for idx,chunk in enumerate(chunks):
            #chunk = chunks.get_chunk()
                if global_index==0:
                    header_status = True
                    mode = 'w'
                else: ## otherwise append to file
                    header_status = False
                    mode = 'a'
                
                chunk = consolidate_country(chunk)
                chunk = merge_country_name(chunk,df_country,year=year)
                fil_df,g_meta = filter_results(chunk)
                g_length+=g_meta['length']
                fil_df.to_csv(out_p,encoding='utf-8',index=False,header=header_status,mode=mode)  
                print('---- finish chunk {}; global length {} ----'.format(idx,g_length))
                global_index +=1
            ## write out global meta info
            #write_meta_info([year,g_length],meta_out_p)
            print('-----  global length {} ---'.format(g_length))
    
    ## export summary 
    app_sum_df = sequential_aggregate(agg_p,col_n='bio_application_date')
    app_sum_df.name = 'sum_by_application_year'
    app_sum_df = pd.DataFrame(app_sum_df)
    app_sum_df.reset_index(inplace=True)
    #%%
    grant_sum_df = sequential_aggregate(agg_p,col_n='bio_date')
    grant_sum_df.name = 'sum_by_grant_year'
    grant_sum_df = pd.DataFrame(grant_sum_df)
    grant_sum_df.reset_index(inplace=True)
    #final_df = pd.concat([app_sum_df, grant_sum_df], axis=1)

    final_df= app_sum_df.merge(grant_sum_df,
                            on=['year','Country_Name'], ## switch to use assignee country ; was using bio_applicant_country
                            validate='1:1',
                            indicator=True)
    final_df = final_df[['year','Country_Name','sum_by_application_year','sum_by_grant_year']]
    final_df=final_df[(final_df.year<2021)&(final_df.year>1975)]
    #final_df.index = final_df.index.astype(int)
    #%%
    final_df.to_excel(os.path.join(data_out_path,'global_summary_by_country.xlsx'))
        #%%
#        chunks = pd.read_csv(agg_p,chunksize = 50000,error_bad_lines=False)
#        sum_dfs = []
#        for idx,chunk in enumerate(chunks):
#            sum_df_chunk = agg_by(chunk,col_n='bio_application_date')
#            sum_dfs.append(sum_df_chunk)
#        
#        final_agg_df = pd.concat(sum_dfs)
#        final_agg_df = final_agg_df.groupby(['year'])['bio_doc-number_count'].sum()

##%%        
#year = 1978
#df_p = os.path.join(data_path,'{}_est.csv'.format(year))
#chunks = pd.read_csv(df_p,chunksize = 5000, error_bad_lines=False)
#chunk = chunks.get_chunk()
#    