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
#import ast
#import joblib
#from joblib import Parallel, delayed
#import glob
#from download_util import export_to_excel
#import copy
import argparse
from csv import writer

#%%

def filter_results(df):
    
    fil_df = df[~chunk['est_description'].isna()]
    global_meta = {'length':len(df)}
    
    return fil_df,global_meta

def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('-s', '--start_year', type=int,action='store', dest='start_year',
                        default=1976)#'adaboost')
    parser.add_argument('-e', '--end_year', type=int,action='store', dest='end_year',
                        default=2021) # NoSignal; Selected; Signal; SignalandSelected; NoSent
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
    #df_files = glob.glob(os.path.join(data_path,'*_search_res_est.csv'))        
    #%%
    for year in range(args.start_year,args.end_year):
        # reset global meta info
        g_length = 0 
        #df_p = df_files[0]
        df_p = os.path.join(data_path,'{}_est.csv'.format(year))
        out_p = os.path.join(data_out_path,'{}_fil.csv'.format(year))
        meta_out_p = os.path.join(data_out_path,'global_meta_by_grant_year.csv')
        print(df_p) 
        
        chunks = pd.read_csv(df_p,chunksize = 10000, error_bad_lines=False)  ## 80000
        for idx,chunk in enumerate(chunks):
        #chunk = chunks.get_chunk()
            if idx==0:
                header_status = True
                mode = 'w'
            else: ## otherwise append to file
                header_status = False
                mode = 'a'
                
            fil_df,g_meta = filter_results(chunk)
            g_length+=g_meta['length']
            fil_df.to_csv(out_p,encoding='utf-8',index=False,header=header_status,mode=mode)  
            print('---- finish chunk {}; global length {} ----'.format(idx,g_length))
        
        ## write out global meta info
        write_meta_info([year,g_length],meta_out_p)
        print('----- write out global length {} ---'.format(g_length))
        

#%%

#    df_p = df_files[0]
#    chunks = pd.read_csv(df_p,chunksize = 40000, error_bad_lines=False)
#    #%%
#    chunk = chunks.get_chunk()
    