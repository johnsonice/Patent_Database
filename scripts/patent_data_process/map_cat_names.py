# -*- coding: utf-8 -*-
"""
Created on Wed Jan 12 11:14:52 2022

@author: CHuang
"""

### map category code with cat names 
import os,sys,re
sys.path.insert(0,'../lib/')

import pandas as pd
from download_util import load_from_pkl
from category_utils.get_uspc_map import Tree,node
from category_utils.uspc import USPC_Tree
import csv
import argparse

#### some dirty code to get around read csv issues ####
#maxInt = sys.maxsize
#while True:
#    try:
#        csv.field_size_limit(maxInt)
#        break
#    except OverflowError:
#        maxInt = int(maxInt/10)
#
########################################################

#%%
def get_docid_by_Pid(df,pid):
    '''
    for debuging purpose 
    '''
    doc_id = df[df['bio_main-classification_national']==pid]['bio_doc-number'].iloc[0]
    return doc_id

def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('-s', '--start_year', type=int,action='store', dest='start_year',
                        default=1976)
    parser.add_argument('-e', '--end_year', type=int,action='store', dest='end_year',
                        default=2008) 
    args = parser.parse_args()    
    return args

#%%
if __name__ == '__main__':
    
    args = parse_args()
    
    #data_path = 'F:/Data/USTPO/raw'  ## i saved temp data in an external hd
    # use txt data instead of xml
    data_path = 'F:/Data/USTPO/raw_txt/out'
    out_folder = 'F:/Data/USTPO/with_cat'
    map_folder = os.path.join('..','..',r'keywords\classification\uspc')
    tree_pickle = os.path.join(map_folder,'uscp_tree.p') 
    
    #rc = 'US D 1130'
    uspc_map = USPC_Tree(tree_pickle)
    
    #%%
    ## technically we should harmonize ipc cpc uspc into one, but USPC has the most coverage, it covers all items with ipc and cpc code 
    ## so in this case, we just used uspc system 
    ## ipc code and maps are also available in category_utils 
    for year in range(args.start_year,args.end_year):
        print('working on {}'.format(year))
        df_p = os.path.join(data_path,'{}.csv'.format(year))
        out_p = os.path.join(out_folder,'{}_cat.csv'.format(year))
        
        chunks = pd.read_csv(df_p,chunksize = 20000, error_bad_lines=False,
                             encoding='utf8') 
        #df = chunks.get_chunk()
        #fil_res = []    
        for idx,df in enumerate(chunks): 
            ## if first time, overwirte
            if idx==0:
                header_status = True
                mode = 'w'
            else: ## otherwise append to file
                header_status = False
                mode = 'a'
                
            #df = chunks.get_chunk()
#            columns_keep = ['abstract_paras','bio_applicant_country','bio_country','bio_date','bio_doc-number',
#                            'bio_main-classification_national','claims_claims','description_paras']
#            df = df[columns_keep]   
            results = df['bio_main-classification_national'].map(uspc_map.get_label).tolist()
    
            ## remember to set the index to be the same
            final_df = df.join(pd.DataFrame(results,columns=['Cat_name','status']).set_index(df.index))
            final_df.to_csv(out_p,encoding='utf-8',index=False,header=header_status,mode=mode)  
            #fil_dfs = filter_final_results(final_df)
            #fil_res.append(copy.deepcopy(fil_dfs))



#%%
#    data_path = 'F:/Data/USTPO/with_est'  ## i saved temp data in an external hd
#    y = '2015_est'
#    df_p = os.path.join(data_path,'{}.csv'.format(y))
#    chunks = pd.read_csv(df_p,chunksize = 10500, error_bad_lines=False) # us on_bad_lines='skip'
#    #%%
#    df = chunks.get_chunk()
#    print('bio_main-classification_national' in df.columns)
#    #%%
#    results = df['bio_main-classification_national'].map(uspc_map.get_label).tolist()
    




