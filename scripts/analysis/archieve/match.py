# -*- coding: utf-8 -*-
"""
Created on Mon Dec  6 20:24:26 2021

@author: CHuang
"""
### import some modules we gonna use 
import os , sys, re
sys.path.insert(0,'../lib/')
from collections import Counter
import pandas as pd
import ast
import joblib
from joblib import Parallel, delayed
from download_util import export_to_excel
import copy
#%%
### match keywords 
## define a function to loacte keywords
def construct_rex(keywords):
    """
    construct regex for multiple match 
    """
    #r_keywords = [r'\b' + re.escape(k) + r'(s|es)?\b'for k in keywords]    # tronsform keyWords list to a patten list, find both s and es 
    r_keywords = [r'\b' + re.escape(k) + r'\b'for k in keywords]
    rex = re.compile('|'.join(r_keywords),flags=re.I)                        # use or to join all of them, ignore casing
    #match = [(m.start(),m.group()) for m in rex.finditer(content)]          # get the position and the word
    return rex

def find_exact_keywords(content,keywords,rex=None):
    if rex is None: 
        rex = construct_rex(keywords)
    content = content.replace('\n', '').replace('\r', '')#.replace('.',' .')
    match = Counter([m.group() for m in rex.finditer(content)])             # get all instances of matched words 
                                                                            # and turned them into a counter object, to see frequencies
    total_count = sum(match.values())
    return match,total_count

def clean_keys(keywords):  
    klist=[]
    for key in keywords: 
        if '(' in key:
            key=key[0:key.find('(')].strip()+' '+key[key.find(')')+1:-1].strip()
            klist.append(key) 
        else:
            klist.append(key)            
    return klist

def get_keywords(key_path,clean=False):
    with open(key_path,encoding='utf-8') as file:
        keywords=file.readlines()
        keywords = [key.strip().lower() for key in keywords]
        keywords=list(filter(None,keywords))
    
    if clean:
        ## process ()s in keywords list 
        keywords = clean_keys(keywords)
        
    return keywords 

def match_by_row(inp,rex,verbose=True):
    """
    match keywords with all contents 
    """
    idx,row = inp
    #print(row)
    content = ''
    if pd.notna(row['abstract_paras']):
        try:
            temp_list = ast.literal_eval(row['abstract_paras'])
        except:
            if verbose:
                print('error reading content: /n {}'.format(row['abstract_paras']))
            temp_list = ['']
            
        content += '\n'.join(temp_list)
        
    #print(content)
    if pd.notna(row['claims_claims']):
        try:
            temp_list = ast.literal_eval(row['claims_claims'])
        except:
            if verbose:
                print('error reading content: /n {}'.format(row['claims_claims']))
            temp_list = ['']
        content += '\n'.join(temp_list)

    if pd.notna(row['description_paras']):
        try:
            temp_list = ast.literal_eval(row['description_paras'])
        except:
            if verbose:
                print('error reading content: /n {}'.format(row['description_paras']))
            temp_list = ['']
        content += '\n'.join(temp_list)
    
    match,total_count = find_exact_keywords(content,None,rex)
    
    return dict(match),total_count

def filter_final_results(df):
    
    key_df = df[df['total_counts']>0]
    stats=[{
            'total_pt': len(df),
            'total_with_k': len(key_df)
            }]
    stats_df = pd.DataFrame(stats)
    return key_df,stats_df
    

def test_function(x):
    i,y = x 
    return y 

#%%
if __name__ == "__main__":
    ## global variables 
    
    org_root_folder = "C:/Users/chuang/OneDrive - International Monetary Fund (PRD)/Climate Change Challenge/USPTO"
    root_folder = "F:/USPTO"
    processed_folder = os.path.join(root_folder,'processed_data')
    res_folder = os.path.join(root_folder,'results')
    key_path = os.path.join(org_root_folder,'keywords','keywords_v1.txt')
    keys =   get_keywords(key_path) 
    rex= construct_rex(keys)

    #%% data pathes 
    for year in range(2019,2022):
        #year = 2005    
        uspto_path = os.path.join(processed_folder,'{}_clean.csv'.format(year))
        out_path = os.path.join(processed_folder,'{}_search_res.csv'.format(year))
        res_path = os.path.join(res_folder,'{}_filtered_res.xlsx'.format(year))

        ## run in distributed way 
        number_of_cpu = joblib.cpu_count() -2
        parallel_pool = Parallel(n_jobs=number_of_cpu,verbose=5)
    
        ## read and process by chunk to control memory usage    
        chunks = pd.read_csv(uspto_path,chunksize=20000, error_bad_lines=False)
        fil_res = []    
        for idx,df in enumerate(chunks): 
            ## if first time, overwirte
            if idx==0:
                header_status = True
                mode = 'w'
            else: ## otherwise append to file
                header_status = False
                mode = 'a'
                
            #df = chunks.get_chunk()
            rows = df.iterrows()   
            ## multi process task 
            delayed_funcs = [delayed(match_by_row)(x,rex) for x in rows]
            results = parallel_pool(delayed_funcs)
            
            ## remember to set the index to be the same
            final_df = df.join(pd.DataFrame(results,columns=['keys_hit','total_counts']).set_index(df.index))
            final_df.to_csv(out_path,encoding='utf-8',index=False,header=header_status,mode=mode)  
            fil_dfs = filter_final_results(final_df)
            fil_res.append(copy.deepcopy(fil_dfs))
            
        ## format readable results file with hits 
        res_df1 = [d[0] for d in fil_res ]
        res_df2 = [d[1] for d in fil_res ]
        final_res_df1 = pd.concat(res_df1).reset_index(drop=True)
        final_res_df2 = sum(res_df2)
        ## export to multiple sheets 
        export_to_excel([final_res_df1,final_res_df2],res_path)
        
        ## clear memory 
        del delayed_funcs   ## clear memory 
        del parallel_pool   ## clear memory 

    #%%
#    ## load and process data 
#    df = pd.read_csv(uspto_path)
#    ## for testing
#    #df = df.head(1000)
#    #############
#    rows = df.iterrows()   
#    ## multi process task 
#    parallel_pool = Parallel(n_jobs=number_of_cpu,verbose=5)
#    delayed_funcs = [delayed(match_by_row)(x,rex) for x in rows]
#    results = parallel_pool(delayed_funcs)
#    delayed_funcs = None  ## clear memory 
#    parallel_pool = None  ## clear memory 
#    
#    ## export results to folder     
#    final_df = df.join(pd.DataFrame(results,columns=['keys_hit','total_counts']))
#    final_df.to_csv(out_path,encoding='utf-8',index=False)    
#    fil_dfs = filter_final_results(final_df)
#    export_to_excel(fil_dfs,res_path)
    
