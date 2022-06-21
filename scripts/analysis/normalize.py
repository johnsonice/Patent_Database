#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Feb 21 16:09:30 2022

@author: chengyu

follow : 
    https://www.analyticsinsight.net/company-names-standardization-using-a-fuzzy-nlp-approach/
    
"""
import pandas as pd 
from collections import Counter
import re
from cleanco import basename
from fuzzywuzzy import fuzz#,process
import numpy as np
from itertools import combinations
from joblib import Parallel, delayed
import joblib
import matplotlib.pyplot as plt
from sklearn import cluster
from difflib import SequenceMatcher 
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

#%%

### temp global variable 

rep = {' AND ':' & ',
   'COMPANY':' ',
   'HOLDINGS,':'HOLDING',
   'B\.V\.':' ',
   'SOLUTIONS,':'SOLUTION',
#   r'\bAG\b':' ',
   'S\.A\.':' ',
   'N\.A\.':' ',
   r'L\.P\.':' ',
   'AG & Co':' ',
#   r'\bSAB\b':' ',
   r'\bNV\b':' ',
   r'\bA/S\b':' ',
   r"\([^()]*\)":' ',
   'AS ADMINISTRATIVE AGENT':'',
   'AS COLLATERAL AGENT':'',
   'AS SUCCESSOR AGENT':'',
   'AS AGENT':'',
   'AS COLLATERAL TRUSTEE':'',
   'NETWORK ENTERTAINMENT PLATFORM':'',
   '  ':' ',
   ', ,':', '
   }

rep2 ={
        'GMBH':' ',
        'INC\.,':' ',
        'INC\.':' ',
        'INC':' ',
        'LIMITED':' ', 
        'LTD':' ',
        'LTD\.':' ',
        'CORPORATION':' ',
        'CORP.':' ',
        'CO\.\,':' ',
        'LLC\.':' ',
        'LLC':' ',
        'LLC,':' ',
        }
rep3 ={
        'BANK':'',
        'OF':'',
        'GROUP':'',
        'SYSTEMS':'',
        'INTERNATIONAL':'',
        'TECHNOLOGIES':'',
        'TECHNOLOGY':'',
        'THERAPEUTICS':'',
        'LABORATORIES':'',
        'PHARMACEUTICALS':'',
        'HOLDING':'',
        'NATIONAL ASSOCIATION':'',
        'ASSOCIATION':'',
        'ASSOCIATES':'',
        'ELECTRONICS':'',
        'DEUTSCHLAND':'',
        'DEVICES':'',
        'AMERICA':'',
        'AMERICAN':'',
        'AUTOMOTIVE':'',
        'RESEARCH FOUNDATION':'',
        'INDUSTRIES':'',
        'UNIVERSITY':'',
        'SEMICONDUCTOR':'',
        }
rep2.update(rep)
## see top words, likely some of them are stop words 
def find_stop_words(list_names):
    name_str = ' '.join(list_names)
    ns = name_str.split()
    ns = [s for s in ns if s != '']
    res = Counter(ns)
    return res

def replace_words(text, dic):
    for i, j in dic.items():
        #print(i,j)
        text = re.sub(i,j,text)
    return text

def deepclean(text,dic,deep=2):
    if deep == 0:
        res = replace_words(text,dic=dic)
    if deep == 1:
        res = basename(replace_words(text,dic=dic))
    if deep == 2:
        res = basename(basename(replace_words(text,dic=dic)))
    
    res = res.strip().strip(',').strip().strip(',')
    
    return res

def deepclean_r(text,dic1,dic2):
    ## recursive deepclearn; deal with edge cases, manually fix then with dict2
    res = deepclean(text,dic=dic1,deep=2)
    if res == '' or res == ' ':
        res = deepclean(text,dic=dic2,deep=0)
    
    if res == '' or res == ' ':
        res = text
    
    return res 
    
def process_one_pair(tokens):
    s1 = fuzz.token_set_ratio(tokens[0],tokens[1]) + 10e-10
    s2 = fuzz.partial_ratio(tokens[0],tokens[1]) + 10e-10
    sim = 2*s1*s2/(s1+s2)
    
    return sim

def cal_distance(com_names):
    
    ## get all parirs 
    logger.info('...prepare all pairs ...')
    all_pairs = list(combinations(com_names, 2))
    logger.info('# of pairs to process {}'.format(len(all_pairs)))
    ## calculate distance 
    n_cpu = joblib.cpu_count()-2
    with Parallel(n_jobs = n_cpu,verbose=5) as parallel_pool:
        deplayed_functions = [delayed(process_one_pair)(p) for p in all_pairs]
        pool_res = parallel_pool(deplayed_functions)
    
    ## make them into dict 
    sim_dict = dict(zip(all_pairs,pool_res))
    
    return sim_dict

def convert2mx(com_names,sim_dict):
    h=w= len(com_names)
    similarity_mx = np.ones((h,w))*100
    for i in range(h):
        for j in range(i+1,h):
            key = (com_names[i],com_names[j])
            val = sim_dict.get(key)
            if val is None:
                key = (com_names[j],com_names[i])
                val = sim_dict.get(key)
                if val is None:
                    raise('error, can not get similarities ')
            else:
                similarity_mx[i][j] = val 
                similarity_mx[j][i] = val
    np.fill_diagonal(similarity_mx,100)
    #plt.imshow(similarity_mx)
    return similarity_mx

def get_common_name(df_clusters):
    """
    Suggest the most common component as standard name
    """
    standard_name = {}
    for c in df_clusters['cluster'].unique():
        names = df_clusters[df_clusters['cluster'] == c]['CompanyName'].to_list()
        l_common_substring = []
        if len(names)>1:
            for i in range(len(names)):
                for j in range(i+1,len(names)):
                    seqMatch = SequenceMatcher(None,names[i],names[j])
                    m = seqMatch.find_longest_match(0,len(names[i]),0,len(names[j]))
                    if m.size!=0:
                        l_common_substring.append(names[i][m.a:m.a+m.size].strip())
            #n = len(l_common_substring)
            counts = Counter(l_common_substring)
            counts_pairs = counts.most_common()
            max_count = counts_pairs[0][1]
            mode = [k for k,v in counts_pairs if v == max_count]
            standard_name[c] = ';'.join(mode)
        else:
            standard_name[c] = names[0]
        
    df_standard_names = pd.DataFrame(list(standard_name.items()),columns=['cluster','StandardName'])
    df_final_cluster = df_clusters.merge(df_standard_names,on='cluster',how='left')
    
    return df_final_cluster

# chars_to_remove = [")", "(", ".", "|", "[", "]", "{", "}", "'"]
# rx = '[' + re.escape(''.join(chars_to_remove)) + ']'
# def remove_stop_words(in_s, stop_rx = rx, stop_w=None):
#     if stop_w:
#         stop_rx = '[' + re.escape(''.join(chars_to_remove)) + ']'
#     else:
#         if stop_rx is None:
#             raise('need stop words replace pattern')
#     res = re.sub(stop_rx, '', string)  # remove the list of chars defined above
#     return res 
#%%
if __name__ == '__main__':
    ## specity path 
    f_path = 'assignee_distribution.xlsx'
    out_path = 'company_names_map.xlsx'

    ## read and clean data 
    names_df = pd.read_excel(f_path)
    names_df.columns = ['names','count']
    ##apply rule based transformations 
    names_df['names_v1'] = names_df['names'].apply(deepclean_r,dic1=rep,dic2=rep2)
    names_df['names_v2'] = names_df['names_v1'].apply(deepclean,dic=rep3,deep=0)

    ###########
    ## quick check of rule fix quality 
    r_check = names_df[names_df['names_v2']=='']
    print(r_check)
    ###########

    ## too many companies to process, only get companies > 20 counts 
    com_names = names_df['names_v2'][names_df['count']>20].unique()
    com_names.sort()
    logger.info('totla length of filteed names: {}'.format(len(com_names)))
    sim_dict = cal_distance(com_names)
    sim_mx =  convert2mx(com_names,sim_dict)
    
    # ### run one tst 
    # testkey=('3M INNOVATIVE PROPERTIES', 'A123 SYSTEMS')
    # print(sim_dict[testkey])
    #%%
    ## get cluster 
    clusters = cluster.AffinityPropagation(affinity='precomputed',preference=50).fit_predict(sim_mx)
    n_c = len(np.unique(clusters))
    print('# of clusters generated : {}'.format(n_c))
    
    ## get the common part as suggested names 
    df_clusters = pd.DataFrame(list(zip(com_names,clusters)),columns=['CompanyName','cluster'])
    df_clusters.sort_values(by=['cluster'],inplace=True)
    df_clusters = get_common_name(df_clusters)
    
    #%%
    ## merge all data together and export 
    names_df_final = names_df.merge(df_clusters,left_on='names_v2',right_on='CompanyName',how='left')
    names_df_final.drop(columns=['names_v2'],inplace=True)
    names_df_final.sort_values(by=['cluster','count','names'],inplace=True)
    names_df_final.to_excel(out_path)
    print('............Finished.................')
    





    
    