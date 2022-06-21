# -*- coding: utf-8 -*-
"""
Created on Mon Feb 14 14:50:11 2022

@author: CHuang
"""

import os , sys#, re
sys.path.insert(0,'../lib/')
import pandas as pd
pd.set_option('display.max_columns', None)
from download_util import export_to_excel

#%%
def transform_docid(doc_id):
    try:
        new_id = doc_id.lstrip('0').replace('.0','')
        return new_id
    except:
        print('error:{}'.format(doc_id))
        return 'nan'
    

#%%
if __name__ == '__main__':
    
    export_map = True
    export_distribution = True
    root_path = 'F:/Data/USTPO'
    country_map_path = os.path.join(r'C:\Users\chuang\OneDrive - International Monetary Fund (PRD)\Climate Change Challenge\USPTO\keywords',
                                    'country_map.xlsx')
    
    assingee_path = os.path.join(root_path,'assingee','assignee.csv')
    doc_id_path = os.path.join(root_path,'assingee','documentid.csv')
    agg_df_path = os.path.join(root_path,'results_small','agg_small.xlsx')
    
    out_assigee_map = os.path.join(root_path,'assingee','docid_assignee_map.csv')
    out_path = os.path.join(root_path,'results_small','agg_small_assignee.xlsx')
    out_path_ass_uni = os.path.join(root_path,'results_small','assignee_distribution_v2.xlsx')
    
    #%%
    ## read all files 
    agg_df = pd.read_excel(agg_df_path)

    if export_map:
        country_map = pd.read_excel(country_map_path,sheet_name='ee_country_names')
        docid_df = pd.read_csv(doc_id_path)
        as_df = pd.read_csv(assingee_path)
        ## merge assignee info with doc meta info
        map_df = as_df.merge(docid_df,how='left',on='rf_id',indicator=True)
        print('map merging results')
        print(map_df['_merge'].describe())
        map_df.drop(columns='_merge',inplace=True)   ## drop merging after checking 
        map_df['grant_doc_num'] = map_df['grant_doc_num'].astype(str)
        map_df = map_df[map_df['grant_doc_num']!='nan']
        ## deduplicate by grant number, only pick the first one
        map_df = map_df.drop_duplicates(subset=['grant_doc_num'],keep='first')
        ## merge proper country name
        map_df = map_df.merge(country_map,
                                     how='left',
                                     left_on='ee_country',
                                     right_on='raw_country_name',
                                     indicator=False) ## no indicator
        map_df.to_csv(out_assigee_map,encoding='utf8')
    else:
        map_df = pd.read_csv(out_assigee_map)
    #%%
    ## [re-process map ids 
    agg_df['doc_id_map'] = agg_df['bio_doc-number'].astype(str)
    agg_df['doc_id_map'] = agg_df['doc_id_map'].map(transform_docid)
    ## maybe want to check dup, why it happens
    #agg_df['dup_tag'] = agg_df['doc_id_map'].duplicated(keep=False)
    #######################
    ## merge assignee info 
    #######################
    merged_df = agg_df.merge(map_df,how='left',
                             left_on='doc_id_map',right_on='grant_doc_num',
                             indicator=True)
    print('assignee merging results')
    print(merged_df['_merge'].value_counts())
    
    drop_cols = ['pgpub_doc_num','pgpub_doc_num','pgpub_doc_num','grant_date',
                 'title','lang','appno_doc_num','grant_country','appno_country','appno_date']
    merged_df.drop(columns=drop_cols,inplace=True)
    
    #%%
    #########################################
    # merge correct assignee country info ##
    #########################################
    ## if mapped assignes has countryname, change original country to new assignee country
    ee_country_flag = (
                    merged_df['ee_country'].map(
                    lambda x:len(x) if isinstance(x,str) else 0
                    ) >1) & (
                    map_df['ee_country']!='NOT PROVIDED'
                    ) & (
                        map_df['Country_Name_Fix'].notna()
                        ) & (map_df['Country_Name_Fix']!='nan') & (merged_df['bio_assignee_name'].isna())

    #x = merged_df[['Country_Name','bio_assignee_name','ee_country','2_Code','3_Code','Country_Name_Fix','2_Code_Fix','3_Code_Fix']][ee_country_flag].head(100)
    
    #%%
    merged_df.loc[ee_country_flag,'Country_Name'] = merged_df['Country_Name_Fix']
    merged_df.loc[ee_country_flag,'2_Code'] = merged_df['2_Code_Fix']
    merged_df.loc[ee_country_flag,'3_Code'] = merged_df['3_Code_Fix']
    ## merge mapped assignee info to 
    merged_df['bio_assignee_name'].fillna(merged_df['ee_name'],inplace=True)
    merged_df.drop(columns=['raw_country_name', 'changed name', '2_Code_Fix', '3_Code_Fix','Country_Name_Fix', '_merge'],inplace=True)
    
    ## export mapped results to drive 
    export_to_excel(merged_df,out_path)
    
    #%%
    if export_distribution:
        ## export assignee distribution 
        ass_name_unique = merged_df['bio_assignee_name'].value_counts()
        ass_name_unique.sort_values(ascending=False,inplace=True)
        ass_name_unique.to_excel(out_path_ass_uni)
#    
    #%%
#    ####################################
#    # export all possible country names 
#    ####################################
#    map_df = pd.read_csv(out_assigee_map)
#    cc = pd.DataFrame(map_df['ee_country'].unique())
#    cc.to_excel(r'C:\Users\chuang\OneDrive - International Monetary Fund (PRD)\Climate Change Challenge\USPTO\keywords\ee_country.xlsx')
#    
    
    
    
    
    
    #%%
    ##########################
    #https://towardsdatascience.com/fuzzy-matching-people-names-6e738d6b8fe
    #https://www.analyticsinsight.net/company-names-standardization-using-a-fuzzy-nlp-approach/
    #https://towardsdatascience.com/python-tutorial-fuzzy-name-matching-algorithms-7a6f43322cc5
    #https://medium.com/@isma3il/supplier-names-normalization-part1-66c91bb29fc3
    ##########################
    
#    #%%
#    doc_nums = agg_df['bio_doc-number'].to_list()
#    grant_ids = map_id_df['grant_doc_num'].to_list()
#    rf_ids = map_id_df['rf_id'].to_list() 
#    app_doc_nums = map_id_df['appno_doc_num'].to_list() 
#    #%%
#    for i in range(-100,0):
#        print(doc_nums[i],doc_nums[i].lstrip('0').replace('.0',''),doc_nums[i].lstrip('0').replace('.0','') in grant_ids)
#    #%%
#    for i in range(10000,100):
#        print(map_ids[0] in doc_nums)
#        
#    #%%
#        
#    test = '33450368' 
#    test in rf_ids
        