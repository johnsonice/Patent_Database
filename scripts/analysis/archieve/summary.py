# -*- coding: utf-8 -*-
"""
Created on Mon Dec 13 11:18:16 2021

@author: CHuang
"""
import os , sys, re
sys.path.insert(0,'../lib/')
import pandas as pd
import glob
import copy
from download_util import export_to_excel

##

def get_1_split(t):
    if isinstance(t,str):
        if "||" in t:
            res = t.split('||')[0]
            return res    
    return ''
    
def extract_cat_l1(df_obj):
    """
    get level 1 category 
    """
    df_obj['cat_l1'] = df_obj['Cat_name'].map(get_1_split)
    return df_obj    

class AGG(object):
    def __init__(self,ex_fs:list):
        self.ex_fs = ex_fs
        self.agg_dfs()
        #self.agg_df = self.sumarize()

    def agg_dfs(self):
        res_dfs = []
        meta_dfs = []
        for ex_f in self.ex_fs:
            df,year = self.keep_simple(ex_f)
            df_meta = self.keep_total(ex_f,year)
            res_dfs.append(copy.copy(df))
            meta_dfs.append(copy.copy(df_meta))
        
        self.res_df = pd.concat(res_dfs,ignore_index=True,axis=0)
        self.meta_df = pd.concat(meta_dfs,ignore_index=True,axis=0)
        return None
    
    def keep_simple(self,ex_f):
        df = pd.read_excel(ex_f,sheet_name='Sheet1')
        df = df[['bio_applicant_country','bio_date','bio_doc-number','keys_hit','total_counts','Cat_name']]
        year = int(str(df['bio_date'].iloc[0])[:4])
        df['year'] = year
        return df, year
    
    def keep_total(self,ex_f,year):
        df = pd.read_excel(ex_f,sheet_name='Sheet2')
        df = df[['total_pt']]
        df['year']= year
        return df

    def sumarize(self,transform=None):
        if transform:
            self.res_df = transform(self.res_df)
        agg_df = self.res_df.groupby(['year','bio_applicant_country','cat_l1']).agg({'total_counts':['count','sum']})
        agg_df.columns = agg_df.columns.map('_'.join)
        agg_df.reset_index(inplace=True)
        return agg_df
    
class AGG_raw(AGG):
    def __init__(self,ex_fs:list):
        super().__init__(ex_fs)
        
    def agg_dfs(self):
        return None
    
    
#%%
if __name__ == "__main__":
    ## global variables 
    #root_folder = "C:/Users/chuang/OneDrive - International Monetary Fund (PRD)/Climate Change Challenge/USPTO"
    root_folder = r'F:/USPTO'
    res_folder = os.path.join(root_folder,'results')
    df_files = glob.glob(os.path.join(res_folder,'*_filtered_res.xlsx'))
    df_files = [d for d in df_files if "~" not in d]
    RES = AGG(df_files)
    RES.agg_df = RES.sumarize(extract_cat_l1)
    #%%
    ## reshape and export 
    df_raw = RES.agg_df
    
    df_count = RES.agg_df.pivot_table(index = ['bio_applicant_country'],columns='year',values='total_counts_count')
    df_sum = RES.agg_df.pivot_table(index = ['bio_applicant_country'],columns='year',values='total_counts_sum')
    
    df_count_cat = RES.agg_df.pivot_table(index = ['cat_l1'],columns='year',values='total_counts_count')
    df_sum_cat = RES.agg_df.pivot_table(index = ['cat_l1'],columns='year',values='total_counts_sum')
    
    export_to_excel([df_raw,df_count,df_sum,RES.meta_df,df_count_cat,df_sum_cat],os.path.join(res_folder,'agg_res.xlsx'))
    