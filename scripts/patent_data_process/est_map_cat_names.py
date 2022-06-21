# -*- coding: utf-8 -*-
"""
Created on Sun Jan 30 15:16:47 2022

@author: CHuang
"""

#### get EST map
import os,sys,re
sys.path.insert(0,'../lib/')
import pandas as pd
import logging
from download_util import load_from_pkl
from category_utils.get_uspc_map import Tree,node
from category_utils.uspc import USPC_Tree
import joblib
from joblib import Parallel, delayed
import argparse

logging.basicConfig(level=logging.INFO,format='%(levelname)s - %(message)s') #set up the config
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

def get_est_table(url,cach=None):
    
    if cach:
        df= pd.read_csv(cach)
    else:
        tables = pd.read_html(url)
        df = tables[3]
        new_header = df.iloc[0] #grab the first row for the header
        df = df[1:] #take the data less the header row
        df.columns = new_header #set the header row as the df header
    return df

def create_level(df):
    """
    convert topic to leveled labels 
    """
    df['L0'] = df['TOPIC'].map(lambda x: x if "." not in x else None)
    df['L1'] = df['TOPIC'].map(lambda x: x.replace('.','').strip() if x.count('.')==1 else None)
    df['L2'] = df['TOPIC'].map(lambda x: x.replace('.','').strip() if x.count('.')==2 else None)
    df['L3'] = df['TOPIC'].map(lambda x: x.replace('.','').strip() if x.count('.')==3 else None)
    cols = ['L0','L1','L2','L3']
    df.loc[:,cols] = df.loc[:,cols].ffill()
    df = df[~df['USPC'].isna()]
    df.fillna('',inplace=True)
    df['topic_des'] = df[['L0','L1','L2','L3']].agg('|'.join,axis=1)
    return df

class EST_MAPER(object):
    def __init__(self,
                 tree_pickle,
                 est_group=None):
        self.uspc_map = USPC_Tree(tree_pickle)
        self.match_f_ins = None
        self.est_group = est_group
        
    def process_pattern_inputs(self,p):
        res = []
        ps = p.split(';')
        for p in ps:
            if ',' in p:
                parent = p.split('/')[0].strip().zfill(3)
                subs = p.split('/')[1].split(',')
                aug_subs = []
                for sub in subs:
                    s_sub = sub.strip('+').replace('.','')
                    if '+' in sub:
                        sub_children = self.uspc_map.map[parent]['tree'].get_all_child_nodes(s_sub)
                        aug_subs.append(s_sub)
                        aug_subs.extend(sub_children)
                    else:
                        aug_subs.append(s_sub)
                aug_subs = set(aug_subs) ## remove dups 
                
                for sub in aug_subs:
                    res.append("{}/{}".format(parent,sub).replace(' ',''))
            else:
                if '/' in p:
                    parent = p.split('/')[0].strip().zfill(3)
                    sub = p.split('/')[1]
                    s_sub = sub.strip('+').replace('.','').strip()
                    if '+' in sub:
                        aug_subs = self.uspc_map.map[parent]['tree'].get_all_child_nodes(s_sub)
                        #aug_subs = list(set(aug_subs)) ## probably don't need dedup 
                        aug_subs = ['{}/{}'.format(parent,s) for s in aug_subs]
                        res.extend(aug_subs)
                    else:
                        res.append('{}/{}'.format(parent,s_sub))
                else:
                    res.append(p.strip().zfill(3))
                    
        
        return res 
        
    def update_match_p(self,input_str):
        #input_strs = input_strs.split(';')
        if '-' in input_str:
            parent,sub1,sub2 = re.split('/|-',input_str)
            return {'type':0,
                    'match_pattern':(parent,sub1,sub2)}
        elif '/' not in input_str:
            parent = input_str
            return {'type':2,
                    'match_pattern':parent}
        else:
            rgx_p = re.compile(input_str.replace('+','.*').replace('various','.*'))
            return {'type':1,
                    'match_pattern':rgx_p}
    
    def update_match_f_in(self,input_str,replace=True):
        '''
        create cach match pattern
        '''
        processed_in = self.process_pattern_inputs(input_str)
        if replace:
            self.match_f_ins =[self.update_match_p(i) for i in processed_in ]
        else:
            return [self.update_match_p(i) for i in processed_in ]
         
    @staticmethod
    def process_match_str(match_str):
        if '/' in match_str:
            parent,sub = re.split('/',match_str)
        else:
            parent = match_str
            sub = None
        return parent,sub
        
        
    def find_match(self,match_str,input_str=None,mfis=None):
        
        if input_str is not None:
            #logger.warn('use updated input pattern, updated cached match pattern')
            self.update_match_f_in(input_str)
        elif self.match_f_ins is None and input_str is None and mfis is None :
            raise('no match pattern fund,please pass in input pattern')
        else:
            pass
        
        ## check if there are cached info in it 
        if mfis is None:
            match_f_ins = self.match_f_ins
        else:
            match_f_ins = mfis
        
        for match_f_in in match_f_ins:
            if match_f_in['type'] == 0:
                match_p,match_sub = self.process_match_str(match_str)
                parent,sub1,sub2 = match_f_in['match_pattern']
                if match_p == parent and match_sub is not None:
                    if match_sub >= sub1 and match_sub <= sub2:
                        return True 
            
            elif match_f_in['type'] == 1:
                rgx = match_f_in['match_pattern']
                res = rgx.findall(match_str)
                if len(res)>0:
                    return True
                
            elif match_f_in['type'] ==2:
                parent = match_f_in['match_pattern']
                match_p,match_sub = self.process_match_str(match_str)
                if match_p == parent:
                    return True
        
        ## after all patterns checekd, still no match, return false 
        return False
    
    def cach_match_f_ins(self):
        ## cache math pattern in memory 
        self.match_f_ins_cache = [(i[0],i[1],self.update_match_f_in(i[0],replace=False)) for i in self.est_group]
        
    
    def find_all_match(self,match_str):
        '''
        match with a group of options and all est map codes 
        '''
        try:
            ## convert raw code from raw file to human readable 
            if not isinstance(match_str,str):
                return None 
            if match_str == '[]':
                return None
            match_options = self.uspc_map.process_raw_code(match_str)
            final_math_options = []
            for p,s in match_options:
                if s == '':
                    final_math_options.append(p)
                else:
                    final_math_options.append('{}/{}'.format(p,s))
                    
            ## see if we can find a math in est map 
            for ms in final_math_options:
                for in_str, topic_des,mfis in self.match_f_ins_cache:
                    if self.find_match(ms,None,mfis):
                        return topic_des
        except:
            logger.warning('error: {}'.format(match_str))
            
        ## if not return None
        return None


#def multi_process_func(in_code,est_class):
#    return est_class.find_all_match(in_code)

def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('-s', '--start_year', type=int,action='store', dest='start_year',
                        default=1976)#'adaboost')
    parser.add_argument('-e', '--end_year', type=int,action='store', dest='end_year',
                        default=2021) # NoSignal; Selected; Signal; SignalandSelected; NoSent
    args = parser.parse_args()    
    return args

#%%

if __name__ == '__main__':
    
    args = parse_args()
    
    ## specify data path 
    map_link = 'https://www.uspto.gov/web/patents/classification/international/est_concordance.htm'
    map_folder = r'C:\Users\chuang\OneDrive - International Monetary Fund (PRD)\Climate Change Challenge\USPTO\keywords\classification\uspc'  
    est_table_file = os.path.join(map_folder,'est_table.csv') 
    tree_pickle = os.path.join(map_folder,'uscp_tree.p') 
    data_path = 'F:/Data/USTPO'  ## i saved temp data in an external hd
    data_out_path = data_path
    
    ## multi process setup 
    number_of_cpu = 2 #joblib.cpu_count() - 2 
    parallel_pool = Parallel(n_jobs=number_of_cpu,verbose=5)
    ####
    
    ## load object map 
    if os.path.exists(est_table_file):
        df = get_est_table(map_link,est_table_file)
    else:
        df = get_est_table(map_link)
        df.to_csv(est_table_file,index=False)
        
    df = create_level(df)
    ## covert df to dictionary 
    est_group = list(df[['USPC','topic_des']].to_records(index=False))

    est = EST_MAPER(tree_pickle,est_group)
    est.cach_match_f_ins()
    #%%
    for year in range(args.start_year,args.end_year):
        #print('working on {}'.format(year))
        df_p = os.path.join(data_path,'with_cat','{}_cat.csv'.format(year))
        out_p = os.path.join(data_out_path,'with_est','{}_est.csv'.format(year))
        
        chunks = pd.read_csv(df_p,chunksize = 40000, error_bad_lines=False) 
        #df = chunks.get_chunk()
        #fil_res = []    
        for idx,df in enumerate(chunks): 
            print('processing year {}; chunk {}'.format(year,idx))
            ## if first time, overwirte
            if idx==0:
                header_status = True
                mode = 'w'
            else: ## otherwise append to file
                header_status = False
                mode = 'a'
            
            ##single process
            results = df['bio_main-classification_national'].map(est.find_all_match).tolist()
            
            ## multi process
    #        delayed_funcs = [delayed(multi_process_func)(x,est) for x in df['bio_main-classification_national']]
    #        results = parallel_pool(delayed_funcs)
            
            ## remember to set the index to be the same
            final_df = df.join(pd.DataFrame(results,columns=['est_description']).set_index(df.index))
            final_df.to_csv(out_p,encoding='utf-8',index=False,header=header_status,mode=mode)  
            #fil_dfs = filter_final_results(final_df)
            #fil_res.append(copy.deepcopy(fil_dfs))

    
    
    
    
    #%%
    
    
    
    
#    #%%
#    #%%
#    in_p = '424; 514/1.1; 422/1.1-43'
#    match_in = '044/589'
#    #print(est.find_match(match_in,in_p))   
#    #print(est.find_all_match(match_in))
#    #%%
#    match_in = 'US 044 589 R'
#    print(est.uspc_map.process_raw_code(match_in))
#    print(est.find_all_match(match_in))
    
    #%%

    