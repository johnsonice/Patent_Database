# -*- coding: utf-8 -*-
"""
Created on Thu Jan 13 23:36:50 2022

@author: CHuang
"""

### map category code with cat names 
import os,sys,re
try:
     script_path = os.path.dirname(os.path.abspath(__file__))
except:
    script_path = '.'

sys.path.insert(0,script_path)
sys.path.insert(0,os.path.join(script_path,'..'))
sys.path.insert(0,os.path.join(script_path,'../../lib/'))

import pandas as pd
from download_util import load_from_pkl
from get_uspc_map import Tree,node
#%%

class USPC_Tree(object):
    def __init__(self,pkl_path=None):
        if pkl_path is not None:
            self.map = load_from_pkl(pkl_path)

    @staticmethod
    def process_raw_code(rc):
        '''
        reformat raw id to be consistant with map file 
        '''
        rc=rc.strip()[3:]
        parts = [c for c in rc.split(' ') if c != '']
        
        ## case 1 cat number start with letter 
        if not rc[0].isnumeric():
            cat_id = rc[:3].replace(' ','0')
            node_id = rc[3:]
            #node_id='.'.join(node_id[i:i+3] for i in range(0, len(node_id), 3))
            node_id=node_id.lstrip()
            return [(cat_id,node_id)]
        
        if len(parts)==2:
            cat_id = parts[0].zfill(3)
            node_id = parts[1]
            return [(cat_id,node_id)]
        
        if len(parts)==1:
            res = []
            for i in range(1,4):
                cat_id = rc[:i].zfill(3)
                node_id = rc[i:]
            
                res.append((cat_id,node_id))
            
            return res
        
        if len(parts)>2:
            ## try to capture code like 333 21 R  ;  358  11-  118
            res = []
            cat_id = parts[0].zfill(3)
            node_id = parts[1].strip('-')
            res.append((cat_id,node_id))
            #node_id = parts[1:3].strip()
            node_id = ''.join(parts[1:3])
            res.append((cat_id,node_id))  
            return res
                
        print('Error : {}'.format(rc))
        return None
    
    def get_label(self,raw_code,level=None):
        status = {'status':'fail',
                  'info':raw_code}
        try:
            #cat_id,node_id = self.process_raw_code(raw_code)
            res = self.process_raw_code(raw_code)
            for cat_id,node_id in res:
                try:
                    names = self.map[cat_id]['tree'].get_family_history(node_id)['names']
                    names = '||'.join([n[1] for n in names])
                    status = {'status':'success'}
                    return names,status
                except:
                    continue
            
            ## if error get very high level lable only 
            for cat_id,node_id in res:
                try:
                    names = self.map[cat_id]['name']
                    return names,status
                except:
                    continue 
            ## if all options are wrong return error 
            return None, status
        except:
            return None,status
        #%%
if __name__ == '__main__':
    
    data_path = 'F:/USPTO'  ## i saved temp data in an external hd
    map_folder = os.path.join('..','..','..',r'keywords\classification\uspc')
    tree_pickle = os.path.join(map_folder,'uscp_tree.p') 
    
    data_path = 'F:/USPTO'  ## i saved temp data in an external hd
    #year = 2010
    for year in range(2005,2021):
        print('year {}'.format(year))
        df_p = os.path.join(data_path,'processed_data','{}.csv'.format(2005))
        chunks = pd.read_csv(df_p,chunksize = 20000, error_bad_lines=False) # us on_bad_lines='skip'
        for df in chunks:
            x = df[(df['bio_main-classification_national'].isna()& df['bio_main-classification_ipc'].notna())]
            print(len(x))
    #%%
    
    
    
    
    
    
    
    
#    #%%
#    df = chunks.get_chunk()
#    uspc_map = USPC_Tree(tree_pickle)
#    for rc in df['bio_main-classification_national'].head(5000):
#        res = uspc_map.get_label(rc)
#        print(res)
#        if res[1]['status'] == 'fail':
#            print(res[1])
#            
            

    #%%
#    data_path = 'F:/USPTO'  ## i saved temp data in an external hd
#    df_p = os.path.join(data_path,'processed_data','{}.csv'.format(2012))
#    df = pd.read_csv(df_p, error_bad_lines=False) # us on_bad_lines='skip'
#    
#    #%%
#    for idx,df in enumerate(chunks):
#        pass
#    for rc in df['bio_main-classification_national'].head(5000):
#        res = uspc_map.get_label(rc)
#        print(res)
#        if res[1]['status'] == 'fail':
#            print(res[1])
    
    #%%
#    test = 'US 48197R'
#    pc = uspc_map.process_raw_code(test)
#    print(pc)
#    #%%
#    names = self.map[cat_id]['tree'].get_family_history(node_id)['names']
#    #%%
#    test = 'US 48197R'
#    #uspc_map.process_raw_code(test)
#    doc_id = get_docid_by_Pid(df,test)
#    print(doc_id)
#        cat_id,node_id = process_raw_code(rc)
#        names = uspc_map[cat_id]['tree'].get_family_history(node_id)['names']
#        names = '||'.join([n[1] for n in names])
#        print(cat_id,node_id,names)