# -*- coding: utf-8 -*-
"""
Created on Thu Jan 13 21:48:48 2022

@author: CHuang
"""

from bs4 import BeautifulSoup
import copy
import pandas as pd
import os,sys,re
sys.path.insert(0,'../lib/')
from download_util import request_get_n_try,save_as_json,load_from_json,save_as_pkl,load_from_pkl
import logging
logging.basicConfig(level=logging.INFO,format='%(levelname)s - %(message)s') #set up the config
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
 
#%%

class IPC_Tree(object):
    def __init__(self,xml_path):
        with open(xml_path,'r',encoding='utf8') as f:
            self.root = BeautifulSoup(f.read(),'lxml')
    
    @staticmethod
    def process_raw_code(raw_code):
        """
        process code to be consistant with mapp file 
        """
        codes = raw_code.split(';')
        code= codes[0].split(' [')[0]
        cat_id,subid = code.split()
        subid1,subid2 = subid.split('/')
        subid1= subid1.zfill(4)
        subid2=subid2.ljust(6,'0')
        
        final_code = '{}{}{}'.format(cat_id,subid1,subid2)
        
        return final_code
        
        
    def get_label(self,raw_code,level=None,error_return=None):
        code = self.process_raw_code(raw_code)
        info = self.get_family_history(code)
        if info is not None:
            res = info['names']
            res ='||'.join(res)
            return res
        else:
            return error_return
    
    @staticmethod
    def get_title_text(tag,error_return=None):
        try:
            parts = tag.find('textbody').findAll('titlepart')
            title = ";".join([p.find('text').text.strip() for p in parts])
        except:
            title = error_return
            
        return title 
        
    def get_family_history(self,node_id,error_return=None):
        node = self.root.find('ipcentry',{'symbol':node_id})
        if node is not None:
            res = {'codes':[node_id],
                   'names':[self.get_title_text(node)]}
            
            for p in node.parents:
                code = p.get('symbol')
                if code is not None:
                    name = self.get_title_text(p)
                    res['codes'].insert(0,code)
                    res['names'].insert(0,name)
        else:
            res = error_return
            
        return res 
        
    def get_node(self,node_id):
        node = self.root.find('ipcentry',{'symbol':node_id})
        return node

#%%
if __name__ == '__main__':
    map_folder = r'C:\Users\chuang\OneDrive - International Monetary Fund (PRD)\Climate Change Challenge\USPTO\keywords\classification\ipc\ipc_scheme_20220101'
    raw_info_xml = os.path.join(map_folder,'clean_EN_ipc_scheme_20220101.xml')    

    tree = IPC_Tree(raw_info_xml)

    #%%
    tree.get_label('H04L 9/20')
    
##%%
#    for i in df['bio_main-classification_ipc']:
#        if not pd.isna(i):
#            print(tree.get_label(i))
#    
    
    

