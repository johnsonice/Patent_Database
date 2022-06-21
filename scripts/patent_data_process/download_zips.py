# -*- coding: utf-8 -*-
"""
Created on Wed Oct 27 22:47:27 2021

@author: CHuang
"""

import requests
import pandas as pd
import os,sys,re
import time
import json
sys.path.insert(0,'../lib/')
from download_util import download_from_link,request_get_n_try,load_from_json,creat_folder,save_as_json,unzip_file
import logging
logging.basicConfig(level=logging.INFO,format='%(levelname)s - %(message)s') #set up the config
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

#%%

def multi_try_download(url,n_try=20,sleep=10):
    for i in range(n_try):
        try:
            success_status = download_from_link(url,sub_data_folder,n_try=5,verbose=True,sleep=5)
            if success_status:
                return success_status
        except:
            success_status = False
            logger.warning("failed; retry attempt {};{}".format(i+1,url))
            time.sleep(sleep)
    
    return success_status

def check_unsuccess_cases(out_download_status_file):
    res_list = []
    all_links = load_from_json(out_download_status_file)
    for idx,(k,v) in enumerate(all_links.items()):
        for url,status in v['down_status'].items():
            if not status:
                res_list.append(url)
    return res_list
    
#%%
if __name__ == '__main__':
    
    #########
    #out_folder = 'C:/Users/chuang/International Monetary Fund (PRD)/Zhang, Yuchen - Climate Change Challenge/USPTO/raw_data/USPTO'
    out_folder = 'C:/Users/chuang/OneDrive - International Monetary Fund (PRD)/Climate Change Challenge/USPTO/raw_data/USPTO'
    #########
    
    out_links_file = os.path.join(out_folder,'links.json')
    out_download_status_file = os.path.join(out_folder,'links_download_status.json')
    
    if os.path.exists(out_download_status_file):
        ## if there was a downlaod status file, load from previous 
        all_links = load_from_json(out_download_status_file)
    else:
        ## of not initiate from begaining 
        all_links = load_from_json(out_links_file)
    
    #batch_size = 10
    for idx,(k,v) in enumerate(all_links.items()):
        if v.get('down_status') is None: 
            ## if not download before, no status dict, create one
            v['down_status']={}
        
        ## if folder did not exist, create one
        sub_data_folder=os.path.join(out_folder,v['year'])
        creat_folder(sub_data_folder)
        
        if len(v['file_url'])>0:
            for idx,url in enumerate(v['file_url']):
                if not v['down_status'].get(url):
                    ## if downlaod status is None or False, initate download
                    d_status = multi_try_download(url,n_try=10)
                    v['down_status'][url] = d_status
                else:
                    ## if download status is True, then do nothing
                    #logger.info('{} already downloaded'.format(os.path.basename(url)))
                    pass
        else:
            logger.warning(v)
    
    ## dump download status file 
    save_as_json(all_links,out_download_status_file)

    ## check if till soem not downloaded
    res = check_unsuccess_cases(out_download_status_file)
    
    if len(res)== 0:
        print('all update to date')
    else:
        print("{} files failed, try update again".format(len(res)))
    