# -*- coding: utf-8 -*-
"""
Created on Wed Oct 27 23:49:49 2021

@author: CHuang
"""

import os,sys
sys.path.insert(0,'../lib/')
import pandas as pd
from download_util import creat_folder,unzip_file
import logging
logging.basicConfig(level=logging.INFO,format='%(levelname)s - %(message)s') #set up the config
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
#%%
def generate_status_file(raw_data_folder,unzip_folder):
    """
    generate a status file with all files in our folder 
    """
    status_file = []
    sub_folder = [f for f in os.listdir(raw_data_folder) if os.path.isdir(os.path.join(raw_data_folder,f))]
    for sf in sub_folder:
        new_folder = os.path.join(unzip_folder,sf)
        for z_f in os.listdir(os.path.join(raw_data_folder,sf)):
            if '.zip' in z_f:
                zip_file = os.path.join(raw_data_folder,sf,z_f)
                status_file.append((zip_file,new_folder,False))
    
    df=pd.DataFrame(status_file,columns=['zip_file','out_folder','unzip_status'])
    
    return df
                
if __name__ == '__main__':
    
    ## path variables 
    #raw_data_folder = 'C:/Users/chuang/International Monetary Fund (PRD)/Zhang, Yuchen - Climate Change Challenge/USPTO/raw_data/USPTO'
    raw_data_folder = 'C:/Users/chuang/OneDrive - International Monetary Fund (PRD)/Climate Change Challenge/USPTO/raw_data/USPTO'
    #unzip_folder = "C:/Users/chuang/International Monetary Fund (PRD)/Zhang, Yuchen - Climate Change Challenge/USPTO/raw_data/USPTO_unzipped"    
    unzip_folder = 'C:/Users/chuang/OneDrive - International Monetary Fund (PRD)/Climate Change Challenge/USPTO/raw_data/USPTO_unzipped'
    status_file = os.path.join(unzip_folder,'unzip_status.csv')
    
    
    if os.path.exists(status_file):
        logger.info('read form existing status file')
        df = pd.read_csv(status_file)
    else:
        logger.info('generate new status file status file')
        df = generate_status_file(raw_data_folder,unzip_folder)
    
    ## process unzip 
    chunk_size = 100 
    counter = 0 
    for index, row in df.iterrows():
        if not row['unzip_status']:
            logger.info("unzipping {} ... ".format(os.path.basename(row['zip_file'])))
            try:
                unzip_file(row['zip_file'],row['out_folder'])
                unzip_status = True
                counter +=1 
            except:
                unzip_status = False
                logger.info("unzip failed {}".format(row['zip_file']))
                counter +=1 
                
            df.at[index,'unzip_status'] = unzip_status
        
        if counter!= 0 and counter%chunk_size == 0:
            df.to_csv(status_file,index=False)
            logger.info('---- udpate staus file on disk ---- ')
    #%%
    ## once done, export to csv
    df.to_csv(status_file,index=False)
    
    
    