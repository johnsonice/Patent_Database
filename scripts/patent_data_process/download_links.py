# -*- coding: utf-8 -*-
"""
Created on Wed Oct 27 09:20:34 2021

@author: CHuang
"""

from bs4 import BeautifulSoup
import requests
import os,sys,re
sys.path.insert(0,'../lib/')
from download_util import request_get_n_try,save_as_json,load_from_json
import logging
logging.basicConfig(level=logging.INFO,format='%(levelname)s - %(message)s') #set up the config
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

#%%
def get_all_zip_links(url="https://bulkdata.uspto.gov/#rsrch",
                      verbose=True):
    
    page=requests.get(url)
    soup = BeautifulSoup(page.text, 'html.parser')
    all_links = soup.findAll('a')
    all_links = [a for a in all_links if 'Patent Grant Full Text' in a.text]
    #all_links = all_links[:2]
    link_dict = {}
    for a in all_links:
        sub_url = a.get('href')
        sub_title = a.text
        year = re.findall(r"\s+([0-9]{4})\)", sub_title)[0]
        ## populate metadata info        
        c_dict = {}
        c_dict['title'] = sub_title
        c_dict['year'] = year
        c_dict['parent_url'] = sub_url
        ## populate links to zip files 
        c_dict = get_child_link_zips(c_dict,verbose=verbose)
        link_dict[c_dict['title']] = c_dict
    
    return link_dict

def get_child_link_zips(c_dict,verbose=True):
    ## get links to zip files 
    page,sub_url_status = request_get_n_try(c_dict['parent_url'],
                                           stream=False,
                                           n_try=5,
                                           verbose=True,
                                           sleep=5)
    
    if verbose:
        logger.info("{} retrieve {}".format(sub_url_status,c_dict['title']))
    
    if sub_url_status:
        soup = BeautifulSoup(page.text, 'html.parser')
        sub_links = soup.findAll('a')
        sub_links = [s for s in sub_links if '.zip' in s.text]
        sub_links = [build_url(year=c_dict['year'],name=s.text) for s in sub_links]
        
        c_dict['file_url'] = sub_links
        c_dict['status'] = True
    else:
        logger.warning("failed to retrieve {}".format(c_dict['parent_url']))
        c_dict['status'] = False
    
    return c_dict

def update_all_link_dict(all_links,verbose=True):
    update_status = True
    for k,v in all_links.items():
        if not v['status'] :
            logger.info('working on {}'.format(k))
            ## update v with ziped info
            v = get_child_link_zips(v,verbose=verbose)
            if not v['status']:
                update_status=False
    
    if update_status:
        logger.info('all links updated')
    else:
        logger.info('still liks to be updated , try again')
    
    return all_links,update_status

def build_url(year,
              name,
              prefix="https://bulkdata.uspto.gov/data/patent/grant/redbook/fulltext",):
    url = "{}/{}/{}".format(prefix,year,name)
    
    return url 

#%%
if __name__ == '__main__':
    
    ###########
    url = "https://bulkdata.uspto.gov/#rsrch"
    out_folder = 'C:/Users/chuang/OneDrive - International Monetary Fund (PRD)/Climate Change Challenge/USPTO/raw_data/USPTO'
    ###########
    
    out_links_file = os.path.join(out_folder,'links.json')

    if os.path.exists(out_links_file):
        all_links=load_from_json(out_links_file)
    else:
        all_links = get_all_zip_links(url,verbose=True)
    
    ## udpate loaded links file
    for i in range(5):
        all_links,update_status = update_all_link_dict(all_links,True)
        if update_status: break 
    if not update_status:
        save_as_json(all_links,all_links)
        raise Exception('Link file is not completed. Try update link file again')
    else:
        save_as_json(all_links,all_links)    

        
            

            

        
#    for k,v in all_links.items():
#        fold_path = os.path.join(out_folder,v['year'])
#        creat_folder()
    
