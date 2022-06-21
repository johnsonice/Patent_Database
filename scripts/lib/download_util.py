# -*- coding: utf-8 -*-
"""
Created on Wed Oct 27 11:21:31 2021

@author: CHuang
"""
#from bs4 import BeautifulSoup
import requests
import os,sys
from tqdm import tqdm
from bs4 import BeautifulSoup
import logging
from collections.abc import Iterable
import time
import json
import zipfile
import pandas as pd
import pickle
logging.basicConfig(level=logging.INFO,format='%(levelname)s - %(message)s') #set up the config
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

## follow
# https://github.com/sirbowen78/lab/blob/master/file_handling/dl_file1.py

#def flatten(list_of_lists):
#    """
#    flatten nested lists
#    """
#    if len(list_of_lists) == 0:
#        return list_of_lists
#    if isinstance(list_of_lists[0], list):
#        return flatten(list_of_lists[0]) + flatten(list_of_lists[1:])
#    return list_of_lists[:1] + flatten(list_of_lists[1:])

def flatten(l):
    for el in l:
        if isinstance(el, Iterable) and not isinstance(el, (str, bytes)):
            yield from flatten(el)
        else:
            yield el

def pretify_xml(in_f,out_f=None):
    with open(in_f, "r",encoding='utf8') as f:
        xml_content= f.read()
        soup = BeautifulSoup(xml_content,'lxml')
    
    if out_f:
        with open(out_f, "w",encoding='utf8') as of:
            of.write(soup.prettify())
    
    return soup

def unzip_file(f_path,out_folder):
    f_name = os.path.basename(f_path).split('.')[0]
    of_p = os.path.join(out_folder,f_name)
    with zipfile.ZipFile(f_path, 'r') as zip_ref:
        zip_ref.extractall(of_p)

    
def save_as_json(content,out_file):
    with open(out_file, "w") as outfile:
        json.dump(content, outfile,indent=4)

def load_from_json(in_file):
    with open(in_file,'r') as json_file:
        content = json.load(json_file)
    return content

def save_as_pkl(content,out_file):
    with open(out_file, 'wb') as handle:
        pickle.dump(content, handle, protocol=pickle.HIGHEST_PROTOCOL)

def load_from_pkl(in_file):
    with open(in_file, 'rb') as handle:
        content = pickle.load(handle)
    return content

def export_to_excel(res_pds,out_file_path):
    if isinstance(res_pds,(list,tuple)):
        writer = pd.ExcelWriter(out_file_path, engine='xlsxwriter')
        for idx,sub_df in enumerate(res_pds):
            sub_df.to_excel(writer,sheet_name='Sheet{}'.format(idx+1),index=False)
        writer.save()
        print('saved to {}'.format(out_file_path))
    else:
        res_pds.to_excel(out_file_path,index=False)
    
    return None

def get_val_size(v,unit='kb'):
    """
    get avriable size 
    """
    res = int(sys.getsizeof(v)/1024)
    return res

def request_get_n_try(url,stream=False,n_try=5,verbose=False,sleep=5):
    for t in range(n_try):
        try:
            res = requests.get(url,stream=stream)
            file_size = res.headers.get('content-length')
            if file_size is None:
                file_size= 21
            else:
                file_size= int(file_size)
            
            if res.status_code == 200 and file_size>20:
                status = True
                return res,status
        except Exception as e:
            status = False
            if verbose:
                logger.warning("error in get page {}, retry attempt {}".format(url,t+1))
                #print(e)
                #raise
            time.sleep(sleep)
            
    return None,status


def download_from_link(url,out_folder,n_try=5,verbose=True,sleep=5):
    """
    Download file from url by chunk with process bar
    """
    filename = os.path.basename(url)
    abs_path=os.path.join(out_folder,filename)
    r,status = request_get_n_try(url,stream=True,n_try=n_try,verbose=verbose,sleep=sleep)
    chunk_size = 1024
    file_size = int(r.headers.get('content-length'))
 
    if r.status_code == 200 and file_size>20:
        with open(abs_path, "wb") as f, tqdm(
            unit="B",           # unit string to be displayed.
            unit_scale=True,    # let tqdm to determine the scale in kilo, mega..etc.
            unit_divisor=1024,  # is used when unit_scale is true
            total=file_size,    # the total iteration.
            file=sys.stdout,    # default goes to stderr, this is the display on console.
            desc=filename       # prefix to be displayed on progress bar.
        ) as progress:
            for chunk in r.iter_content(chunk_size=chunk_size):
                datasize=f.write(chunk)
                progress.update(datasize)
        success_status = True
    else:
        success_status = False
        
    return success_status

def creat_folder(f_path,verbose=False):
    if not os.path.exists(f_path):
        os.makedirs(f_path)
        status = True
    else:
        status = False
    
    if verbose:
        if status:
            print("{} creatd".foramt(f_path))
        else:
            print("Folder already existed, did not craete new")
    
    return status