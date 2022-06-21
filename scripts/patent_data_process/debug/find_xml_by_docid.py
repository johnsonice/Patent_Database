# -*- coding: utf-8 -*-
"""
Created on Wed Jan 12 15:05:02 2022

@author: CHuang
"""

### find one raw xml using doc-number 


from xml2csv import read_split_raw_xml
import glob,os
import pandas as pd
from bs4 import BeautifulSoup

#%%
def pretify_xml(soup,out_f=None):
    with open(out_f, "w",encoding='utf8') as of:
        of.write(soup.prettify())    
    return soup

#%%
raw_xml_folder = "C:/Users/chuang/OneDrive - International Monetary Fund (PRD)/Climate Change Challenge/USPTO/raw_data/USPTO_unzipped"
processed_folder = "F:/USPTO/processed_data"
year = 2015
logfile = os.path.join(processed_folder,'extract_log_{}.csv'.format(year))
logmap = pd.read_csv(logfile)


test_code ='08925447'

out_path = test_code+'.xml'
f = logmap[logmap['doc_number'] == test_code].iloc[0,0]
error_file = []
xmls_str = read_split_raw_xml(f)    ##parse txt file into seperate xml chunks
for x in xmls_str:
    if test_code in x:
        out_xml = x
        soup = BeautifulSoup(out_xml,'lxml')
        pretify_xml(soup,out_path)
        break
