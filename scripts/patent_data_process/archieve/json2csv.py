# -*- coding: utf-8 -*-
"""
Created on Mon Dec  6 15:32:53 2021

@author: CHuang
"""

import os,sys,re
sys.path.insert(0,'../lib/')
from download_util import load_from_json
from xml2json import Patent_Parser
import pandas as pd
import joblib
from joblib import Parallel, delayed

#%%

def dict_flatten(in_dict, dict_out=None, parent_key=None, separator="_"):
   if dict_out is None:
      dict_out = {}

   for k, v in in_dict.items():
      k = f"{parent_key}{separator}{k}" if parent_key else k
      if isinstance(v, dict):
         dict_flatten(in_dict=v, dict_out=dict_out, parent_key=k)
         continue

      dict_out[k] = v

   return dict_out

#%%


if __name__ == "__main__":
    #debug = False
    #raw_xml_folder = "C:/Users/chuang/OneDrive - International Monetary Fund (PRD)/Climate Change Challenge/USPTO/raw_data/USPTO_unzipped"
    processed_folder = "C:/Users/chuang/OneDrive - International Monetary Fund (PRD)/Climate Change Challenge/USPTO/processed_data"
    number_of_cpu = joblib.cpu_count()
    #%%
    ## small test conversion 
#    var_s = Patent_Parser(xml_content=None).json
#    vs = dict_flatten(var_s)

    #%%
    for i in range(2019,2022):
        print('working on {}'.format(i))
        test_path = os.path.join(processed_folder,'{}.json'.format(i))
        out_path = os.path.join(processed_folder,'{}.csv'.format(i))
        content = load_from_json(test_path)
        #%%
        ## multi process task 
        parallel_pool = Parallel(n_jobs=number_of_cpu,verbose=5)
        delayed_funcs = [delayed(dict_flatten)(x) for x in content]
        content = parallel_pool(delayed_funcs)
        delayed_funcs = None  ## clear memory 
        parallel_pool = None  ## clear memory 
        #%%
        content = pd.DataFrame(content)
        content.to_csv(out_path,index=False) 
        content = None

     