# -*- coding: utf-8 -*-
"""
Created on Wed Nov  3 22:31:15 2021

@author: chuang
"""
## follow 
## https://coderzcolumn.com/tutorials/python/joblib-parallel-processing-in-python
## https://joblib.readthedocs.io/en/latest/parallel.html
import joblib
from joblib import Parallel, delayed
import time 

def slow_power(x, p):
    time.sleep(1)
    return x ** p

if __name__ == "__main__":
    
    number_of_cpu = joblib.cpu_count()
    delayed_funcs = [delayed(slow_power)(i, 5) for i in range(10)]
    parallel_pool = Parallel(n_jobs=number_of_cpu,verbose=5)
    
    #%%
    %time  parallel_pool(delayed_funcs)
#    %time  [slow_power(i,5) for i in range(10)]