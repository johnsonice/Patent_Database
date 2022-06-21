# -*- coding: utf-8 -*-
"""
Created on Thu Mar 31 15:49:44 2022


@author: chuang
"""
## transform txt raw data to csv 

import os,sys,re
sys.path.insert(0,'../lib/')
from download_util import flatten,save_as_json
import glob
import logging
import pandas as pd
import joblib
from joblib import Parallel, delayed
#from functools import wraps
from xml2csv import exception_handler,extract_multi_p_results 
sys.setrecursionlimit(2000)
print(sys.getrecursionlimit())


logging.basicConfig(level=logging.INFO,format='%(levelname)s - %(message)s') #set up the config
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

def read_txt(f_path):
    with open(f_path,encoding='utf8',errors='ignore') as f:
        lines = f.read()
    
    return lines 

def split_txt(c,delimiter='PATN'):
    """
    split long string to buckets by indivisual pattern 
    """
    ## split but keep delimiter in buckets 
    cs = [delimiter+e for e in c.split(delimiter) if e]
    cs = cs[1:]
    cs = [e.split('\n') for e in cs]
    cs = [[i.rstrip().replace('"','\"').replace("'","\'") for i in l if i !=''] for l in cs]
    return cs 

def check_mod_name(name:str,check_list:list):
    ''' check if name already exist and modify '''
    if name not in check_list:
        return name
    else:
        if '_' in name:
            name = name+'i'
        else:
            name = name+'_i'
        ## recursive check and add 
        return check_mod_name(name,check_list)

def sublist2dict(chunk:list):
    """
    turn each chunk of information into dict for easy reference latter
    """
    key=chunk[0]   ## get first idem as key
    b_d = {key:{}} ## define an empyt dict 
    for i in chunk[1:]:
        k,v = i[:3],i[3:].strip()
        ### for description, claims etc, we don't car sub categories; make then to paragraph
        if key in ['GOVT','PARN','BSUM','DRWD','DETD','DCLM','CLMS','ABST']:
            k = 'PAL'            
        ### merge different types to paragraphs
        if k in ['   ','PAR','PAC','TBL','PA1','PA0','PA2']:  ## mayve don't need this check anymore 
            k = 'PAL'
        if k[:2] == 'PA':
            k = 'PAL'
        ### turn pals into a list 
        if k == 'PAL':
            if k in b_d[key].keys():
                b_d[key][k].append(v)
            else:
                b_d[key][k] = [v]
        else:
            k = check_mod_name(k,b_d[key].keys())
            b_d[key][k] = v
    
    return b_d

def buckets2dict(bucket:list,included_tags=None):
    """
    convert one bucket of a patent info to dict for easy retrival 
    """
    bb_dict = {}
    if isinstance(included_tags,list):
        bucket = [i for i in bucket if i[0].rstrip() in included_tags]
    sub_dicts = [sublist2dict(i) for i in bucket] 
    if len(sub_dicts)>200:
        raise Exception('something is wrong with dict conversion for {}'.format(bucket))
    for sd in sub_dicts:
        for k,v in sd.items():
            k = check_mod_name(k,bb_dict.keys())
            bb_dict[k] = v
    return bb_dict

def list2buckets(str_list:list):
    """
    input: a string list contain all info for one pattern; it should be 1 item from split_txt results
    transformed to bucketed lists for dic transformation
    """
    keys = [k for k in str_list if ' ' not in k]
    buckets = []
    subbucket = []
    for l in str_list:
        if l in keys:
            if len(subbucket) == 0:
                subbucket.append(l)
            else:
                buckets.append(subbucket)
                subbucket = [l]
        else:
            subbucket.append(l)
    
    return buckets

#def get_all_info_from_dict(dict_info):
#    for k,v in dict_info.items():
        
    

class Patent_Parser(object):
    def __init__(self,txt_list=None,included_tags=[],verbose=False):
        self.verbose = verbose 
        self.ini_json_structure()
        ## only keep those tages, it is specific to txt patent parser 
        self.included_tags = ['PATN','ASSG','INVT','CLAS','ABST','PRIR','GOVT','PARN',
                              'BSUM','DRWD','DETD','DCLM','CLMS']
        if txt_list is None:
            logger.info('initiate empty obj')
        else:
            self.pat_info = buckets2dict(list2buckets(txt_list),included_tags=self.included_tags)
        
    def ini_json_structure(self):
        self.json = {'bio':
                            {'doc-number':None,
                            'country':None,
                            'date':None,
                            'application_date':None,
                            'applicant':{'first_name':None,
                                         'last_name':None,
                                         'organization_name':None,
                                         'country':None,
                                         'state':None,
                                         'city':None},
                            'inventor':{'first_name':None,
                                         'last_name':None,
                                         'organization_name':None,
                                         'country':None,
                                         'state':None,
                                         'city':None},
                            'main-classification':{'ipc':None,
                                                   'cpc':None,
                                                   'national':None,
                                                   'locarno':None
                                                   },
                            'assignee':{'name':None,
                                        'city':None,
                                        'state':None,
                                        'country':None,
                                        'zip':None
                                    }
                            },
                     'abstract':{'paras':None},
                     'description':{'paras':None},
                     'claims':{'claims':None}
                     }
    
    def get_all_info(self,flat=True):
        """
        run all info extaction and return log status 
        """
        bio_status = self.get_bio()
        doc_number = self.json['bio']['doc-number']
        applicant_status = self.get_applicat_info()
        abstract_status = self.get_abstract()
        des_status = self.get_description()
        claim_status = self.get_claim()
        
        self.get_inventor_info()
        self.get_assignee_info()
        self.get_cls()
        
        self.consolidate()
        
        if flat:
            self.json = self.dict_flatten(self.json)
            
        return doc_number,bio_status,abstract_status,des_status,claim_status,applicant_status
    
    def get_bio(self):
        '''
        get bio information 
        '''
        try:
            self.json['bio']['doc-number'] = self.pat_info['PATN'].get('WKU')
            self.json['bio']['date'] = self.pat_info['PATN'].get('ISD')
            self.json['bio']['application_date'] = self.pat_info['PATN'].get('APD')
            country = self.pat_info['ASSG'].get('CNT')
            if country is None:
                country = self.pat_info['INVT'].get('CNT')
            self.json['bio']['country'] = country
            status = True
        except Exception as e:
            if self.verbose:
                logger.warning('{}: bio info not found'.format(self.json['bio']['doc-number']))
            status = False        
        return status
    
    @exception_handler(error_msg = 'inventor info not found')
    def get_applicat_info(self):
        """ no application info in this version, use inventor info """
        invt = self.pat_info['INVT'] ## if error then, no invitor info available 
        self.json['bio']['applicant']['country'] = invt.get('CNT')
        self.json['bio']['applicant']['state'] = invt.get('STA')
        self.json['bio']['applicant']['city'] = invt.get('CTY')
        self.json['bio']['applicant']['zip'] = invt.get('ZIP')
        self.json['bio']['applicant']['first_name'] = invt.get('NAM').split(';')[1].strip()
        self.json['bio']['applicant']['last_name'] = invt.get('NAM').split(';')[0].strip()
        
        return True
    
    @exception_handler(error_msg = 'inventor info not found')
    def get_inventor_info(self):
        invt = self.pat_info['INVT'] ## if error then, no invitor info available 
        self.json['bio']['inventor']['country'] = invt.get('CNT')
        self.json['bio']['inventor']['state'] = invt.get('STA')
        self.json['bio']['inventor']['city'] = invt.get('CTY')
        self.json['bio']['inventor']['zip'] = invt.get('ZIP')
        self.json['bio']['inventor']['first_name'] = invt.get('NAM').split(';')[1].strip()
        self.json['bio']['inventor']['last_name'] = invt.get('NAM').split(';')[0].strip()

        return True
    
    @exception_handler(error_msg = 'assignee info not found')
    def get_assignee_info(self):
        assg = self.pat_info['ASSG'] ## if error then, no invitor info available 
        self.json['bio']['assignee']['name'] = assg.get('NAM').strip()
        self.json['bio']['assignee']['country'] = assg.get('CNT')
        self.json['bio']['assignee']['state'] = assg.get('STA')
        self.json['bio']['assignee']['city'] = assg.get('CTY')
        self.json['bio']['assignee']['zip'] = assg.get('ZIP')
        return True
    
    @exception_handler(error_msg = 'calssification info not found')
    def get_cls(self):
        cls_info = self.pat_info['CLAS'] ## if not found , will warn 
        self.json['bio']['main-classification']['national'] = 'US '+ cls_info.get('OCL').strip()
        self.json['bio']['main-classification']['ipc'] = cls_info.get('ICL')
        return True
    
    #@exception_handler(error_msg = 'abstract info not found')
    def get_abstract(self):
        self.json['abstract']['paras'] = self.agg_dict_values(self.pat_info.get('ABST'))
        return True
    
    #@exception_handler(error_msg = 'des info not found')
    def get_description(self):
        res = []
        res.extend(self.agg_dict_values(self.pat_info.get('GOVT')))
        res.extend(self.agg_dict_values(self.pat_info.get('PARN')))
        res.extend(self.agg_dict_values(self.pat_info.get('BSUM')))
        res.extend(self.agg_dict_values(self.pat_info.get('DRWD')))
        res.extend(self.agg_dict_values(self.pat_info.get('DETD')))
        self.json['description']['paras'] = res
        
        return False
    
    @exception_handler(error_msg = 'claim info not found')
    def get_claim(self):
        res = []
        res.extend(self.agg_dict_values(self.pat_info.get('DCLM')))
        res.extend(self.agg_dict_values(self.pat_info.get('CLMS')))
        self.json['claims']['claims'] = res
        return True
    
    def consolidate(self):
        ## udpate country info
        if self.pat_info.get('PRIR') is not None:           ## see if there are foreign country priority info if so, update 
            country = self.pat_info.get('PRIR').get('CNT')
            self.json['bio']['applicant']['country'] = country
            self.json['bio']['assignee']['country'] = country
        else:
            self.json['bio']['country'] = 'US'
            if self.json['bio']['applicant']['country'] is None:
                self.json['bio']['applicant']['country'] = 'US'
            if self.json['bio']['assignee']['country'] is None:
                self.json['bio']['assignee']['country']='US'
            
        ## update applicant info -- to be the same as inventor
        # self.json['bio']['applicant'] = self.json['bio']['inventor']      ## in old version, we don't have application info, make it the same as inventor info
        
        return True
    
    def dict_flatten(self, in_dict, dict_out=None, parent_key=None, separator="_"):
       if dict_out is None:
          dict_out = {}
    
       for k, v in in_dict.items():
          k = f"{parent_key}{separator}{k}" if parent_key else k
          if isinstance(v, dict):
             self.dict_flatten(in_dict=v, dict_out=dict_out, parent_key=k)
             continue
    
          dict_out[k] = v
    
       return dict_out
   
    @staticmethod
    def agg_dict_values(d:dict):
        if isinstance(d,dict):
            res = list(d.values())
            res = list(flatten(res))
            return res
        elif isinstance(d,str):
            return [d]
        elif isinstance(d,list):
            return d
        elif d is None:
            return []
        else:
            raise Exception('not a dict, please check')
            return []
    

def multi_process_func(txt_content,verbose=False):
    """
    delayed process function for joblib 
    """
    x_f = Patent_Parser(txt_content,verbose=verbose)
    log_x = x_f.get_all_info()
    c_x = x_f.json
    
    return log_x,c_x
        
#%%
if __name__ == "__main__":
    #raw_txt_folder = "C:/Users/chuang/OneDrive - International Monetary Fund (PRD)/Climate Change Challenge/USPTO/raw_data/USPTO_unzipped"
    #processed_folder = "C:/Users/chuang/OneDrive - International Monetary Fund (PRD)/Climate Change Challenge/USPTO/processed_data"
    raw_txt_folder="F:/Data/USTPO/raw_txt/in"
    ######## temp processed folder 
    processed_folder = "F:/Data/USTPO/raw_txt/out"
    ###############################
    error_log_path = os.path.join(processed_folder,'error_log.json')
    #raw_xml_path = "C:/Users/chuang/OneDrive - International Monetary Fund (PRD)/Climate Change Challenge/USPTO/raw_data/USPTO_unzipped/2021/ipg211026/ipg211026.xml"
    debug=False
    
    
    #%%
    
    ## use multi process 
    number_of_cpu = 4 # joblib.cpu_count() - 2 
    parallel_pool = Parallel(n_jobs=number_of_cpu,verbose=5)
    
    ## single process for debuging     
    for year in range(1976,2002):
        #year = 2000
        log_file = os.path.join(processed_folder,'extract_log_{}.csv'.format(year))
        if year == 2001:
            csv_file = os.path.join(processed_folder,'{}_txt.csv'.format(year))
        else:
            csv_file = os.path.join(processed_folder,'{}.csv'.format(year))
        fs = glob.glob(raw_txt_folder + '/{}/**/*.txt'.format(year),recursive = True)
        log_history = []
        content_history = []
        error_file =[]
        ## go through each file in one year
        
        counter = 0 
        for f in fs:
            logger.info('processing file : {}'.format(f))
            try:
                c = read_txt(f)
                delimiter = 'PATN'
                cs = split_txt(c,delimiter)    ##parse txt file into seperate xml chunks
            except:
                error_file.append(f)
                continue
            
            ##########################
            ## for dubugging purpose##
            ##########################
            if debug:
                log_x,c_x = multi_process_func(cs[0],verbose=False)
                print(c_x)
                print(cs[0]) 
                raise Exception('stop here')
            #########################
            #########################
                
            counter+=1
            ## if first time, overwirte
            if counter==1:
                header_status = True
                mode = 'w'
            else: ## otherwise append to file
                header_status = False
                mode = 'a'
            
            delayed_funcs = [delayed(multi_process_func)(x) for x in cs]
            f_result = parallel_pool(delayed_funcs)
            ## aggregate results 
            #log_history,content_history,log_df = extract_multi_p_results(f,f_result,log_history,content_history)
            log_history,content_chunk,log_df = extract_multi_p_results(f,f_result,log_history,
                                                                           content_history=None,content_accumulate=False)
            
            ## export results 
            log_df.to_csv(log_file,index=False)
            content_df = pd.DataFrame(content_chunk)
            content_df.to_csv(csv_file,encoding='utf-8',index=False,header=header_status,mode=mode) 
            #save_as_json(content_history,json_file)
            logger.info('counter: {}; header: {}; mode: {}; length: {}'.format(counter,header_status,mode,len(content_chunk)))
            logger.info('one example: {}'.format(content_chunk[-1]['bio_doc-number']))
        
    ## export all error files 
    save_as_json(error_file,error_log_path)
            
        
        
#%%
#     ##single process for debuging     
#    #for year in range(1980,2000):
#    year = 1984
#    log_file = os.path.join(processed_folder,'extract_log_{}.csv'.format(year))
#    csv_file = os.path.join(processed_folder,'{}.csv'.format(year))
#    fs = glob.glob(raw_txt_folder + '/{}/**/*.txt'.format(year),recursive = True)
#    log_history = []
#    content_history = []
#    error_file =[]
#    ## go through each file in one year
#    
#    overall_counter = 0 
#    for f in fs:
#    #######################
#    ## for debuging purpose 
#    #f = 'F:/Data/USTPO/raw_txt/in/2001\pftaps20010109_wk02\pftaps20010109_wk02.txt'
#    #f = 'F:/Data/USTPO/raw_txt/in/1984/pftaps19840103_wk01/pftaps19840103_wk01.txt'
#    #f = 'F:/Data/USTPO/raw_txt/in/1984\pftaps19841127_wk48\pftaps19841127_wk48.txt'
#    ##########################
#        logger.info('processing file : {}'.format(f))
#        try:
#            c = read_txt(f)
#            delimiter = 'PATN'
#            cs = split_txt(c,delimiter)    ##parse txt file into seperate xml chunks
#        except:
#            error_file.append(f)
#            #continue
#        counter=0
#        for css in cs:
#            overall_counter+=1
#            counter+=1
#            log_x,c_x = multi_process_func(css,verbose=False)
#            log_history.append((f,*log_x))
#            content_history.append(c_x)
#            if counter%1000 == 0:
#                logger.info('.....working on {}/{} files from {}.....'.format(counter,len(cs), os.path.basename(f)))
#                c_x['description_paras'] = ''
#                logger.info('sample json:{}'.format(c_x))
#            
#    #log_df = pd.DataFrame(log_history,columns=['file','doc_number','abstract','bio','des','claim','applicant_status'])
#    #log_df.to_csv(log_file,index=False)
#    #content_df = pd.DataFrame(content_history)
#    #content_df.to_csv(csv_file,encoding='utf-8',index=False,header=True,mode='w') 

        
        
        
