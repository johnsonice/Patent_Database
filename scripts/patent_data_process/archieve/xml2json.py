# -*- coding: utf-8 -*-
"""
Created on Sat Oct 30 10:00:31 2021

@author: CHuang
"""

from bs4 import BeautifulSoup
import os,sys,re
sys.path.insert(0,'../lib/')
from download_util import save_as_json,load_from_json
import glob
import pandas as pd
import logging
from functools import wraps
import joblib
from joblib import Parallel, delayed

logging.basicConfig(level=logging.INFO,format='%(levelname)s - %(message)s') #set up the config
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

def writeout_xml(xmls_str_i,out_file):
    '''
    write out one small file for debug
    '''
    with open(out_file,'w') as f:
        f.write(xmls_str_i)

def read_split_raw_xml(xml_path,split_term = "<?xml"):
    with open(xml_path,'r') as f:
        c= f.read()
    xmls = c.split(split_term)    
    xmls = [split_term + l for l in xmls if len(l.strip())>0]
    return xmls 

def exception_handler(error_msg='error handleing triggered'):
    '''
    follow: https://stackoverflow.com/questions/30904486/python-wrapper-function-taking-arguments-inside-decorator
    '''
    def outter_func(func):
        @wraps(func)
        def inner_function(*args, **kwargs):
            try:
                res = func(*args, **kwargs)
            except:
                try:
                    if args[0].verbose:
                        logger.warning('{}:{}'.format(args[0].json['bio']['doc-number'],error_msg))
                except:
                    logger.warning('{}'.format(error_msg))
                res = False
            return res 
        return inner_function
    
    return outter_func

@exception_handler(error_msg='test')
def test_error(inp):
    res = inp[0]
    return res 

class Patent_Parser(object):
    def __init__(self,xml_content,verbose=False):
        self.verbose = verbose 
        self.ini_json_structure()
        if xml_content is None:
            logger.info('initiate empty obj')
        else:
            soup = BeautifulSoup(xml_content,'lxml')
            self.root = soup.body.p.find('us-patent-grant')
        

    def ini_json_structure(self):
        self.json = {'bio':
                            {'doc-number':None,
                            'country':None,
                            'date':None,
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
                            'main-classification':None},
                    
                     'abstract':{'paras':None},
                     'description':{'paras':None},
                     'claims':{'claims':None}
                     }
    
    @staticmethod
    def get_tag_text(tag):
        if tag is None:
            return tag
        else:
            return tag.text
        
    def get_all_info(self):
        """
        run all info extaction and return log status 
        """
        bio_status = self.get_bio()
        abstract_status = self.get_abstract()
        des_status = self.get_description()
        claim_status = self.get_claim()
        applicant_status = self.get_applicat_info()
        inventor_status = self.get_inventor_info(self)
        
        return self.json['bio']['doc-number'],bio_status,abstract_status,des_status,claim_status,applicant_status
    
    def get_bio(self):
        '''
        get bio information 
        '''
        try:
            publication_ref = self.root.find('publication-reference')
            self.json['bio']['doc-number'] = publication_ref.find('doc-number').text
            self.json['bio']['country'] = publication_ref.find('country').text
            self.json['bio']['date'] = publication_ref.find('date').text
            status = True
        except Exception as e:
            if self.verbose:
                logger.warning('{}: bio info not found'.format(self.json['bio']['doc-number']))
            status = False
            
#        ## get classification number 
#        try:
#            classification = self.root.find('classification-locarno')
#            self.json['bio']['main-classification'] =  classification.find('main-classification').text
#            status = True
#        except Exception as e:
#            logger.warning('{}: classification info not found'.format(self.json['bio']['doc-number']))
#            #print(e)
#            status = False
            
        return status
    @exception_handler(error_msg = 'applicant info not found')
    def get_applicat_info(self):
        try:
            applicant = self.root.find('us-applicants').findAll('us-applicant')[0]  
        except:
            applicant = self.root.find('applicants').findAll('applicant')[0]  
        first_name = self.get_tag_text(applicant.find('first-name'))
        last_name = self.get_tag_text(applicant.find('last-name'))
        organization_name = self.get_tag_text(applicant.find('orgname'))
        country = self.get_tag_text(applicant.find('country'))
        city = self.get_tag_text(applicant.find('city'))
        state = self.get_tag_text(applicant.find('state'))
        
        self.json['bio']['applicant']={'first_name':first_name,
                                 'last_name':last_name,
                                 'organization_name':organization_name,
                                'country':country,
                                 'state':state,
                                 'city':city}
        return True
    
    @exception_handler(error_msg = 'inventor info not found')
    def get_inventor_info(self):
        applicant = self.root.find('inventors').findAll('inventor')[0]  
        first_name = self.get_tag_text(applicant.find('first-name'))
        last_name = self.get_tag_text(applicant.find('last-name'))
        organization_name = self.get_tag_text(applicant.find('orgname'))
        country = self.get_tag_text(applicant.find('country'))
        city = self.get_tag_text(applicant.find('city'))
        state = self.get_tag_text(applicant.find('state'))
        
        self.json['bio']['inventor']={'first_name':first_name,
                                     'last_name':last_name,
                                     'organization_name':organization_name,
                                    'country':country,
                                     'state':state,
                                     'city':city}
        return True
    
    @exception_handler(error_msg = 'abstract info not found')
    def get_abstract(self):
        abstract = self.root.find('abstract')  
        cs = abstract.findAll('p')
        ps = [p.text.strip() for p in cs]
        self.json['abstract']['paras']=ps
        return True
    
    @exception_handler(error_msg = 'des info not found')
    def get_description(self):
        description = self.root.find('description')  
        cs = description.findAll('p')
        ps = [p.text.strip() for p in cs]
        self.json['description']['paras']=ps
        return True

    @exception_handler(error_msg = 'claim info not found')
    def get_claim(self):
        claims = self.root.find('claims')
        cs = claims.findAll('claim')
        ps = [p.text.strip() for p in cs]
        self.json['claims']['claims']=ps
        #raise('test')
        return True
    
def multi_process_func(xml_content):
    """
    delayed process function for joblib 
    """
    x_f = Patent_Parser(xml_content,verbose=False)
    log_x = x_f.get_all_info()
    c_x = x_f.json
    
    return log_x,c_x

def extract_multi_p_results(file_path,f_result,log_history,content_history):
    
    f_log = [(file_path,)+f[0] for f in f_result]
    content_log = [f[1] for f in f_result]
    log_history.extend(f_log)
    content_history.extend(content_log)
    log_df = pd.DataFrame(log_history,columns=['file','doc_number',
                                               'abstract','bio','des',
                                               'claim','applicant_status'])
    return log_history,content_history,log_df
    
#%%

if __name__ == '__main__':
    debug = False
    raw_xml_folder = "C:/Users/chuang/OneDrive - International Monetary Fund (PRD)/Climate Change Challenge/USPTO/raw_data/USPTO_unzipped"
    processed_folder = "C:/Users/chuang/OneDrive - International Monetary Fund (PRD)/Climate Change Challenge/USPTO/processed_data"
    error_log_path = os.path.join(processed_folder,'error_log.json')
    #raw_xml_path = "C:/Users/chuang/OneDrive - International Monetary Fund (PRD)/Climate Change Challenge/USPTO/raw_data/USPTO_unzipped/2021/ipg211026/ipg211026.xml"
    #%%
    number_of_cpu = joblib.cpu_count()
    parallel_pool = Parallel(n_jobs=number_of_cpu,verbose=5)
    
    ## before 2005, data structure is a bit different, need to fix the problem latter 
    ## for now, just use data starting from 2005
    for year in range(2015,2021):
        log_file = os.path.join(processed_folder,'extract_log_{}.csv'.format(year))
        json_file = os.path.join(processed_folder,'{}.json'.format(year))
        fs = glob.glob(raw_xml_folder + '/{}/**/*.xml'.format(year),recursive = True)
        log_history = []
        content_history = []
        error_file =[]
        ## go through each file in one year
        for f in fs:
            logger.info('processing file : {}'.format(f))
            try:
                xmls_str = read_split_raw_xml(f)    ##parse txt file into seperate xml chunks
            except:
                error_file.append(f)
                continue
            
            if debug:
                log_x,c_x= multi_process_func(xmls_str[0])
                print(c_x)
                print(xmls_str[0]) 
                raise('stop here')
            else:
                delayed_funcs = [delayed(multi_process_func)(x) for x in xmls_str]
                f_result = parallel_pool(delayed_funcs)
                ## aggregate results 
                log_history,content_history,log_df = extract_multi_p_results(f,f_result,
                                                                             log_history,
                                                                             content_history)
                ## export results 
                log_df.to_csv(log_file,index=False)
                save_as_json(content_history,json_file)
                save_as_json(error_file,error_log_path)
            
                logger.info('one example: {}'.format(content_history[-1]['bio']))
        
    
        
#    log_info = []
#    content = []
#    counter = 0 
#    for f in fs:
#        xmls_str = read_split_raw_xml(f)    
#        for idx,x in enumerate(xmls_str):
#            counter += 1
#            x_f = Patent_Parser(x,verbose=False)
#            log_x = x_f.get_all_info()
#            log_info.append((f,*log_x))
#            content.append(x_f.json)
#        
#            if counter%1000 == 0:
#                logger.info('.....working on {}/{} files from {}.....'.format(counter,len(xmls_str), os.path.basename(f)))
#                logger.info('sample json:{}'.format(x_f.json['bio']))
#                df = pd.DataFrame(log_info,columns=['file','doc_number','abstract','bio','des','claim','applicant_status'])
#                df.to_csv(log_file,index=False)
#                save_as_json(content,json_file)
#                
#    logger.info('Finished folder {}; export log file'.format(f))
#    df = pd.DataFrame(log_info,columns=['file','doc_number','bio','des','claim'])
#    df.to_csv(log_file,index=False)
#    save_as_json(content,json_file)

