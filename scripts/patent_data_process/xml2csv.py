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
import argparse

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
    xmls = [i.rstrip().replace('"','\"').replace("'","\'") for i in xmls]
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
                    if args[0].verbose: ## in an class args[0] is self 
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
            if verbose:
                logger.info('initiate empty obj')
        else:
            soup = BeautifulSoup(xml_content,'lxml')
            self.root = soup.body.p.find('us-patent-grant')
        

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
                            'assignee':{'name':None,
                                        'city':None,
                                        'state':None,
                                        'country':None,
                                        },
                            'main-classification':{'ipc':None,
                                                   'cpc':None,
                                                   'national':None,
                                                   'locarno':None
                                                   },
                            },
                     'abstract':{'paras':None},
                     'description':{'paras':None},
                     'claims':{'claims':None}
                     }
    
    @staticmethod
    def get_tag_text(tag,strip=True):
        if tag is None:
            return tag
        else:
            if strip:    
                return tag.text.strip()
            else:
                return tag.text
        
    def get_all_info(self,flatten=True):
        """
        run all info extaction and return log status 
        """
        bio_status = self.get_bio()
        doc_number = self.json['bio']['doc-number']
        abstract_status = self.get_abstract()
        des_status = self.get_description()
        claim_status = self.get_claim()
        applicant_status = self.get_applicat_info()
        self.get_inventor_info()
        self.get_assignee_info()
        self.get_ipc_cls()
        self.get_cpc_cls()
        self.get_national_cls()
        self.get_locarno_cls()
        
        if flatten:
            self.json = self.dict_flatten(self.json)
            
        return doc_number,bio_status,abstract_status,des_status,claim_status,applicant_status
    
    def get_bio(self):
        '''
        get bio information 
        '''
        try:
            ## get final granted info
            publication_ref = self.root.find('publication-reference')
            self.json['bio']['doc-number'] = publication_ref.find('doc-number').text
            self.json['bio']['country'] = publication_ref.find('country').text
            self.json['bio']['date'] = publication_ref.find('date').text
            ## get application info
            application_ref = self.root.find('application-reference')
            self.json['bio']['application_date'] = application_ref.find('date').text
            status = True
        except Exception as e:
            if self.verbose:
                logger.warning('{}: bio info not found'.format(self.json['bio']['doc-number']))
            status = False        
        return status
    
    @exception_handler(error_msg = 'ipc classification info not found')
    def get_ipc_cls(self):
        ## get IPC codes
        ipcs_code = []
        try:
            ipcs_root = self.root.find('classifications-ipcr')  ## find first ipc root # findall will find some in citations
            ipcs =  ipcs_root.findAll('classification-ipcr')
            for ipc in ipcs:
                ipc_date = self.get_tag_text(ipc.find('ipc-version-indicator'))
                ipc_section = self.get_tag_text(ipc.find('section'))
                ipc_class = self.get_tag_text(ipc.find('class'))
                ipc_subclass = self.get_tag_text(ipc.find('subclass'))
                ipc_main_group = self.get_tag_text(ipc.find('main-group'))
                ipc_subgroup = self.get_tag_text(ipc.find('subgroup'))
                ipc_symbol_position =  self.get_tag_text(ipc.find('symbol-position'))
                ipc_cls_value =  self.get_tag_text(ipc.find('classification-value'))
                ipc_action_date =  self.get_tag_text(ipc.find('action-date'))
                ipc_generating_office =  self.get_tag_text(ipc.find('generating-office'))
                ipc_cls_status =  self.get_tag_text(ipc.find('classification-status'))
                ipc_cls_data_source =  self.get_tag_text(ipc.find('classification-data-source'))
                ipc_scheme_origination_code =  self.get_tag_text(ipc.find('scheme-origination-code'))
                
                ipcs_code.append('{}{}{} {}/{} [{}_{}_{}_{}_{}_{}_{}_{}]'.format(ipc_section,ipc_class,ipc_subclass,        ## class info
                                                                             ipc_main_group,ipc_subgroup,                   ## group info
                                                                             ipc_date,ipc_symbol_position,ipc_cls_value,    ## all other meta info
                                                                             ipc_action_date,ipc_generating_office,
                                                                             ipc_cls_status,ipc_cls_data_source,
                                                                             ipc_scheme_origination_code))
            ipcs_code = ";".join(ipcs_code)
            status = True
        except:
            ipcs_code =""
            status = False
            
        self.json['bio']['main-classification']['ipc']=ipcs_code
        
        return status
    
    @exception_handler(error_msg = 'cpc classification info not found')
    def get_cpc_cls(self):
        ## get CPC codes
        cpcs_code = []
        try:
            cpcs_root = self.root.find('classifications-ipcr')  ## find first cpcs root # findall will find some in citations 
            cpcs =  cpcs_root.findAll('classification-cpc')
            for cpc in cpcs:
                cpc_date = self.get_tag_text(cpc.find('cpc-version-indicator'))
                cpc_section = self.get_tag_text(cpc.find('section'))
                cpc_class = self.get_tag_text(cpc.find('class'))
                cpc_subclass = self.get_tag_text(cpc.find('subclass'))
                cpc_main_group = self.get_tag_text(cpc.find('main-group'))
                cpc_subgroup = self.get_tag_text(cpc.find('subgroup'))
                cpc_symbol_position =  self.get_tag_text(cpc.find('symbol-position'))
                cpc_cls_value =  self.get_tag_text(cpc.find('classification-value'))
                cpc_action_date =  self.get_tag_text(cpc.find('action-date'))
                cpc_generating_office =  self.get_tag_text(cpc.find('generating-office'))
                cpc_cls_status =  self.get_tag_text(cpc.find('classification-status'))
                cpc_cls_data_source =  self.get_tag_text(cpc.find('classification-data-source'))
                cpc_scheme_origination_code =  self.get_tag_text(cpc.find('scheme-origination-code'))
                
                cpcs_code.append('{}{}{} {}/{} [{}_{}_{}_{}_{}_{}_{}_{}]'.format(cpc_section,cpc_class,cpc_subclass,        ## class info
                                                                             cpc_main_group,cpc_subgroup,                   ## group info
                                                                             cpc_date,cpc_symbol_position,cpc_cls_value,    ## all other meta info
                                                                             cpc_action_date,cpc_generating_office,
                                                                             cpc_cls_status,cpc_cls_data_source,
                                                                             cpc_scheme_origination_code))
            cpcs_code = ";".join(cpcs_code)
            status=True
        except:
            cpcs_code =""
            status=False
            
        self.json['bio']['main-classification']['cpc']=cpcs_code
        
        return status
    
    @exception_handler(error_msg = 'national classification info not found')
    def get_national_cls(self):
        ## get national codes
        nationals_code = []
        try:
            nationals = self.root.findAll('classification-national')[:1]  ## we will just take the first one; find all get some from citations
            for national in nationals:
                national_cls_country = self.get_tag_text(national.find('country'))
                national_main_cls = self.get_tag_text(national.find('main-classification'))
                nationals_code.append("{} {}".format(national_cls_country,national_main_cls))
            nationals_code = ";".join(nationals_code)
            status=True
        except:
            nationals_code=''
            status=False
                        
        self.json['bio']['main-classification']['national']=nationals_code
        return status
    
    @exception_handler(error_msg = 'locarno classification info not found')
    def get_locarno_cls(self):
        ## get national codes
        locarnos_code = []
        try:
            locarnos = self.root.findAll('classification-locarno')[:1]  ## we will just take the first one; find all get some from citations 
            for locarno in locarnos:
                locarno_edition = self.get_tag_text(locarno.find('edition'))
                locarno_main_cls = self.get_tag_text(locarno.find('main-classification'))
                locarnos_code.append("{} [{}]".format(locarno_main_cls,locarno_edition))
            locarnos_code = ";".join(locarnos_code)
            status=True
        except:
            locarnos_code=''
            status=False
                        
        self.json['bio']['main-classification']['locarno']=locarnos_code
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
    @exception_handler(error_msg = 'assignee info not found')
    def get_assignee_info(self):
        assignee = self.root.find('assignees').findAll('assignee')[0]  
        name = self.get_tag_text(assignee.find('addressbook').find('orgname'))
        city = self.get_tag_text(assignee.find('addressbook').find('address').find('city'))
        state = self.get_tag_text(assignee.find('addressbook').find('address').find('state'))
        country = self.get_tag_text(assignee.find('addressbook').find('address').find('country'))
        
        self.json['bio']['assignee']={'name':name,
                                     'country':country,
                                     'state':state,
                                     'city':city}
        return True
    
    @exception_handler(error_msg = 'inventor info not found')
    def get_inventor_info(self):
        if self.root.find('inventors') is not None:
            applicant = self.root.find('inventors').findAll('inventor')[0] 
        else:
            applicant = self.root.find('applicants').findAll('applicant')[0] 
            
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
    
def multi_process_func(xml_content):
    """
    delayed process function for joblib 
    """
    x_f = Patent_Parser(xml_content,verbose=False)
    log_x = x_f.get_all_info()
    c_x = x_f.json
    
    return log_x,c_x

def extract_multi_p_results(file_path,f_result,log_history,content_history=None,content_accumulate=False):
    
    f_log = [(file_path,)+f[0] for f in f_result]
    content_log = [f[1] for f in f_result]
    log_history.extend(f_log)
    if content_accumulate:
        content_history.extend(content_log)
    else:
        content_history = content_log
        
    log_df = pd.DataFrame(log_history,columns=['file','doc_number',
                                               'abstract','bio','des',
                                               'claim','applicant_status'])
    return log_history,content_history,log_df

def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('-s', '--start_year', type=int,action='store', dest='start_year',
                        default=2005)#'adaboost')
    parser.add_argument('-e', '--end_year', type=int,action='store', dest='end_year',
                        default=2021) # NoSignal; Selected; Signal; SignalandSelected; NoSent
    args = parser.parse_args()    
    return args

#%%

if __name__ == '__main__':
    debug = False
    args = parse_args()
    
    raw_xml_folder = "F:/Data/USTPO/raw_txt/in"
    #raw_xml_folder = "C:/Users/chuang/OneDrive - International Monetary Fund (PRD)/Climate Change Challenge/USPTO/raw_data/USPTO_unzipped"
    #processed_folder = "C:/Users/chuang/OneDrive - International Monetary Fund (PRD)/Climate Change Challenge/USPTO/processed_data"
    ######## temp processed folder 
    processed_folder = "F:/Data/USTPO/raw_txt/out"
    ###############################
    error_log_path = os.path.join(processed_folder,'error_log.json')
    #raw_xml_path = "C:/Users/chuang/OneDrive - International Monetary Fund (PRD)/Climate Change Challenge/USPTO/raw_data/USPTO_unzipped/2021/ipg211026/ipg211026.xml"
    
    #%%
#   #%%
    number_of_cpu = joblib.cpu_count() - 4 
    parallel_pool = Parallel(n_jobs=number_of_cpu,verbose=5)
    
    ## before 2005, data structure is a bit different, need to fix the problem latter 
    ## for now, just use data starting from 2005
    for year in range(args.start_year,args.end_year):
    #for year in [2021]:
        log_file = os.path.join(processed_folder,'extract_log_{}.csv'.format(year))
        #json_file = os.path.join(processed_folder,'{}.json'.format(year))
        csv_file = os.path.join(processed_folder,'{}.csv'.format(year))
        fs = glob.glob(raw_xml_folder + '/{}/**/*.xml'.format(year),recursive = True)
        log_history = []
        content_history = []
        error_file =[]
        ## go through each file in one year
        counter = 0 
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
                counter += 1
                            ## if first time, overwirte
                if counter==1:
                    header_status = True
                    mode = 'w'
                else: ## otherwise append to file
                    header_status = False
                    mode = 'a'
                    
                delayed_funcs = [delayed(multi_process_func)(x) for x in xmls_str]
                f_result = parallel_pool(delayed_funcs)
                ## aggregate results 
                #log_history,content_history,log_df = extract_multi_p_results(f,f_result,log_history,content_history)
                log_history,content_chunk,log_df = extract_multi_p_results(f,f_result,log_history,
                                                                           content_history=None,content_accumulate=False)
                ## export results 
                log_df.to_csv(log_file,index=False)
                content_df = pd.DataFrame(content_chunk)
                content_df.to_csv(csv_file,encoding='utf-8',index=False,header=header_status,mode=mode) 
#                save_as_json(content_history,json_file)
                
                logger.info('counter: {}; header: {}; mode: {}; length: {}'.format(counter,header_status,mode,len(content_chunk)))
                logger.info('one example: {}'.format(content_chunk[-1]['bio_doc-number']))
        
    save_as_json(error_file,error_log_path)
        
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

##%% Test out one file 
#    test_xml_path = "../../test/test2.xml"
#    with open(test_xml_path,'r') as f:
#        xml_content= f.read()
#    x_f = Patent_Parser(xml_content,verbose=False)
#    x_f.get_all_info()
#    print(x_f.json)
