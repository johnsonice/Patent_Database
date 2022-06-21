# -*- coding: utf-8 -*-
"""
Created on Wed Apr  6 08:56:39 2022

@author: chuang
"""

import os,sys,re
sys.path.insert(0,'../lib/')
from bs4 import BeautifulSoup
from download_util import flatten,save_as_json
import glob
import logging
import pandas as pd
import joblib
from joblib import Parallel, delayed
#from functools import wraps
from xml2csv import exception_handler,extract_multi_p_results,Patent_Parser,read_split_raw_xml 
sys.setrecursionlimit(2000)
print(sys.getrecursionlimit())

logging.basicConfig(level=logging.INFO,format='%(levelname)s - %(message)s') #set up the config
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

#%%

class Patent_Parser_SGML(Patent_Parser):
    def __init__(self,xml_content,verbose=False):
        super(Patent_Parser_SGML, self).__init__(xml_content=None,verbose=False)
        if xml_content is None and verbose:
            logger.info('initiate empty sub obj')
        else:
            if verbose:
                logger.info('initiate sub obj')
            soup = BeautifulSoup(xml_content,'lxml')
            self.root = soup.body.p.find('patdoc')
#            ## modify the initial json, add assignee place holder 
#            self.json['bio']['assignee']={'name':None,
#                                            'city':None,
#                                            'state':None,
#                                            'country':None,
#                                            'zip':None
#                                            }
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
        self.get_national_cls()
        self.get_ipc_cls()
        
        self.consolidate()
        
        if flat:
            self.json = self.dict_flatten(self.json)
            
        return doc_number,bio_status,abstract_status,des_status,claim_status,applicant_status
    
    def get_bio(self):
        '''
        get bio information 
        '''
        try:
            ## get publication info 
            publication_ref = self.root.sdobi.find('b100')
            self.json['bio']['doc-number'] = publication_ref.find('b110').find('pdat').text.strip()
            self.json['bio']['country'] = publication_ref.find('b190').find('pdat').text.strip()
            self.json['bio']['date'] = publication_ref.find('b140').find('date').find('pdat').text.strip()
            
            ## get application date info
            foreign_ref = self.root.sdobi.find('b300')
            if foreign_ref is not None:
                self.json['bio']['application_date'] = foreign_ref.find('b320').find('date').find('pdat').text.strip()
            else:
                application_ref = self.root.sdobi.find('b200')
                self.json['bio']['application_date'] = application_ref.find('b220').find('date').find('pdat').text.strip()
            
            status = True
        except Exception as e:
            if self.verbose:
                logger.warning('{}: bio info not found'.format(self.json['bio']['doc-number']))
            status = False        
        return status
    
    @exception_handler(error_msg = 'classification info not found')
    def get_national_cls(self):
        ## get national codes
        try:
            nationals_code = 'US ' + self.get_tag_text(self.root.find('b500').find('b520').find('b521'))  ## we will just take the first one; find all get some from citations
            status=True
        except:
            nationals_code=''
            status=False
                        
        self.json['bio']['main-classification']['national']=nationals_code
        return status

    @exception_handler(error_msg = 'classification info not found')
    def get_ipc_cls(self):
        ## get national codes
        try:
            ipc_tag = self.root.find('b500').find('b510')
            ipc_code = '{} [{}]'.format(ipc_tag.find('b511').text,
                                        ipc_tag.find('b516').text)
            status=True
        except:
            ipc_code=''
            status=False
                        
        self.json['bio']['main-classification']['ipc']=ipc_code
        return status
            
    @exception_handler(error_msg = 'applicant info not found')
    def get_applicat_info(self):
        inventor_tag = self.root.find('b720').find('b721').find('party-us')
        first_name = self.get_tag_text(inventor_tag.nam.fnm)
        last_name = self.get_tag_text(inventor_tag.nam.snm)
        city = self.get_tag_text(inventor_tag.adr.city)
        state = self.get_tag_text(inventor_tag.adr.state)
        organization_name = None
        country = None
        
        self.json['bio']['applicant']={'first_name':first_name,
                                 'last_name':last_name,
                                 'organization_name':organization_name,
                                 'country':country,
                                 'state':state,
                                 'city':city}
        ### for older version, make inventor same as applicate ###
        self.json['bio']['inventor']={'first_name':first_name,
                         'last_name':last_name,
                         'organization_name':organization_name,
                         'country':country,
                         'state':state,
                         'city':city}
        return True
    
    @exception_handler(error_msg = 'inventor info not found')
    def get_inventor_info(self):
        ### for older version, make inventor same as applicate ###
        self.get_applicat_info()
        return True
    
    @exception_handler(error_msg = 'assignee info not found')
    def get_assignee_info(self):
        assg = self.root.find('b730') ## if error then, no invitor info available 
        self.json['bio']['assignee']['name'] = assg.find('nam').text.strip()
        self.json['bio']['assignee']['country'] = None
        self.json['bio']['assignee']['state'] = assg.find('adr').find('state').text.strip()
        self.json['bio']['assignee']['city'] = assg.find('adr').find('city').text.strip()
        #self.json['bio']['assignee']['zip'] = None
        return True
    
    @exception_handler(error_msg = 'abstract info not found')
    def get_abstract(self):
        abstract = self.root.find('sdoab')  
        cs = abstract.findAll('pdat')
        ps = [p.text.strip() for p in cs]
        self.json['abstract']['paras']=ps
        return True
    
    #@exception_handler(error_msg = 'des info not found')
    def get_description(self):
        description = self.root.find('sdode')  
        cs = description.findAll('pdat')
        ps = [p.text.strip() for p in cs]
        self.json['description']['paras']=ps
        return True

    @exception_handler(error_msg = 'claim info not found')
    def get_claim(self):
        claims = self.root.find('sdocl')
        cs = claims.findAll('pdat')
        ps = [p.text.strip() for p in cs]
        self.json['claims']['claims']=ps
        #raise('test')
        return True
    
    @exception_handler(error_msg = 'consolidation error')
    def consolidate(self):
        ## udpate country info
        foreign_tag = self.root.find('b300')
        if foreign_tag is not None:           ## see if there are foreign country priority info if so, update 
            country = foreign_tag.find('b330').find('ctry').text.strip()
            self.json['bio']['applicant']['country'] = country
            self.json['bio']['assignee']['country'] = country
        else:
            self.json['bio']['country'] = 'US'
            if self.json['bio']['applicant']['country'] is None:
                self.json['bio']['applicant']['country'] = 'US'
            if self.json['bio']['assignee']['country'] is None:
                self.json['bio']['assignee']['country']='US'

        return True
    
def multi_process_func(txt_content,verbose=False):
    """
    delayed process function for joblib 
    """
    x_f = Patent_Parser_SGML(txt_content,verbose=verbose)
    log_x = x_f.get_all_info()
    c_x = x_f.json
    
    return log_x,c_x

#%%
if __name__ == '__main__':
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
    number_of_cpu = joblib.cpu_count() - 2 
    parallel_pool = Parallel(n_jobs=number_of_cpu,verbose=5)
    
    ## single process for debuging     
    for year in range(2001,2005):
        if year == 2001:
            fs = glob.glob(raw_txt_folder + '/{}/**/*.sgm*'.format(year),recursive = True)
            split_term = '<!DOCTYPE PATDOC'
            log_file = os.path.join(processed_folder,'extract_log_sgm_{}.csv'.format(year))
            csv_file = os.path.join(processed_folder,'{}_sgm.csv'.format(year))
        else:
            fs = glob.glob(raw_txt_folder + '/{}/**/*.xml'.format(year),recursive = True)
            split_term = '<?xml'
            log_file = os.path.join(processed_folder,'extract_log_{}.csv'.format(year))
            csv_file = os.path.join(processed_folder,'{}.csv'.format(year))
        
        log_history = []
        content_history = []
        error_file =[]
        ## go through each file in one year
        
        counter = 0 
        for f in fs:
            logger.info('processing file : {}'.format(f))
            try:
                cs = read_split_raw_xml(f,split_term = split_term)
            except:
                error_file.append(f)
                continue
            
            ##########################
            ## for dubugging purpose##
            ##########################
            if debug:
                log_x,c_x = multi_process_func(cs[0],verbose=False)
                #print(c_x)
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
    

    #%% test one document 
#    test_file = 'F:/Data/USTPO/raw_txt/in/2003/pg030211/pg030211.XML'
#    xmls = read_split_raw_xml(test_file,split_term = "<?xml") # or <!DOCTYPE PATDOC
#    for i in range(0,10):    
#        P = Patent_Parser_SGML(xmls[i])
#        P.get_all_info()
#        print(P.json)