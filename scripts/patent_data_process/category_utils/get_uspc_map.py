# -*- coding: utf-8 -*-
"""
Created on Mon Jan 10 20:18:44 2022

@author: CHuang
"""
### scrape USPC mapping 

from bs4 import BeautifulSoup
import requests
import copy
import pandas as pd
import os,sys,re
try:
     script_path = os.path.dirname(os.path.abspath(__file__))
except:
    script_path = '.'
    
sys.path.insert(0,os.path.join(script_path,'../../lib/'))
from download_util import request_get_n_try,save_as_json,load_from_json,save_as_pkl,load_from_pkl
import logging
logging.basicConfig(level=logging.INFO,format='%(levelname)s - %(message)s') #set up the config
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
 
#%%

def get_tag_text(tag):
    if tag is None:
        return tag
    else:
        return tag.text.strip()
    
def get_all_links(url="https://www.uspto.gov/web/patents/classification/selectnumwithtitle.htm",
                      id_only=False,verbose=True):
    
    #url = 'https://www.uspto.gov/web/patents/classification/selectnumwithtitle.htm'
    page=requests.get(url)
    soup = BeautifulSoup(page.text, 'html.parser')
    #mainArea > div.container > div > div:nth-child(3) > div > form > table:nth-child(6) > tbody
    nums = soup.findAll("td", {"valign" : "top","width":"27"})
    all_ids = [(n.text,n.findNextSibling().text) for n in nums]
    if id_only:
        return all_ids
    
    l_p = 'https://www.uspto.gov/web/patents/classification/uspc{}/sched{}.htm'
    link_dict = {i:{'name':n,'link':l_p.format(i.lower(),i.lower())} for i,n in all_ids}
    
    return link_dict


def get_all_mapping_links(url="https://www.uspto.gov/web/patents/classification/selectnumwithtitle.htm"):
    all_ids = get_all_links(url,id_only=True)
    l_p = 'https://www.uspto.gov/web/patents/classification/uspc{}/us{}toipc8.htm#ipcinfo'
    link_dict = {i:{'name':n,'link':l_p.format(i.lower(),i.lower())} for i,n in all_ids}
    return link_dict

def get_ipc_uspc_map():
    """
    get ipc to uspc big category map 
    
    return {'ipc_code':{‘’}}
    """
    link_dict = get_all_mapping_links()
    res = {}
    for cat_id,v in link_dict.items():
        name,link = v['name'],v['link']
        try:
            table = pd.read_html(link)[0]
        except:
            logger.warn('read page error: {}'.format(link))
        
        for index, row in table.iterrows():
            if index >1:
                code = '{} {}'.format(row[1].replace(' ',''),row[2].replace(' ',''))
                res[code]= {'cat_id':cat_id,
                           'name':name}
    
    return res 


def process_table(t):
    """
    input : t as xml tag
    """
    name = get_tag_text(t.find('td',{'class':'SubTtl'}))
    if name is None:
        return None
    
    code = get_tag_text(t.find('td',{'width':'99'}))
    
    level_tag = t.find('td',{'class':'SubTtl'}).big.img
    if level_tag is not None:
        level = int(level_tag.get('alt').split()[-1])
    else:
        level = 0
    
    return code,level,name

def get_rawinfo_from_link(url):
    """
    get triplet info from page 
    (id,level,name)
    """
    page=requests.get(url)
    soup = BeautifulSoup(page.text, 'html.parser')
    items = soup.findAll('table',recursive=True)
    res = [process_table(i) for i in items if process_table(i) is not None]
    
    return res 

class node(object):
    def __init__(self,id,name,parent,data=None):
        self.parent = parent
        self.id = id
        self.name = name
        self.data = data  ## {'level':0}
        
    
class Tree(object):
    def __init__(self,name='root'):
        root = node(id='root',name=name,parent=None,data={'level':-1})
        self.root = root
        self.level = {root.data['level']:[root.id]}
        self.nodes = {root.id:root}
        self.memory ={'recent_node':root.id,
                      'recent_nodes':{
                                      root.data['level']:root.id
                                      }
                      }
        
    def get_node(self,id):
        
        res_node = self.nodes.get(id)
        
        return res_node
        
    def add_node_seq(self,code,name,data,verbose=False):
        ## determent the correct parent id 
        pre_level =  self.get_node(id=self.memory['recent_node']).data['level']
        if data['level'] > pre_level:
            parent_id = self.memory['recent_node']
        elif data['level'] == pre_level:
            parent_id = self.get_node(self.memory['recent_node']).parent
        elif data['level'] < pre_level:
            parent_id =  self.get_node(self.memory['recent_nodes'][data['level']]).parent
        
        ## add new node to the tree 
        if code is None: ## make sure it is not None type
            code = ''
        if self.nodes.get(code) is not None:
            ## update dup code with new code by adding '_next' to previous one
            code = self.memory['recent_node']+'_next'    
            if verbose:
                logger.warn('node {} already existed; generate a new code for it'.format(code))
        
        new_node = node(code,name,parent=parent_id,data=data)
        self.nodes[code] = new_node
        if self.level.get(data['level']) is None:
            self.level[data['level']]= [code]
        else:
            self.level[data['level']].append(code)
    
        ## update memory 
        self.memory['recent_node'] = code
        self.memory['recent_nodes'][data['level']] = code
            
    def get_family_history(self,node_id,end_level=-1):
        
        node = self.get_node(node_id)
        family = {'ids':[node.id],'names':[(node.data['level'],node.name)]}
        parent_node = self.get_node(node.parent)
        family['ids'].insert(0,parent_node.id)
        family['names'].insert(0,(parent_node.data['level'],parent_node.name))
        
        while parent_node.data['level'] != end_level:
            parent_node = self.get_node(parent_node.parent)
            family['ids'].insert(0,parent_node.id)
            family['names'].insert(0,(parent_node.data['level'],parent_node.name))
            
        return family 
    
    def get_all_child_nodes(self,node_id):
        res_ids = []
        for nid in self.nodes:
            if nid == 'root':
                pass
            else:
                family_ids = self.get_family_history(nid)['ids']
                if node_id in family_ids:
                    res_ids.append(nid)
        return res_ids
        
#%%
if __name__ == '__main__':
    
    map_folder = r'C:\Users\chuang\OneDrive - International Monetary Fund (PRD)\Climate Change Challenge\USPTO\keywords\classification\uspc'
    raw_info_json = os.path.join(map_folder,'uscp_info.json')    
    tree_pickle = os.path.join(map_folder,'uscp_tree.p') 
    ipc_map_pickle = os.path.join(map_folder,'ipc_map.p') 
    overwrite=False  ## regenerate or load from cach
    
    if os.path.exists(raw_info_json) and overwrite is False:
        print('load json from disk')
        USPC_map = load_from_json(raw_info_json)
    else:
        link_dict = get_all_links()
        USPC_map = {}
        for cat_id,val in  link_dict.items():
            res = get_rawinfo_from_link(val['link'])
            USPC_map[cat_id] = {'name':val['name'],
                                'tree_info':res}
        ## cache scrapped info
        save_as_json(USPC_map,raw_info_json)
#%%
    if os.path.exists(tree_pickle) and overwrite is False:
        USPC_map=load_from_pkl(tree_pickle)
        print('load pickle from disk')
    else:
        for cat_id in USPC_map.keys():
            print('creating tree for {}'.format(cat_id))
            tree = Tree(name=USPC_map[cat_id]['name'])
            for idx, r in enumerate(USPC_map[cat_id]['tree_info']):
                code,level,name = r 
                if code is None:
                    code = ''
                code = code.replace('.','') ## remove all "." to be consistant from the database
                tree.add_node_seq(code,name,data={'level':level})
            USPC_map[cat_id]['tree'] = copy.copy(tree)
        
        save_as_pkl(USPC_map,tree_pickle)

#%%
    if os.path.exists(ipc_map_pickle) and overwrite is False:
        print('load pickle from disk')
        IPC_map = load_from_pkl(ipc_map_pickle)
    else:
        IPC_map = get_ipc_uspc_map()
        save_as_pkl(ipc_map,ipc_map_pickle)
        
        
        #%%
    print(USPC_map['D99']['tree'].get_family_history('42'))
    print(next(iter(IPC_map.values())))
        #%%
        
        
        
        
        
        
        
        
        
        
        
        
        
        
#        link_dict = get_all_mapping_links()
#        res = {}
#        for cat_id,v in link_dict.items():
#            name,link = v['name'],v['link']
#            try:
#                table = pd.read_html(link)[0]
#            except:
#                logger.warn('read page error: {}'.format(link))
#            
#            for index, row in table.iterrows():
#                if index >1:
#                    code = '{} {}'.format(row[1].replace(' ',''),row[2].replace(' ',''))
#                    res[code]= {'cat_id':cat_id,
#                               'name':name}
        
            
        
        
#    #%%
#    url = link_dict['PLT']['link']
#    page=requests.get(url)
#    soup = BeautifulSoup(page.text, 'html.parser')
#    items = soup.findAll('table',recursive=True)
#    res = [process_table(i) for i in items if process_table(i) is not None]

            
            
        

    
