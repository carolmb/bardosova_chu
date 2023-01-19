
from pathlib import Path
from tqdm.auto import tqdm
from multiprocessing import Pool
import importlib
import json
import dbgz

dataPath = Path("/mnt/e/WoS/WoS_2022_DBGZ/")
WOSDataPath = dataPath / "WoS_2022_All.dbgz"

def select_title(article):
    titles = article['titles']['title']
    for title in titles:
        if title['@type'] == 'item':
            return title['#text'].lower()
    return ''
    

keyword = 'chitosan'    

def select_keyword(article, keyword = 'chitosan'):
    try:
        return keyword in select_title(article['data']['static_data']['summary']) or keyword in select_abstract_text(article['data']['static_data'])
    except:
        return False
    
    
def select_abstract_text(entry):
    condition_abstract = entry['summary']['pub_info']['@has_abstract']
    if condition_abstract == 'Y':
        abstract = entry['fullrecord_metadata']['abstracts']['abstract']['abstract_text']

        if abstract['@count'] != '1':
            complete_abstract = ' '.join(abstract['p'])
        else:
            complete_abstract = abstract['p']
        return complete_abstract
    return ''
        
    
pool = Pool(16)
keyword_papers = []
entries = []
max_entries = 100000
i = 1
with dbgz.DBGZReader(WOSDataPath) as fd:
    for entry in tqdm(fd.entries,total=fd.entriesCount):   
        entries.append(entry)        
        if len(entries) > max_entries:
            select_papers = pool.map(select_keyword, entries)
            N = len(select_papers)
            for condition,article in zip(select_papers, entries):
                if condition:
                    keyword_papers.append(article)
            entries = []
         
    if len(entries) > 0:
        select_papers = pool.map(select_keyword, entries)
        N = len(select_papers)
        for condition,article in zip(select_papers, entries):
            if condition:
                keyword_papers.append(article)
                        
output = json.dumps(keyword_papers)
with open('json_%s_papers.json' % keyword, 'w') as outfile:
    outfile.write(output)
