
import glob
import dbgz
import json
import tqdm
import numpy as np
import pandas as pd
import igraph as ig
import xnetwork as xn
import dask.dataframe as dd

from pathlib import Path
from functools import partial
from itertools import combinations
from multiprocessing import Pool
from collections import defaultdict
from tqdm.contrib.concurrent import process_map 


# valid_journals = glob.glob("/mnt/e/WoS/ACS/*.xnet")
# valid_journals = [file[15:-19].lower() for file in valid_journals]

chu_journal = 'ACS Appl. Mater. Interfaces'

journals = [
    chu_journal, 'ACS Nano', 'Nano Lett.', 'Nat. Nanotechnol.', 
            'Nat. Mater.', 'Langmuir', "J. Mater. Chem. C", "J. Mater. Chem. A", 
            "J. Mater. Chem.", 
            'Adv. Funct. Mater.', 'Adv. Mater.',
            'J. Mat. Chem. B', 'Chem. Mat.', 
            'J. Am. Chem. Soc.',
            'Macromolecules']


valid_journals = [j.lower() for j in journals]
print(valid_journals)

dataPath = Path("/mnt/e/WoS/")
WOSDataPath = dataPath / "Data/WoSAggregatedData2020_ALL.bgz"


def select_valid(valid_journals, article):
    journal = article['titles']['title']
    valid = False
    name = ''
    for key in journal:
        if key['_type'] == 'abbrev_iso' and key['_VALUE'].lower() in valid_journals:
            valid = True
            break
    if valid:
        outputname = article['UID'].replace(':', '')
        out = open('papers_valid/%s.json' % outputname, 'w')
        out.write(json.dumps(article))
        out.close()

        
        
def get_cits_net(names, file):
    edges = []
    article = json.loads(open(file).read())
    article_uid = article['UID'].replace(':','')
    refs = []
    if 'reference' in article:
        for ref in article['reference']:
            if 'uid' in ref:
                refs.append(ref['uid'].replace(':',''))
    for ref in refs:
        if ref in names:
            edges.append((article_uid, ref))
    article_journal = ''
    for title in article['titles']['title']:
        if title['_type'] == 'abbrev_iso':
            article_journal = title['_VALUE'].lower()

            break
    
    title = ''
    if 'titles' in article:
        for title1 in article['titles']['title']:
            if title1['_type'] == 'item':
                title = title1['_VALUE']
        
    if 'abstract_text' in article:
        abstract = article['abstract_text']['p']
    else:
        abstract = ''
    year = article['pub_info']['_pubyear']

    return article_uid, article_journal, abstract, year, title, refs, edges,

    
if __name__ == '__main__':

#     g = xn.xnet2igraph('subset_chu/citation_net_11journal_15102022.xnet')

    g = xn.xnet2igraph('citation_net_170122.xnet')
    print(g.vcount(), g.ecount())
    
    journal11 = {'j. mater. chem. c', 'adv. funct. mater.', 'acs nano', 'chem. mat.', 'acs appl. mater. interfaces', 'nat. mater.', 'j. mat. chem. b', 'nat. nanotechnol.', 'nano lett.', 'j. mater. chem. a', 'adv. mater.'}
    
    vertex_seq = g.vs.select(journal_in=journal11, year_ge=2010, year_le=2020)
    
    subg  = g.subgraph(vertex_seq)
    print(subg.vcount(), subg.ecount())
    xn.igraph2xnet(subg, 'subset_chu/citation_net_11journal_09112022.xnet')
