import WOS
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


def get_coref(g, ids):
    i1, i2 = ids
    v1 = g.vs[i1]
    v2 = g.vs[i2]
    if set(v1['refs']) & set(v2['refs']):
        return (v1['name'], v2['name'])


def get_edges(vs_refs, i_iref):
    i, iref = i_iref
    output = open('temp/coref_%d_temp' % i , 'w')
    iref = set(iref.split(','))
    for j, jref in enumerate(vs_refs):
        if i == j:
            break
        commom = iref & set(jref.split(','))
        if len(commom) > 0:
            w = len(commom)
            edge = (i, j, w)
            output.write("%s\t%s\t%d\n" % edge)
    output.close()
    
    
if __name__ == '__main__':
#     reader = WOS.DatabaseReader(WOSDataPath)
#     pool = Pool(20)    
#     valid_articles = defaultdict(lambda:[])
#     c = 0
#     while True:
#         articles = reader.readNextArticles(500)
#         if not articles: 
#             break
#         if len(articles) == 0:
#             break
#         pool.map(partial(select_valid, valid_journals), articles)
#         c += 1
#         print(c, end='\r')
        
#     reader.close()        

#     ---------------------------------------------------------------------------
    '''
    files = glob.glob('papers_valid/*.json')
    names = set([f[13:-5] for f in files])
    print(len(names))
    print(list(names)[:10])
    
    result = process_map(partial(get_cits_net, names), files, max_workers=25, chunksize=1000)
    
    g = ig.Graph(directed=True)
    g.add_vertices(len(result))
    g.vs['name'] = [r[0] for r in result]
    g.vs['year'] = [r[-4] for r in result]
    print(set(g.vs['year']))
    g.vs['title'] = [r[-3].replace('\n', '').replace('"', '').replace("'", '') for r in result]
    g.vs['refs'] = [','.join(r[-2]) for r in result]
    g.vs['journal'] = [r[1] for r in result]
#     g.vs['abstract'] = ['\t'.join(r[2]) for r in result]

    edges = [r[-1] for r in result]
    edges_flat = [e_ for e in edges for e_ in e]
    g.add_edges(edges_flat)
    
    xn.igraph2xnet(g, 'citation_net_170122.xnet')
    '''
    #     ---------------------------------------------------------------------------

    g = xn.xnet2igraph('citation_net_170122.xnet')
    print(g.vcount(), g.ecount())
    
    for journal in set(g.vs['journal']):
        print(journal)
        vertex_seq = g.vs.select(journal_eq=journal, year_eq=2021)
        print(len(vertex_seq))
        
#         subg  = g.subgraph(vertex_seq)
#         xn.igraph2xnet(subg, 'subset_chu/citation_net_%s_011122.xnet' % journal)
