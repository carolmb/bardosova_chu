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


valid_journals = glob.glob("/mnt/e/WoS/ACS/*.xnet")
valid_journals = [file[15:-19].lower() for file in valid_journals]


dataPath = Path("/mnt/e/WoS/")
WOSDataPath = dataPath / "WoSAggregatedData2020_ALL.bgz"


def select_valid(valid_journals, article):
    journal = article['titles']['title']
    valid = False
    name = ''
    for key in journal:
        if key['_VALUE'].lower() in valid_journals:
            valid = True
            name = key['_VALUE'].lower()
            break
    if valid:
        outputname = article['UID'].replace(':', '')
        out = open('subset_chu/%s.json' % outputname, 'w')
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
        if title['_type'] == 'source':
            article_journal = title['_VALUE'].lower()
            break
    if 'abstract_text' in article:
        abstract = article['abstract_text']['p']
    else:
        abstract = ''
    year = article['pub_info']['_pubyear']

    return article_uid, article_journal, abstract, year, refs, edges


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
            edge = (i, j)
            output.write("%s\t%s\n" % edge)
 
    
if __name__ == '__main__':
#     reader = WOS.DatabaseReader(WOSDataPath)
#     pool = Pool(16)    
#     valid_articles = defaultdict(lambda:[])
#     c = 0
#     while True:
#         articles = reader.readNextArticles(300)
#         pool.map(partial(select_valid, valid_journals), articles)

#     reader.close()        

#     ---------------------------------------------------------------------------
#     files = glob.glob('subset_chu/*.json')
#     names = set([f[11:-5] for f in files])
    
#     result = process_map(partial(get_cits_net, names), files, max_workers=20, chunksize=1000)
    
#     g = ig.Graph(directed=True)
#     g.add_vertices(len(result))
#     g.vs['name'] = [r[0] for r in result]
#     g.vs['year'] = [r[-3] for r in result]
#     g.vs['refs'] = [','.join(r[-2]) for r in result]
#     g.vs['journal'] = [r[1] for r in result]
#     g.vs['abstract'] = ['\t'.join(r[2]) for r in result]

#     edges = [r[-1] for r in result]
#     edges_flat = [e_ for e in edges for e_ in e]
#     g.add_edges(edges_flat)
    
#     xn.igraph2xnet(g, 'citation_net.xnet')
#     ---------------------------------------------------------------------------

    g = xn.xnet2igraph('citation_net.xnet')
#     vs_refs = g.vs['refs']
#     del g
    
#     output = open('coref_edges.csv', 'w')
#     n = len(vs_refs)
    
#     process_map(partial(get_edges, vs_refs), enumerate(vs_refs), max_workers=16, total=n)
#     output.close()
    
#     ---------------------------------------------------------------------------
    
    g_coref = ig.Graph(directed=True)
    g_coref.add_vertices(g.vcount())
    g_coref.vs['name'] = g.vs['name']
    g_coref.vs['year'] = g.vs['year']
    g_coref.vs['refs'] = g.vs['refs']
    g_coref.vs['journal'] = g.vs['journal']
    g_coref.vs['abstract'] = g.vs['abstract']
    
    chunksize = 100000
    c = 0
    for chunk in pd.read_csv('coref_edges_full.csv', chunksize=chunksize, sep='\t', header=None):
        print(c, end ='\r')
        c += 1
        edges = []
        for _,row in chunk.iterrows():
            edges.append((row[0], row[1]))
        g_coref.add_edges(edges)
    
    xn.igraph2xnet(g_coref, 'coref_net.xnet')
