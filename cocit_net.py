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


def get_cocit(valid_pid, paper):
    valid_refs = []
    if 'reference' in paper:
        for ref in paper['reference']:
            if 'uid' in ref:
                uid = ref['uid'].replace(':','')
                if uid in valid_pid:
                    valid_refs.append(uid)
        cocits = list(combinations(valid_refs, 2))
        return cocits
    return []


if __name__ == '__main__':
    g = xn.xnet2igraph('data/citation_net.xnet')
    valid_pid = set(g.vs['name'])

    
    dataPath = Path("/mnt/e/WoS/")
    WOSDataPath = dataPath / "Data/WoSAggregatedData2020_ALL.bgz"
    reader = WOS.DatabaseReader(WOSDataPath)
    
    pool = Pool(16)
    out = open('cocit_edges.csv', 'w')
    count = 0
    while True:
        articles = reader.readNextArticles(1000)
        if len(articles) <= 0:
            print()
            break
        
        print('running... %d' % count, end='\r')
        count += 1
        result = pool.map(partial(get_cocit, valid_pid), articles)
        
        for r in result:
            for pair in r:
                out.write("%s\t%s\n" % pair)
    
    out.close()
    reader.close()   
