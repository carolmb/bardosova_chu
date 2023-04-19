import json
import pandas as pd
import numpy as np
import xnetwork as xnet
from tqdm.auto import tqdm
from collections import Counter
from itertools import combinations
from collections import defaultdict, namedtuple
from multiprocessing import Pool
    
def get_impact(chu_journal): # citations
    
    dyear = 1
    
    papers_per_year = json.loads(open('bardo_paper_per_year_040123_nat_mat.json').read())
    papers_per_year = papers_per_year[chu_journal.lower() + '_']
    
    '''
    papers_per_year = json.loads(open('bardo_paper_per_year_040123_%s.json' % chu_journal).read()) #'310522.json').read())
    
    for a,b in papers_per_year.items():
        temp = b
    papers_per_year = temp
    '''
    
    papers = pd.read_csv('paper_journals_cat_170122.csv', sep='\t', header=None)
    papers.columns = ['journal', 'id', 'year', 'categories', 'refs']
    chu_papers = papers[papers.journal == chu_journal]
    chu_papers = set(chu_papers['id'].tolist())

#     net = xnet.xnet2igraph("subset_chu/citation_net_%s_180122_bardo.xnet" % chu_journal)
#     net = xnet.xnet2igraph("subset_chu/citation_net_%s_voting_schema_261022.xnet" % chu_journal)
    
    net = xnet.xnet2igraph("subset_chu/citation_net_%s_011122_voting_schema_bardo_infomap_01112022.xnet" % chu_journal)
    
    all_papers_citing_journal = pd.read_csv('papers_citing_journal_%s.csv' % chu_journal, sep='\t', header=None)
    all_papers_citing_journal.columns = ['id', 'journal', 'year', 'categories', 'refs']
    all_papers_citing_journal = all_papers_citing_journal.dropna()
    print(all_papers_citing_journal.head())
    
    bardo_dist = defaultdict(lambda: defaultdict(lambda: 0.0))
    for y in range(1995, 2020):
#         subset_papers = all_papers_citing_journal[(all_papers_citing_journal.year >= y) & (all_papers_citing_journal.year <= y+dyear)]
        subset_papers = all_papers_citing_journal[all_papers_citing_journal.year == y + dyear + 1]
        all_refs = [] 
        for _, paper in subset_papers.iterrows():
            refs = paper['refs'].split(';')
            refs = [r.replace(':', '') for r in refs]
            all_refs += refs
        
        unique, count = np.unique(all_refs, return_counts=True)
        count_cit = dict()
        for u, c in zip(unique, count):
            count_cit[u] = int(c)
        cited_papers = net.vs.select(wos_id_in = set(all_refs))

        for vtx_paper in cited_papers:
            bardo_comm = vtx_paper['Cluster Name']
            if y <= vtx_paper['year'] <= y + dyear:
                bardo_dist[bardo_comm][y + dyear + 1] += count_cit[vtx_paper['wos_id']]
    
    print(papers_per_year.keys())
    out_copy = dict()
    C_others = defaultdict(lambda:0)
    P_others = defaultdict(lambda:0)
    for comm, count_cit_per_year in bardo_dist.items():
        if comm[0] in ['A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I']:
            out_copy[comm] = dict()
        
        for y, count_cit in count_cit_per_year.items():
            y1 = 0
            y2 = 0
            if str(y-1) in papers_per_year[comm]:
                y1 = len(papers_per_year[comm][str(y-1)])
            if str(y-2) in papers_per_year[comm]:
                y2 = len(papers_per_year[comm][str(y-2)])
            
            print(comm, y1, y2, count_cit)
            if comm[0] not in ['A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I']:
                C_others[y] += count_cit
                P_others[y] += (y1 + y2)
            else:
                if y1 + y2 > 0:
                    out_copy[comm][y] = count_cit/(y1 + y2)
                else:
                    out_copy[comm][y] = 0
    out_copy['Others'] = dict()
    for y, v in C_others.items():
        out_copy['Others'][y] = v/P_others[y]
    
#     json_str = json.dumps(out_copy)
#     out = open('impact_factor_dist_%s_010622_IFv2_300323.json' % chu_journal, 'w')
#     out.write(json_str)
#     out.close()

    
def get_impact_by_journal(chu_journal): # citations
    
    dyear = 1
    papers_per_year = json.loads(open('bardo_paper_per_year_040123_%s.json' % chu_journal).read()) #'310522.json').read())
    for a,b in papers_per_year.items():
        temp = b
    papers_per_year = temp
#     papers_per_year = papers_per_year[chu_journal.lower() + '_']
    
    papers = pd.read_csv('paper_journals_cat_170122.csv', sep='\t', header=None)
    papers.columns = ['journal', 'id', 'year', 'categories', 'refs']
    chu_papers = papers[papers.journal == chu_journal]
    chu_papers = set(chu_papers['id'].tolist())

#     net = xnet.xnet2igraph("subset_chu/citation_net_%s_180122_bardo.xnet" % chu_journal)
#     net = xnet.xnet2igraph("subset_chu/citation_net_%s_voting_schema_261022.xnet" % chu_journal)
    
    net = xnet.xnet2igraph("subset_chu/citation_net_%s_011122_voting_schema_bardo_infomap_01112022.xnet" % chu_journal)
    
    all_papers_citing_journal = pd.read_csv('papers_citing_journal_%s.csv' % chu_journal, sep='\t', header=None)
    all_papers_citing_journal.columns = ['id', 'journal', 'year', 'categories', 'refs']
    all_papers_citing_journal = all_papers_citing_journal.dropna()
    print(all_papers_citing_journal.head())
    
    bardo_dist = defaultdict(lambda: 0.0)
    for y in range(1995, 2020):
#         subset_papers = all_papers_citing_journal[(all_papers_citing_journal.year >= y) & (all_papers_citing_journal.year <= y+dyear)]
        subset_papers = all_papers_citing_journal[all_papers_citing_journal.year == y + dyear + 1]
        all_refs = [] 
        for _, paper in subset_papers.iterrows():
            refs = paper['refs'].split(';')
            refs = [r.replace(':', '') for r in refs]
            all_refs += refs
        
        unique, count = np.unique(all_refs, return_counts=True)
        count_cit = dict()
        for u, c in zip(unique, count):
            count_cit[u] = int(c)
        cited_papers = net.vs.select(wos_id_in = set(all_refs))

        for vtx_paper in cited_papers:
            bardo_comm = vtx_paper['Cluster Name']
            if y <= vtx_paper['year'] <= y + dyear:
                bardo_dist[y + dyear + 1] += count_cit[vtx_paper['wos_id']]
    
    out_copy = dict()
    C_others = defaultdict(lambda:0)
    P_others = defaultdict(lambda:0)
    for y, count_cit in bardo_dist.items():
        print(y)
        y1 = 0
        y2 = 0
        if str(y-1) in papers_per_year:
            y1 = len(papers_per_year[str(y-1)])
        if str(y-2) in papers_per_year:
            y2 = len(papers_per_year[str(y-2)])

        C_others[y] += count_cit
        P_others[y] += (y1 + y2)

        if y1 + y2 > 0:
            out_copy[y] = count_cit/(y1 + y2)
        else:
            out_copy[y] = 0
    
    json_str = json.dumps(out_copy)
    out = open('impact_factor_dist_%s_010622_IFv2_by_journal_260323.json' % chu_journal, 'w')
    out.write(json_str)
    out.close()

    

if __name__ == '__main__':

    chu_journal = 'ACS Appl. Mater. Interfaces'
    '''journals = [
    chu_journal, 'ACS Nano', 'Nano Lett.', 'Nat. Nanotechnol.', 
            'Nat. Mater.', 'Langmuir', 
        "J. Mater. Chem. C", "J. Mater. Chem. A", 
            "J. Mater. Chem.", 
            'Adv. Funct. Mater.', 'Adv. Mater.',
            'J. Mat. Chem. B', 'Chem. Mat.', 
            'J. Am. Chem. Soc.',
            'Macromolecules'
    ]'''
#     journals = ['nat. mater.']
    journals = ['adv. funct. mater.']
    
#     pool = Pool(10)
#     pool.map(get_impact, journals)
    
    for j in journals:
        print(j)
        get_impact(j)
        # tornar paralelo
        