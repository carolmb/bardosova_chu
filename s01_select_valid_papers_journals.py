import sys
import WOS
import glob
import dbgz
import time, json

import numpy as np
import pandas as pd
import xnetwork as xnet
import matplotlib.pyplot as plt

from copy import copy
from pathlib import Path
from tqdm.auto import tqdm
from collections import Counter
from itertools import combinations

from collections import defaultdict, namedtuple


# https://images.webofknowledge.com/images/help/WOS/hp_subject_category_terms_tasca.html

processedPath = Path("../../WoS/Data/Processed/")

WoSPaperCitationDataFilepath = processedPath/"WoSPaperCitationData.bgz"
WoSPaperCitationDataIndexByIDFilepath = processedPath/"WoSPaperCitationData_indexByID.ibgz"
WoSPaperCitationDataIndexByVenueFilepath = processedPath/"WoSPaperCitationData_indexByVenue.ibgz"
WoSPaperCitationDataIndexByCategoriesFilepath = processedPath/"WoSPaperCitationData_indexByCategories.ibgz"
WoSPaperCitationDataIndexByReferencesFilepath = processedPath/"WoSPaperCitationData_indexByReferences.ibgz"

WoSPaperCitationDataIndexByVenueOnlyChemistryFilepath = processedPath/"WoSPaperCitationData_indexByVenue_onlyChemistry.ibgz"

chu_journal = 'ACS Appl. Mater. Interfaces'


journals = [
    chu_journal, 'ACS Nano', 'Nano Lett.', 'Nat. Nanotechnol.', 
            'Nat. Mater.', 'Langmuir', "J. Mater. Chem. C", "J. Mater. Chem. A", 
            "J. Mater. Chem.", 
            'Adv. Funct. Mater.', 'Adv. Mater.',
            'J. Mat. Chem. B', 'Chem. Mat.', 
            'J. Am. Chem. Soc.',
            'Macromolecules']

journals = [j.lower() for j in journals]

journals_issn = ['1944-8244', '1944-8252',
                 '1936-0851', '1936-086X',
                 '1530-6984', '1530-6992', # print, web
                 '1748-3387', '1748-3395',
                 '1476-1122', '1476-4660',
                 '0743-7463', '1520-5827',
                 '2050-7496', '2050-7518', '2050-7534',  # online only a b or c?
                 '1521-4095',
                 '1616-3028'
                ]


def step0(chu_journal):
    
    papers = pd.read_csv('paper_journals_cat_170122.csv', sep='\t', header=None)
    papers.columns = ['journal', 'id', 'year', 'categories', 'refs']
    print(set(papers['journal']))
    
    chu_papers = papers[papers.journal == chu_journal]
    chu_papers = set(chu_papers['id'].tolist())
    print('step0', chu_journal, len(chu_papers))
    
    Paper = namedtuple('Paper', ['id', 'journal', 'year', 'cat', 'refs'])

    entryIndex = 0
    paper2journal = defaultdict(lambda:[])
    output = open('papers_citing_journal_%s.csv' % chu_journal, 'w')
    
    with dbgz.DBGZReader(WoSPaperCitationDataFilepath) as citationDB:
        print("Scheme: ", citationDB.scheme)
        print("Number of Entries:", citationDB.entriesCount)
        pbar = tqdm(total=citationDB.entriesCount)
        while True:
            entries = citationDB.read(100)
            if(not entries): 
                break
            for entry in entries:
                refs = set(entry['References'])
                if len(refs & chu_papers) > 0:
                    p = Paper(entry['ID'], entry['VenueName'], entry['Year'], ';'.join(entry['Categories']), ';'.join(entry['References']))
                    output.write('%s\t%s\t%s\t%s\t%s\n' % p)

                entryIndex += 1
            pbar.update(len(entries))

    output.close()

    pbar.refresh()
    pbar.close()




def step1():
    Paper = namedtuple('Paper', ['id', 'year', 'cat', 'refs'])
    journals_set = set()
#     entryIndex = 0
    paper2journal = defaultdict(lambda:[])
    with dbgz.DBGZReader(WoSPaperCitationDataFilepath) as citationDB:
        print("Scheme: ", citationDB.scheme)
        print("Number of Entries:", citationDB.entriesCount)
        pbar = tqdm(total=citationDB.entriesCount)
        while True:
            entries = citationDB.read(100)
            if(not entries): 
                break
            for entry in entries:
                for temp_name in entry['VenueNames']:
                    if temp_name.lower() in journals:
                        p = Paper(entry['ID'], entry['Year'], ';'.join(entry['Categories']), ';'.join(entry['References']))
                        paper2journal[entry['VenueName']].append(p)
#                         break
#                 entryIndex += 1
#             if entryIndex > 100000:
#                 break
            pbar.update(len(entries))

    print(paper2journal.keys())
    output = open('paper_journals_cat_170122.csv', 'w') # tmep
    for journal, papers in paper2journal.items():
        for paper in papers:
            output.write(journal+'\t'+('%s\t%s\t%s\t%s\n' % paper))

    output.close()

    pbar.refresh()
    pbar.close()

    
def step2(chu_journal, dyear):
    papers = pd.read_csv('paper_journals_cat_170122.csv', sep='\t', header=None)
    papers.columns = ['journal', 'id', 'year', 'categories', 'refs']
    chu_papers = papers[papers.journal == chu_journal]
    for i, group in chu_papers.groupby('year'):
        if i > 2010:
            print('numero de citações', i, len(group))
    chu_papers = set(chu_papers['id'].tolist())

    all_papers_citing_journal = pd.read_csv('papers_citing_journal_%s.csv' % chu_journal, sep='\t', header=None)
    all_papers_citing_journal.columns = ['id', 'journal', 'year', 'categories', 'refs']
    all_papers_citing_journal = all_papers_citing_journal.dropna()
    print(all_papers_citing_journal.head())
    
    divs = []
    for y in range(2010, 2020): # AQUI FOI MUDADO PARA APENAS OS ANOS MAIS RECENTES
        cats_weight = []
        subset_papers = all_papers_citing_journal[(all_papers_citing_journal.year >= y) & (all_papers_citing_journal.year <= y+dyear)] 
        # lista de todos os artigos que citam a revista CHU_JOURNAL agrupados por revista
        for journal, group_papers in subset_papers.groupby('journal'):
            jcats_temp = group_papers.iloc[0].categories.split(';') # categorias daquela revista
            ncats = len(jcats_temp) # número de categorias
            jcats = defaultdict(lambda:0)
            for cat in jcats_temp: # o peso de cada categoria é igualmente dividido
                jcats[cat] += 1/ncats

            jrefs = 0
            group_papers = group_papers.dropna()
            for _,row in group_papers.iterrows(): # artigos daquela revista 
                references = row.refs.split(';')
                chu_references = set(references) & chu_papers # seleciona as referências que são da CHU_JOURNAL
                nrefs = len(chu_references)
                if nrefs > 0:
                    jrefs += nrefs # jrefs = número de referências que JOURNAL deu para CHU_JOURNAL

            cats_weight.append((journal, jcats, jrefs))

        # gera o dicionario com os pesos de cada categoria
        nrefs = 0
        wcats = defaultdict(lambda:0)
        for j,cs,r in cats_weight: # revista, dict com peso das categorias
            for c,w in cs.items():
                wcats[c] += w*r
            nrefs += r

        if nrefs <= 0:
            print('não tem referências')
            divs.append(-1)
        else:
            p = []
            for c,w in wcats.items():
                p.append(w/nrefs)
                print(c, "%.4f" % p[-1])

            d = np.exp(-np.sum(np.log(p) * p))
            print('year', y, 'd index', d, 'nrefs', nrefs, 'p len', len(p))
    
            divs.append(d)
    return divs


# def get_impact(chu_journal): # citations
    
#     dyear = 1
#     papers_per_year = json.loads(open('bardo_paper_per_year_190122.json').read())
#     papers_per_year = papers_per_year[chu_journal.lower()]
    
#     papers = pd.read_csv('paper_journals_cat_170122.csv', sep='\t', header=None)
#     papers.columns = ['journal', 'id', 'year', 'categories', 'refs']
#     chu_papers = papers[papers.journal == chu_journal]
#     chu_papers = set(chu_papers['id'].tolist())

#     net = xnet.xnet2igraph("subset_chu/citation_net_%s_180122_bardo.xnet" % chu_journal)
            
#     all_papers_citing_journal = pd.read_csv('papers_citing_journal_%s.csv' % chu_journal, sep='\t', header=None)
#     all_papers_citing_journal.columns = ['id', 'journal', 'year', 'categories', 'refs']
#     all_papers_citing_journal = all_papers_citing_journal.dropna()
#     print(all_papers_citing_journal.head())
    
#     bardo_dist = defaultdict(lambda: defaultdict(lambda: 0.0))
#     for y in range(1995, 2020):
#         print(y)
#         subset_papers = all_papers_citing_journal[(all_papers_citing_journal.year >= y) & (all_papers_citing_journal.year <= y+dyear)]
#         all_refs = []
#         for _, paper in subset_papers.iterrows():
#             refs = paper['refs'].split(';')
#             refs = [r.replace(':', '') for r in refs]
#             all_refs += refs
        
#         unique, count = np.unique(all_refs, return_counts=True)
#         count_cit = dict()
#         for u, c in zip(unique, count):
#             count_cit[u] = int(c)
#         cited_papers = net.vs.select(wos_id_in = set(all_refs))

#         for vtx_paper in cited_papers:
#             bardo_comm = vtx_paper['Cluster Name']
#             if y <= vtx_paper['year'] <= y + dyear:
#                 bardo_dist[bardo_comm][y + dyear + 1] += count_cit[vtx_paper['wos_id']]
    
#     out_copy = dict()
#     C_others = defaultdict(lambda:0)
#     P_others = defaultdict(lambda:0)
#     for comm, count_cit_per_year in bardo_dist.items():
#         if comm[0] in ['A', 'B', 'C', 'D', 'E', 'F', 'G']:
#             out_copy[comm] = dict()
        
#         for y, count_cit in count_cit_per_year.items():
#             y1 = 0
#             y2 = 0
#             if str(y-1) in papers_per_year[comm]:
#                 y1 = len(papers_per_year[comm][str(y-1)])
#             if str(y-2) in papers_per_year[comm]:
#                 y2 = len(papers_per_year[comm][str(y-2)])
            
#             if comm[0] not in ['A', 'B', 'C', 'D', 'E', 'F', 'G']:
#                 C_others[y] += count_cit
#                 P_others[y] += (y1 + y2)
#             else:
#                 if y1 + y2 > 0:
#                     out_copy[comm][y] = count_cit/(y1 + y2)
#                 else:
#                     out_copy[comm][y] = 0
#     out_copy['Others'] = dict()
#     for y, v in C_others.items():
#         out_copy['Others'][y] = v/P_others[y]
    
#     json_str = json.dumps(out_copy)
#     out = open('impact_factor_dist_%s_120222.json' % chu_journal, 'w')
#     out.write(json_str)
#     out.close()
    

if __name__ == '__main__':
    
    
#     step1()
    
    
    journals = [
    chu_journal, 'ACS Nano', 'Nano Lett.', 'Nat. Nanotechnol.', 
            'Nat. Mater.', 'Langmuir', "J. Mater. Chem. C", "J. Mater. Chem. A", 
            "J. Mater. Chem.", 
            'Adv. Funct. Mater.'.lower(), 'Adv. Mater.'.lower(),
            'J. Mat. Chem. B'.lower(), 'Chem. Mat.'.lower(), 
            'J. Am. Chem. Soc.'.lower(),
            'Macromolecules'.lower()]
    
#     for j in journals:
#         print(j)
#         step0(j)

    journals = [
#     chu_journal, 'ACS Nano', 'Nano Lett.', 'Nat. Nanotechnol.', 
#             'Nat. Mater.', 
        'Langmuir', 
        "J. Mater. Chem. C", "J. Mater. Chem. A", 
#             "J. Mater. Chem.", 
#             'Adv. Funct. Mater.', 'Adv. Mater.',
#             'J. Mat. Chem. B', 'Chem. Mat.', 
            'J. Am. Chem. Soc.',
#             'Macromolecules'
    ]

    
#     out = open('divs_comp_180122.csv', 'w')
    for j in journals:
        
        print('current', j)
        d = step2(j, 0)
#         out.write(j+',')
#         for t in d:
#             out.write("%.3f," % t)
#         out.write('\n')
        
# #     out.close()
#     for j in journals:
#         print(j)
#         get_impact(j)
#         # tornar paralelo
        