'''
methods:
- select papers from valid journals
- select papers citing papers from valid journals
- calculates IF
- count number of papers per journal
'''


import dbgz, json
import WOSRaw as wos
import xnetwork as xnet
from pathlib import Path
from tqdm.auto import tqdm
from timeit import default_timer as timer

import glob
from functools import partial
from multiprocessing import Pool
from collections import defaultdict


# Path to the existing dbgz file
WOSArchivePath = Path("/mnt/e/WoS/WoS_2022_DBGZ/WoS_2022_All.dbgz")

current_journal1 = 'Adv. Funct. Mater.'.lower()
current_journal2 = 'ACS Appl. Mater. Interfaces'.lower()
items_journal = []

valid_journals = [
    'acs nano',
    'adv. mater.',
    'chem. mat.', 
    'j. am. chem. soc.', 
    'j. mat. chem. b', # correto
    'j. mater. chem. a',  # correto
    'j. mater. chem. c', 
    'langmuir',
    'macromolecules',
    'nano lett.',
    'nat. mater.', # correto
    'nat. nanotechnol.'
]

def get_papers_from_journal():
    # Reading the file sequentially
    with dbgz.DBGZReader(WOSArchivePath) as fd:
        # getting the number of entries
        print("\t Number of entries: ", fd.entriesCount)
        # getting the schema (currently UID and data)
        print("\t Scheme: ", fd.scheme)
        # TQDM can be used to print the progressbar
        for wosEntry in tqdm(fd.entries, total=fd.entriesCount):
            UID = wosEntry["UID"]
            entryData = wosEntry["data"] # XML records data
            sourceData = wos.utilities.getSources(wosEntry)
            title = wos.utilities.getSourceISOAbbreviation(sourceData) # Extracting the title
            title = title.lower()
            if title != '' and (title in valid_journals):
                items_journal.append(entryData)

    test = open('toy_allbut_acsami_afm.txt','w')
    for item in items_journal:
        test.write(json.dumps(item)+"\n")
    test.close()

# get_papers_from_journal()

# files = ['ACS Appl. Mater. Interfaces_completedata_110423.xnet', 'Adv. Funct. Mater._completedata_110423.xnet']

valid_uids = []
files = glob.glob('*_completedata_170423_allbut.xnet')[:1]
print(files)

for file in files:
    print(file)
    g = xnet.xnet2igraph(file)
    valid_uids += g.vs['name']

print(valid_uids[:10])
print(len(valid_uids))
valid_uids = set(valid_uids)


def get_citing():
    citing = []
    # Reading the file sequentially
    with dbgz.DBGZReader(WOSArchivePath) as fd:
        # getting the number of entries
        print("\t Number of entries: ", fd.entriesCount)
        # getting the schema (currently UID and data)
        print("\t Scheme: ", fd.scheme)
        # TQDM can be used to print the progressbar
        for wosEntry in tqdm(fd.entries, total=fd.entriesCount):

            referencesData = wos.utilities.getReferences(wosEntry)
            ref_uids = wos.utilities.getReferencesUIDs(referencesData)
            for current_uid in ref_uids:
                if current_uid in valid_uids:
                    citing.append(wosEntry)
                    break

    test = open('wosentries_citing_allbut_acm_afm.txt','w')
    for item in citing:
        test.write(json.dumps(item)+"\n")
    test.close()

get_citing()
    
def get_papers_year(valid_uids, filename):
    papers_by_year = dict()
    with open(filename) as file:
        for line in file:
            paper = json.loads(line)
            referencesData = wos.utilities.getReferences(paper)
            ref_uids = wos.utilities.getReferencesUIDs(referencesData)
            for ref in ref_uids:
                if ref in valid_uids:
                    publicationInfo = wos.utilities.getPublicationInfo(paper)
                    year = wos.utilities.getPublicationYear(publicationInfo)
                    year = int(year)
                    if ref in papers_by_year:
                        if year in papers_by_year[ref]:
                            papers_by_year[ref][year] += 1
                        else:
                            papers_by_year[ref][year] = 1
                    else:
                        papers_by_year[ref] = dict()
                        papers_by_year[ref][year] = 1
    return papers_by_year


def get_papers_year_union(valid_uids):
    files = glob.glob('citing_temp/citing*')
    print(files)
    
    pool = Pool(6)
    result = pool.map(partial(get_papers_year, valid_uids), files)
    
    print('join result...')
    join_result = result[0]
    for r in result[1:]:
        for paper, cit_by_year in r.items():
            for y,c in cit_by_year.items():
                if paper in join_result:
                    if y in join_result[paper]:
                        join_result[paper][y] += c
                    else:
                        join_result[paper][y] = c
                else:
                    join_result[paper] = dict()
                    join_result[paper][y] = c
    
    return join_result

def get_impact(chu_journal): # citations
    
    print('get_impact...')
    
    file = '%s_completedata_110423_bardo_infomap_110423_voting_schema_top10.xnet' % chu_journal
    net = xnet.xnet2igraph(file)

    papers_per_year = defaultdict(lambda:defaultdict(lambda:0))
    for vtx in net.vs:
        papers_per_year[vtx['cluster_top10']][vtx['year']] += 1
    
    valid_uids = net.vs['wos_id']
    valid_uids = set(valid_uids)
    
    dyear = 1
    
    papers_by_year = get_papers_year_union(valid_uids)
    bardo_dist = defaultdict(lambda: defaultdict(lambda: 0.0))

    print('cits per year...')
    for y in range(2010, 2021): # 2000
        
        for paper in net.vs.select(year_le=y-1, year_ge=y-2):
            bardo_comm = paper['cluster_top10']
            if paper['wos_id'] in papers_by_year:
                cits_by_year = papers_by_year[paper['wos_id']]
                if y in cits_by_year:
                    bardo_dist[bardo_comm][y] += cits_by_year[y]
    
    print(bardo_dist.keys())
    out_copy = dict()
    C_others = defaultdict(lambda:0)
    P_others = defaultdict(lambda:0)
    for comm, count_cit_per_year in bardo_dist.items():
        if comm[0] in ['A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L', 'M', 'N']:
            out_copy[comm] = dict()
#         print(comm, count_cit_per_year.keys() )
        for y, count_cit in count_cit_per_year.items():
            y1 = 0
            y2 = 0
            if y-1 in papers_per_year[comm]:
                y1 = papers_per_year[comm][y-1]
            if y-2 in papers_per_year[comm]:
                y2 = papers_per_year[comm][y-2]
            
            if comm[0] not in ['A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L', 'M', 'N']:
                C_others[y] += count_cit
                P_others[y] += (y1 + y2)
            else:
                if y1 + y2 > 0:
                    out_copy[comm][y] = count_cit/(y1 + y2)
                else:
                    out_copy[comm][y] = 0
    
    out_copy['Others'] = dict()
    for y, v in C_others.items():
        if P_others[y] == 0:
            out_copy['Others'][y] = 0
        else:
            out_copy['Others'][y] = v/P_others[y]

    json_str = json.dumps(out_copy)
    out = open('impact_factor_dist_%s_170423_top10.json' % chu_journal, 'w')
    out.write(json_str)
    out.close()


# files = ['ACS Appl. Mater. Interfaces', 'Adv. Funct. Mater.']
# for file in files[1:]:
#     get_impact(file)

def get_paper_count():
    paper_journal_year = defaultdict(lambda:defaultdict(lambda:0))
    # Reading the file sequentially
    with dbgz.DBGZReader(WOSArchivePath) as fd:
        # getting the number of entries
        print("\t Number of entries: ", fd.entriesCount)
        # getting the schema (currently UID and data)
        print("\t Scheme: ", fd.scheme)
        # TQDM can be used to print the progressbar
        for wosEntry in tqdm(fd.entries, total=fd.entriesCount):
            UID = wosEntry["UID"]
            entryData = wosEntry["data"] # XML records data
            sourceData = wos.utilities.getSources(wosEntry)
            title = wos.utilities.getSourceISOAbbreviation(sourceData) # Extracting the title
            title = title.lower()
            publicationInfo = wos.utilities.getPublicationInfo(wosEntry)
            year = wos.utilities.getPublicationYear(publicationInfo)
            paper_journal_year[title][year] += 1
            

    test = open('paper_journal_year_2020.txt','w')
    test.write(json.dumps(paper_journal_year)+"\n")
    test.close()
    
# get_impact('Adv. Funct. Mater.')