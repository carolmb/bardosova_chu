import sys
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

processedPath = Path("../../WoS/Data/Processed/")

WoSPaperCitationDataFilepath = processedPath/"WoSPaperCitationData.bgz"

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

def step0():
    
#     papers = pd.read_csv('paper_journals_cat_170122.csv', sep='\t', header=None)
#     papers.columns = ['journal', 'id', 'year', 'categories', 'refs']
#     print(set(papers['journal']))
#     valid_journals = set(papers['journal'])
    paper_count = defaultdict(lambda:defaultdict(lambda:0))
    c = 0
    with dbgz.DBGZReader(WoSPaperCitationDataFilepath) as citationDB:
        print("Scheme: ", citationDB.scheme)
        print("Number of Entries:", citationDB.entriesCount)
        pbar = tqdm(total=citationDB.entriesCount)
        while True:
            entries = citationDB.read(1000)
            if(not entries): 
                break
            for entry in entries:
                year = entry['Year']
                journal = entry['VenueName'].lower()

                if journal in valid_journals:
                    paper_count[journal][year] += 1
            pbar.update(len(entries))
#             c += 1
#             if c == 1000:
#                 break
    print(len(paper_count))
    out = open('papers_count_090822.txt', 'w')
    for j, count in sorted(paper_count.items()):
        out.write(j+'\n')
        for y,c in sorted(count.items()):
            out.write(str(y) + '\t' + str(c) + '\n')
        out.write('\n')
    out.close()
    pbar.refresh()
    pbar.close()

# step0()

# years_count = open('papers_count_090822.txt', 'r').read().split("\n\n")

# for journal in years_count:
#     print(journal)
#     lines = journal.split('\n')
#     journal_name = lines[0]
#     total = 0
#     for count in lines[1:]:
#         total += int(count.split('\t')[1])
#     print(journal_name, total)



def get_count_papers_plot():
    
    file = json.loads(open('paper_journal_year_2020.txt').read())
    for journal,cits in file.items():
        if '2020' in cits:
            print(journal, cits['2020'])
#         print(journal, str(sum(cits.values())))
        
get_count_papers_plot()