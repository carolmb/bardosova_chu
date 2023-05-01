import dbgz, json, glob

import numpy as np
import pandas as pd
import xnetwork as xnet
import matplotlib.pyplot as plt

from pathlib import Path
from tqdm.auto import tqdm

from collections import defaultdict

# Path to the existing dbgz file
WOSArchivePath = Path("/mnt/e/WoS/WoS_2022_DBGZ/WoS_2022_All.dbgz")


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
    


def get_count_papers_plot():
    
    file = json.loads(open('paper_journal_year_2020.txt').read())
    for journal,cits in file.items():
        if '2020' in cits:
            print(journal, cits['2020'])
#         print(journal, str(sum(cits.values())))


# get_paper_count()
# get_count_papers_plot()


def wos_papers_full_data():
    files = glob.glob('citing_temp/citing_valid*.csv')
    all_papers = []
    for file in files:
        df = pd.read_csv(file, header=None, sep='\t')
        papers = pd.concat([df[0],df[2]])
        all_papers.append(papers)
        
    all_papers = pd.concat(all_papers).unique()
    print(len(all_papers))
    outfile = open('data_all_papers.csv', 'w')
    for row in all_papers:
        outfile.write(row+'\n')
    outfile.close()
        
wos_papers_full_data()