
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

# current_journal = 'Chem. Mater.'
current_journal1 = 'Adv. Funct. Mater.'.lower()
current_journal2 = 'ACS Appl. Mater. Interfaces'.lower()
items_journal = []

valid_journals = [
#     'acs nano',
#     'adv. mater.',
#     'chem. mat.', 
#     'j. am. chem. soc.', 
    'j. mat. chem. b', # correto
    'j. mater. chem. a',  # correto
#     'j. mater. chem. c', 
#     'langmuir',
#     'macromolecules',
#     'nano lett.',
    'nat. mater.', # correto
#     'nat. nanotechnol.'
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

    test = open('toy_chemb_chema_natmat.txt','w')
    for item in items_journal:
        test.write(json.dumps(item)+"\n")
    test.close()

get_papers_from_journal()