import dbgz, glob
from threading import Thread
from multiprocessing import Pool
from pathlib import Path
from tqdm.auto import tqdm
import xnetwork as xnet 

import WOSRaw as wos
from functools import partial

WOSIndexPath = Path("/mnt/e/WoS/WoS_2022_DBGZ/WoS_2022_All_byUID.idbgz")
print("Loading the index dictionary")

def single(valid_uids, wosEntry):
    referencesData = wos.utilities.getReferences(wosEntry)
    ref_uids = wos.utilities.getReferencesUIDs(referencesData)
    for current_uid in ref_uids:
        if current_uid in valid_uids:
            return wosEntry
    return None
            

def get_citing(chunk, valid_uids, outname):
    citing = []
    pool = Pool(12)
    result = pool.map(partial(single, valid_uids), chunk)
            
    test = open(outname,'w+')
    for item in result:
        if item != None:
            test.write(json.dumps(item)+"\n")
    test.close()
    
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
journalout = 'acs_nano'

WOSArchivePath = Path("/mnt/e/WoS/WoS_2022_DBGZ/WoS_2022_All.dbgz")

chunk = []
with dbgz.DBGZReader(WOSArchivePath) as fd:
    # getting the number of entries
    print("\t Number of entries: ", fd.entriesCount)
    # getting the schema (currently UID and data)
    print("\t Scheme: ", fd.scheme)
    # TQDM can be used to print the progressbar
    for wosEntry in tqdm(fd.entries, total=fd.entriesCount):
        chunk.append(wosEntry)
        
        if len(chunk) > 100000:
            thread = Thread(target = get_citing, args = [chunk, valid_uids, journalout+'toy3_test.json'])
            thread.start()
            
#             get_citing(chunk, valid_uids, journalout+'toy3_test.json')
            del chunk
            chunk = []
        
    get_citing(chunk, valid_uids, journalout+'toy3_test.json')
    