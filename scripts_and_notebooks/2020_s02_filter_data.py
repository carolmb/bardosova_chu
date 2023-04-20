
import glob
import dbgz, json
import pandas as pd
import WOSRaw as wos
import xnetwork as xnet
from pathlib import Path
from tqdm.auto import tqdm

def get_valid_papers_info():
    nets = glob.glob('../results/review2/nets180423/*voting_schema_top10.xnet')

    print(len(nets))
    # print(nets)

    papers = []

    for netname in nets:
        net = xnet.xnet2igraph(netname)

        journal = netname[len('../results/review2/nets180423/'):-len('_completedata_170423_allbut_bardo_infomap_180423_voting_schema_top10.xnet')]
        print(journal)
        for vtx in net.vs:
            item = (vtx['wos_id'], vtx['cluster_top10'], vtx['year'], journal)
            papers.append(item)


    output = open('valid_ids.txt','w')
    for wosid in papers:
        output.write("{}\t{}\t{}\t{}\n".format(wosid[0],wosid[1],wosid[2],wosid[3]))
    output.close()


def save_file(citing, filename):
    output = open(filename, 'w')
    for line in citing:
        output.write('{}\t{}\t{}\t{}\t{}\n'.format(*line))
    output.close()
    
    
def get_citing(valid_uids, paper_uid):
    WOSArchivePath = Path("/mnt/e/WoS/WoS_2022_DBGZ/WoS_2022_All.dbgz")
    count_files = 0   
    
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
            
            citing_id = wosEntry["UID"]
            publicationInfo = wos.utilities.getPublicationInfo(wosEntry)
            year = int(wos.utilities.getPublicationYear(publicationInfo))
            
            for current_uid in ref_uids:
                if current_uid in valid_uids:
                    cited_data = paper_uid[current_uid]
                    cited_publ_year = cited_data[1]
                    cited_journal = cited_data[-1]
                    cited_comm = cited_data[0]
                    if 1 <= year - cited_publ_year <= 2: 
                        citing.append((citing_id, year, current_uid, cited_journal, cited_comm))
                    
            if len(citing) > max_save:
                save_file(citing, 'citing_temp/citing_valid_{}.csv'.format(count_files))
                count_files +=1
                citing = []
                
                
        if len(citing) > 0:
            save_file(citing, 'citing_temp/citing_valid_{}.csv'.format(count_files))
            
            
max_save = 1000000
papers = open('valid_ids.txt')
paper_uid = dict()
for line in papers:
    item = line[:-1].split("\t")
    paper_uid[item[0]] = (item[1], int(item[2][:-2]), item[3])
    
valid_uids = set(paper_uid.keys())
print(len(valid_uids))


get_citing(valid_uids, paper_uid)