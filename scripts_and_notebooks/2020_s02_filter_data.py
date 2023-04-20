
import glob
import dbgz, json
import numpy as np
import pandas as pd

import WOSRaw as wos
import xnetwork as xnet
from pathlib import Path
from tqdm.auto import tqdm
from collections import defaultdict

import matplotlib
import matplotlib.pyplot as plt
from collections import defaultdict
from matplotlib.ticker import MaxNLocator

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
            
            
# max_save = 1000000
# papers = open('valid_ids.txt')
# paper_uid = dict()
# for line in papers:
#     item = line[:-1].split("\t")
#     paper_uid[item[0]] = (item[1], int(item[2][:-2]), item[3])
    
# valid_uids = set(paper_uid.keys())
# print(len(valid_uids))


# get_citing(valid_uids, paper_uid)


def get_impact_factor():
    files = glob.glob('citing_temp/citing_valid*.csv')
    all_info = []
    for file in files:
        print(file)    
        df = pd.read_csv(file, sep='\t', header=None)
        df.columns = ['citing', 'year', 'cited', 'cited_journal', 'cited_comm']
        summary = df.groupby(['cited_journal','cited_comm','year']).count()
        all_info.append(summary)
    
    print("citing data...")
    all_info = pd.concat(all_info)
    all_info_cits = all_info.groupby(['cited_journal','cited_comm','year']).sum()
    
    # get numer of papers per year
    print("papers count...")
    valid_ids = pd.read_csv('valid_ids.txt', sep='\t')
    valid_ids.columns = ['wos_id', 'comm', 'year', 'journal']
    valid_ids_grouped = valid_ids.groupby(['comm', 'year', 'journal'], as_index=False).count()
    print(valid_ids_grouped.columns)
    
    data = defaultdict(lambda:defaultdict(lambda:[]))
    
    print('calculating impact factor...')
    for key,cit_count in all_info_cits.iterrows():
        journal, comm, year = key
        
        if year < 1990 or year > 2020:
            continue
        
        papers1 = valid_ids_grouped.loc[(valid_ids_grouped['journal'] == journal)
                                       & (valid_ids_grouped['comm'] == comm)
                                       & (valid_ids_grouped['year'] == year-1)]
        papers2 = valid_ids_grouped.loc[(valid_ids_grouped['journal'] == journal)
                               & (valid_ids_grouped['comm'] == comm)
                               & (valid_ids_grouped['year'] == year-2)]
        
        if len(papers1) > 0:
            papers_count = papers1['wos_id'].iloc[0]
        else:
            papers_count = 0
        
        if len(papers2) > 0:
            papers_count += papers2['wos_id'].iloc[0]
        
        cit_count = cit_count.iloc[0]
        impact_factor = cit_count/papers_count
        
        data[journal][comm].append((year, impact_factor))

    output = open('impact_factor_2020.json', 'w')
    output.write(json.dumps(data))
    output.close()
        
# get_impact_factor()

def plot_impact_factor():
    cmap = plt.get_cmap("tab10")
    outer_colors = cmap(np.arange(10))
    outer_colors[-1][0] = 0.73
    outer_colors[-1][1] = 0.73
    outer_colors[-1][2] = 0.73
    outer_colors[-1][3] = 1.0
    
    print(outer_colors)
    matplotlib.rcParams['pdf.fonttype'] = 42
    matplotlib.rcParams['ps.fonttype'] = 42
    matplotlib.rc('xtick', labelsize=14) 
    matplotlib.rc('ytick', labelsize=14) 
    
    data = json.loads(open('impact_factor_2020.json').read())
    for journal, comms_data in data.items():
        fg = plt.figure(figsize=(15, 5))
        ax = fg.gca()
        ax.xaxis.set_major_locator(MaxNLocator(integer=True))
        i = 0
        for comm, year_data in comms_data.items():
            X = []
            Y = []
            for x,y in year_data:
                X.append(x)
                Y.append(y)
            label = comm[:60] + '\n' + comm[60:120] + '\n' + comm[120:]
            plt.plot(X, Y, label=label, color=outer_colors[i])
            i += 1
        plt.title(journal)
        plt.legend(prop={'size': 8}, bbox_to_anchor=(1.05,1.05))
        plt.tight_layout()
        plt.savefig('{}_impact_factor_2020.pdf'.format(journal))
        
        
plot_impact_factor()