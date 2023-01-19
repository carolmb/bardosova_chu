import json
import xnetwork as xnet
import igraph as ig

def select_title(article):
    titles = article['data']['static_data']['summary']['titles']['title']
    for title in titles:
        if title['@type'] == 'item':
            return title['#text'].lower()
    return ''
    
def select_year(article):
    pass

keyword = 'chitosan'  
# filename = 'json_%s_papers.json' % keyword
# file = open(filename, 'r').read()

# keyword_papers = json.loads(file)
# N = len(keyword_papers)

# nodes = []
# edges = []
# titles = []
# years = []
# i = 1
# print()
# for paper in keyword_papers:
#     print(i,'of', N, end='\r')
#     i+=1
#     nodes.append(paper['UID'])
#     titles.append(select_title(paper))
#     years.append(int(paper['data']['static_data']['summary']['pub_info']['@pubyear']))
#     refs = []
#     if 'reference' in paper['data']['static_data']['fullrecord_metadata']['references']:
#         for author in paper['data']['static_data']['fullrecord_metadata']['references']['reference']:
#             if 'uid' in author and type(author) != str:
#                 edges.append((paper['UID'],author['uid']))

# print()
# node_set = set(nodes)
# selected_edges = []
# for edge in edges:
#     if edge[0] in node_set and edge[1] in node_set:
#         selected_edges.append(edge)
        
# g = ig.Graph(directed=True)
# g.add_vertices(len(nodes))
# g.vs['name'] = nodes
# g.vs['title'] = titles
# g.vs['year'] = years
# print(set(years))
# g.add_edges(selected_edges)
        
# xnet.igraph2xnet(g, '%s_network_papers_citation.xnet' % keyword)

g = xnet.xnet2igraph('%s_network_papers_citation.xnet' % keyword)

def select_nodes(g, year, mode='lt'):
    if mode == 'lt':
        nodes2del = g.vs.select(year_lt=year)
    elif mode == 'gt':
        nodes2del = g.vs.select(year_gt=year)
        
    g.delete_vertices(nodes2del)

select_nodes(g, 2012, 'lt')
select_nodes(g, 2022, 'gt')

xnet.igraph2xnet(g, '%s_network_papers_citation_2012_2022.xnet' % keyword)



