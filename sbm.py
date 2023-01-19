import graph_tool as gt
import graph_tool.inference as gtInference
from graph_tool.inference.planted_partition import PPBlockState
from graph_tool.inference.blockmodel import BlockState
from tqdm.auto import tqdm

# (o input é um grafo do igraph dentro de uma lista)
def SBMMinimizeMembership(graphs, assortative=False, layerWeights=None, weightMode="real-normal"):
    layered = (len(graphs) > 1)
    graph = graphs[0]
    vertexCount = graph.vcount()

    g = gt.Graph(directed=graph.is_directed())
    for _ in range(vertexCount):
        g.add_vertex()

    hasWeights = "weight" in graph.edge_attributes()
    
    if(assortative):
        weighted = False
    else:
        weighted = hasWeights
        
    if(weighted):
        weightsProperty = g.new_edge_property("double")

    if(layered):
        layerProperty = g.new_edge_property("int32_t")

    for graphIndex, graph in enumerate(graphs):
        if(weighted):
            weightMultiplier = 1
            if(layered and layerWeights is not None):
                weightMultiplier = layerWeights[graphIndex]
        for edge in graph.es:
            if(assortative and hasWeights):
                weight = edge["weight"]
                if(weight<0):
                    continue
            gedge = g.add_edge(edge.source, edge.target)
            if(weighted):
                weight = weightMultiplier*edge["weight"]
                weightsProperty[gedge] = weight
            if(layered):
                layerProperty[gedge] = graphIndex
    if(weighted):
        g.edge_properties["weight"] = weightsProperty
    if(layered):
        g.edge_properties["layer"] = layerProperty

    state_args = {}
    if(weighted):
        state_args["recs"] = [g.ep.weight]
        state_args["rec_types"] = [weightMode]

    if(layered):
        state_args["ec"] = g.ep.layer
        state_args["layers"] = True
        
    # print(state_args)
    state = BlockState
    if(assortative):
        state = PPBlockState
    state = gtInference.minimize.minimize_blockmodel_dl(
        g, state=state, state_args=state_args)
    return list(state.get_blocks())


def SBMMinimizeMembershipNested(graphs, layerWeights=None, weightMode="real-normal"):
    layered = (len(graphs) > 1)
    graph = graphs[0]
    vertexCount = graph.vcount()

    g = gt.Graph(directed=graph.is_directed())
    for _ in range(vertexCount):
        g.add_vertex()

    weighted = "weight" in graph.edge_attributes()
    if(weighted):
        weightsProperty = g.new_edge_property("double")

    if(layered):
        layerProperty = g.new_edge_property("int32_t")

    for graphIndex, graph in enumerate(graphs):
        if(weighted):
            weightMultiplier = 1
            if(layered and layerWeights is not None):
                weightMultiplier = layerWeights[graphIndex]
        for edge in graph.es:
            gedge = g.add_edge(edge.source, edge.target)
            if(weighted):
                weight = weightMultiplier*edge["weight"]
                weightsProperty[gedge] = weight
            if(layered):
                layerProperty[gedge] = graphIndex
    if(weighted):
        g.edge_properties["weight"] = weightsProperty
    if(layered):
        g.edge_properties["layer"] = layerProperty

    state_args = {}
    if(weighted):
        state_args["recs"] = [g.ep.weight]
        state_args["rec_types"] = [weightMode]

    if(layered):
        state_args["ec"] = g.ep.layer
        state_args["layers"] = True
    state_args["deg_corr"] = True
    # print(state_args)
    state = gtInference.minimize.minimize_nested_blockmodel_dl(
        g, state_args=state_args)
    levelMembership = []
    levels = state.get_levels()
    lastIndices = list(range(vertexCount))
    for level, s in enumerate(levels):
        blocks = s.get_blocks()
        lastIndices = [blocks[gindex] for gindex in lastIndices]
        if(len(set(lastIndices)) == 1 and level != 0):
            break
        levelMembership.append([str(entry) for entry in lastIndices])
    levelMembership.reverse()
    return levelMembership