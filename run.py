""" Computing Excitatory-Inhibitory Neurotransmitter Ratios of Discrete 
Subnetworks of the Drosophila Connectome 

Program Author: Ciel Baumann


--- DATA

Dataset: FlyWire Whole-brain Connectome Connectivity Data
Dataset Retrieved From: https://zenodo.org/records/10676866
Dataset Version: 783.0
Dataset Published By: Flywire Consortium

Data Files used:
- proofread_connections_783.feather
- proofread_root_ids_783.npy

Dataset Citation (APA):
FlyWire Consortium. (2024). FlyWire Whole-brain Connectome Connectivity Data 
  (783.0) [Data set]. Zenodo. https://doi.org/10.5281/zenodo.10676866
  
  
--- USAGE

TODO



--- CONTENTS

TODO


"""

import dask
import numpy as np
import os
import random
from collections import Counter
from collections import defaultdict
from dask import bag as db
from dask import dataframe as ddf
from dask.distributed import Client

CLIENT = None # Assigned properly at bottom of script
MIN_CLUSTER_SIZE = 30
MAD_K = 3.5

ROOT_DIR = os.path.dirname(__file__)



### CLUSTER IDENTIFICATION - HELPER FUNCTIONS -----------------------------

def df_to_adjacency_bag(df, undirect=True):
    """ Convert dataframe edges into adjacency list/bag of edges with weights.
    
    Return an adjacency list for the dataframe as a dask bag of the form
        db.from_sequence([(a, [(b, 3)]), (b: [(a, 3)])])
    if undirected, otherwise
        db.from_sequence([(a, []), (b, [(a, 3)])])
    where the edge from b->a (or b->a and a->b in the undirected version) has
    weight 3.
    
    Weight represents the number of synpases between two neurons a and b.
    
    For the DATA301 project, only the undirected version is required, but I
    have included the option for directed should I choose to extend the 
    project (I have not tested with the directed version).
    
    """
    if undirect: # Add (b,a,w) for every (a,b,w)
        edge_bag = df[["pre", "post", "syn_count"]].to_bag()        
        edge_bag = edge_bag.map(lambda edge: [edge, (edge[1], edge[0], edge[2])])
        edge_bag = edge_bag.flatten().distinct()
        adj_df = edge_bag.to_dataframe(
            meta = {"pre": int, "post": int, "syn_count": int})
    else:
        adj_df = df
        
    # grouped is of the general form
    # pre    post      syn_count
    # 0      [1, 5]    [2, 4]
    # where there is an edge of weight 2 between 0 and 1,
    # an edge of weight 4 between 0 and 5, etc
    grouped = adj_df.groupby("pre").agg({"pre": list, "post": list, "syn_count": list})
    
    # Each entry of form ([pre, pre, ...], [post, post, ...], [syn_count, syn_count, ...])
    # where all the pre values within an entry are equal. Edges are currently
    # represented by indices.
    grouped_as_bag = grouped.to_bag()
    adjacency_bag = grouped_as_bag.map( # Possible memory issue here?
        lambda entry: (entry[0][0], list(zip(entry[1], entry[2]))))
    return adjacency_bag.persist()


def get_main_nodes(adjacency_bag):
    """ Return bag of all main/pre nodes in bag """
    main_nodes = adjacency_bag.map(lambda node_adj: node_adj[0])
    return main_nodes


def get_neighbour_nodes(adjacency_bag):
    """ Return bag of all nodes in a list in bag """
    nnodes = adjacency_bag.map(
        lambda node_adj: node_adj[1]).flatten().map(lambda tup: tup[0]).distinct()
    return nnodes


def get_all_nodes(adjacency_bag):
    """ Return bag of all nodes in graph represented by bag """
    main_nodes = get_main_nodes(adjacency_bag)
    nnodes = get_neighbour_nodes(adjacency_bag)
    nodes = db.concat([main_nodes, nnodes])
    return nodes


def get_num_nodes(adjacency_bag):
    """ Return the number of nodes in the graph represented by bag """
    return get_all_nodes(adjacency_bag).count().compute()


def log_removed_edges(removed_edges):
    """ Log removed edges in a temp file. These edges can be analysed to address
    the research question in a similar manner as the clusters. """
    pass # TODO




### PARALLEL BFS ----------------------------------------------------------

class Layer():
    pass


def pbfs(start_node, adjacency_bag, state=None):
    """"""
    # Set-up PBFS
    if not state:
        num_nodes = adjacency_bag.count().compute()
        state = np.full(len(num_nodes), "U", "<U1") # nodes i maps to state i
    all_adj_df = adjacency_bag.to_dataframe(
        meta = {"pre": int, "neighbours": object})
    depth, level_nodes = 0, ddf.from_dict({"node_id": [start_node]})    
    levels = []    
    
    # Run PBFS, accumulating Levels
    while True:
        new_level = Level(depth, level_nodes, state, node_to_i, all_adj_df)
        levels.append(new_level)
        # TODO : mark level_nodes as processed
        level_nodes = new_level.get_children()
        if level_nodes.count().compute() == 0:
            break
        # TODO : mark level nodes as discovered        
        depth += 1
    
    return (levels, state)


def get_component_adjacency_bags(df: ddf.DataFrame, undirected=True):
    """ Return a dask bag of adjacency lists/bags for each component in df.
   
    For each component in the graph represented in df, return the 
    adjacency list of the component containing each edge and their 
    weights. This is done by performing iteratively performing parallel BFS to 
    identify nodes belonging to different components. Only one component can
    be discovered at a time.
    
    Parent-child relationships require a start node, which is outside the
    scope of this function. See the bfs_search function.

    """
    big_adjacency_bag = df_to_adjacency_bag(df, undirected)
    nodes = np.array(big_adjacency_bag.map(
        lambda node_adjacency: node_adjacency[0]).compute())
    state = np.full(len(nodes), "U", "<U1") # nodes indices map to state indices
    components = [] # Will later be a dask bag of adjacency bags (one per component)
    
    # Iterate until all nodes are assigned to a component. Must find one
    # component at a time.
    for node_index in range(len(nodes)):
        if state[node_index] == "U":
            prev_state = state.copy()
            start_node = nodes[node_index]            
            
            all_child_parent_rels, state, leaves = pbfs(start_node, 
                                                        big_adjacency_bag,
                                                        state)
            del all_child_parent_rels, leaves
            
            # Add new component
            diff_indices = np.where(state != prev_state)[0]
            component_nodes = nodes[diff_indices]
            component_adj = big_adjacency_bag.filter(
                lambda node_adjacency: node_adjacency[0] in component_nodes)
            components = components + [component_adj.persist()]
    
    return db.from_sequence(components)




### CLUSTER IDENTIFICATION - PRUNE ----------------------------------------

def cut_deg1_edge(node_adj, should_cut: bool):
    """ Change node adjacency list to empty list if should cut. 
    This effectively makes the node a degree 0 node. """
    if should_cut:
        node_adj = (node_adj[0], [])
    return node_adj


def remove_deg_1_nodes(node_adj, deg1_nodes):
    """ Remove edges from node to nodes in degree 1 nodes """
    neighbours = list(filter(
        lambda tup: tup[0] not in deg1_nodes, node_adj[1]))
    return (node_adj[0], neighbours)


def prune(adjacency_bag: db.Bag) -> db.Bag:
    """ Iteratively remove degree 1 edges from a dask dataframe 
    
    A degree 1 edge is defined here as an edge associated with at least one
    degree 1 node, where a degree 1 node is a node connected by any number of 
    edges to one and only one other node. No nodes in the dataset will have 
    edges to themselves. Synapse count and directionality are not considered.
    
    Adjacency bag format:
            db.from_sequence([(a, [(b, 3)]), (b: [(a, 3)])])
            
    The naive approach would be to take away one edge at a time. This parallel
    version improves performance by finding all degree 1 edges initially, and
    pruning each 'chain' in parallel. The time to complete is the time it takes
    to process the longest 'chain'.

    """
    deg1_nodes = adjacency_bag.filter(
        lambda node_adj: len(node_adj[1]) == 1).map(
            lambda node_adj: node_adj[0]).compute()
    
    while True:
        if len(deg1_nodes) == 0:
            break
        
        # Remove edge from deg1 node to neighbour
        adjacency_bag = adjacency_bag.map(
            lambda node_adj: cut_deg1_edge(node_adj, node_adj[0] in deg1_nodes)).filter(
                lambda node_adj: len(node_adj[1]) > 0).persist()
        
        # Remove edge from neighbours to deg1 nodes
        adjacency_bag = adjacency_bag.map(
            lambda node_adj: remove_deg_1_nodes(node_adj, deg1_nodes)).persist()
    
    return adjacency_bag





### CLUSTER IDENTIFICATION - GIRVAN NEWMAN --------------------------------


def calculate_edge_scores(start_node, child_parent_rels, num_shortest_paths, 
                          leaves, component):
    """ Return Bag of tuples Bag([((pre, post), edge_score), ...])
    Edge scoring rules are detailed on page 365 of MMDS Chapter 10 
    
    The num_shortest_paths bag is of the general form
        db.from_sequence([(node1, num_paths), (node2, num_paths), ...])
    where num_paths is the number of shortest paths from start_node to node1 etc
    
    """
    num_sp = dict(num_shortest_paths.compute()) # Quick look-up
    to_score = leaves
    edge_scores = db.from_sequence([])
    all_child_parent_rels = dict(child_parent_rels.compute())
    
    def credit(node):
        """ Rule 1: leaves get credit = 1. Rule 2: other nodes get credit = 1 + 
        sum of credits of the DAG edges from that node to its children """
        credit = 1 + edge_scores.filter(
            lambda entry: entry[0][0] == node).map(
                lambda entry: entry[1]).sum().compute()
        return (node, credit)
    
    def process_nodes(node_w_credit):
        """ Rule 3: A DAG edge e entering node Z from the level above is given a
        share of the credit of Z proportional to the fraction of shortest
        paths from the root to Z that go through e.
        """
        node, credit = node_w_credit
        parents = db.from_sequence(all_child_parent_rels[node])
        total_num_shortest_paths_to_parents = parents.map(
            lambda parent: num_shortest_paths[parent[0]])
        node_edge_scores = parents.map(
            lambda parent: ((parent[0], node), 
                            credit * num_shortest_paths[parent[0]] / 
                            total_num_shortest_paths_to_parents))
        return node_edge_scores
        
    while to_score.count().compute() > 0:
        print(f"Nodes to score = {to_score.compute()}")
        nodes_w_credits = to_score.map(credit)
        new_edge_scores = nodes_w_credits.map(process_nodes).flatten()
        edge_scores = db.concat([edge_scores, new_edge_scores])
        to_score = nodes_w_credits.map( # to_score = parents
            lambda node_w_c: all_child_parent_rels[node_w_c[0]]).flatten()
        
    return edge_scores


def get_edge_scores(start_node, component):
    """ Return dict of Girvan Newman edge scores starting at start_node.
    df should only contain pre, post, and syn_count cols """
    print("\nRunning PBFS ...")
    child_parent_rels, state, leaves, num_shortest_paths = pbfs(start_node, component)
    print("Ran PBFS")
    print("\nCalculating edge scores ...")
    edge_scores = calculate_edge_scores(start_node, child_parent_rels, 
                                        num_shortest_paths, leaves, component)
    print("Calculated edge scores")
    return edge_scores


def girvan_newman(component):
    """ Set up and do the edge-score calculation phase of Girvan-Newman on a 
    single component """
    # Map random subset of nodes to get_edge_scores.
    # For now, using sample size = quarter the number of nodes in the df.
    print("Getting random node subset ...")
    component_nodes = get_all_nodes(component).compute()
    random_nodes = db.from_sequence(
        random.sample(component_nodes, int(len(component_nodes)/4)))
    print(f"Using nodes {random_nodes.compute()}")
    
    # Bag([((pre, post), edge_score), ...])
    print("Getting edge scores ...")
    all_edge_scores = random_nodes.map(
        lambda start_node: get_edge_scores(start_node, component)).flatten()
    print("Got edge scores")
    
    # Sum edge scores and divide by factor
    factor = 0.5 # Used sample size = quarter # nodes in df -> factor = 0.5
    scores = all_edge_scores.foldby(
        key = lambda edge_score: edge_score[0],
        binop = lambda accum, edge_score: accum + edge_score[1],
        initial = 0,
        combine = lambda accum1, accum2: accum1 + accum2,
        combine_initial = 0
    )
    standardised_scores = scores.map(
        lambda edge_score: (edge_score[0], edge_score[1]/factor))
    return standardised_scores




### CLUSTER IDENTIFICATION - DECOMPOSITION --------------------------------

def get_upper_threshold(edge_scores, k):
    """ Calculate MAD-based upper threshold.
    edge_scores of form Bag([((pre, post), edge_score), ...])
    """
    global MAD_K
    if not k:
        k = MAD_K
    scores = np.array(edge_scores.map(lambda tup: tup[1]).compute())
    median_score = np.median(scores)
    mad = np.median(np.absolute(scores - median_score))
    upper_threshold = median_score + k * mad
    return upper_threshold


def chop(component, edge_scores, upper_threshold):
    """ Remove outlier edges from component. 
    Return updated adjacency bag and bag of edges removed.
    """
    pass # TODO




### CLUSTER IDENTIFICATION - IDENTIFY CLUSTERS ----------------------------

def process_component(component):
    """ Returns a bag of components and whether processing should continue.
    Bag([(component1_bag, _continue), (component2_bag, _continue), ...])
    where component1, component2, ... are components derived from component.
        
    If _continue is true, then the caller function should apply another round
    of girvan newman on the components. continue = False occurs when no more
    edges are removed from the input component (i.e., the component is a 
    cluster).
    
    Takes a bag called component representing the edges of a component.
    """
    global MIN_CLUSTER_SIZE
    edge_scores = girvan_newman(component)
    upper_score_threshold = get_upper_threshold(edge_scores)
    new_adj_bag, removed_edges = chop(component, edge_scores, upper_score_threshold)
    log_removed_edges(removed_edges)
    if get_num_nodes(new_adj_bag).compute() < MIN_CLUSTER_SIZE:
        return (None, False) # Cluster/component too small
    if removed_edges.count().compute() == 0:
        return (new_bag, False) # Cluster found! Don't continue processing.    
    

def recurse(component):
    component_bag, _continue = component
    if not _continue:
        return (component_bag, False)
    return identify_clusters(adjacency_bags = component_bag)


def identify_clusters(df=None, adjacency_bags=None):
    global MIN_CLUSTER_SIZE, CLIENT
    
    # Clean and filter
    if df: adjacency_bags = get_component_adjacency_bags(df)
    adjacency_bags = adjacency_bags.map(prune)
    adjacency_bags = adjacency_bags.filter(
        lambda adj_bag: adj_bag.count >= MIN_CLUSTER_SIZE)
    
    # Bag([(component1_bag, continue), (component2_bag, continue), ...])
    components = adjacency_bags.map(process_component).flatten()
    components = components.map(lambda component: recurse(component))
    
    # Filter and map components to include only bags
    clusters = components.filter(
        lambda component: component[0] is not None).map(
            lambda component: component[0])
    return clusters



### -----------------------------------------------------------------------

if __name__ == "__main__":
    CLIENT = Client()
