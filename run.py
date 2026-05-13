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

class Level():
    """ Represents a set of nodes at a given level of BFS. 
    Stores data about the nodes at a particular level of BFS, including level
    depth, the nodes contained within that level, parents of each node of this 
    level, children of each node of this level, and the number of shortest paths
    from the start node to each node of this level.
    
    On initialisation, the above information is stored in the form of dask 
    graphs in instance variables.

    """
    
    def __init__(self, depth: int, level_nodes: ddf.DataFrame, 
                 state: ddf.DataFrame, parent_level: Level, 
                 all_adj_df: ddf.DataFrame):
        """ Store dask graphs in self for later use """
        global CLIENT
        self.nodes, self.depth = level_nodes, depth
        self._discover_nodes(level_nodes, node_to_i, state)
        self.adj_df = self._get_self_adj_df(self.nodes, all_adj_df)
        self.children, self.pc_rels = self._get_pc_rels(self.adj_df, state, node_to_i)
        self.num_sps, self.cp_rels = self._num_sps(self.nodes, parent_level)
        CLIENT.cancel(self.adj_df)
        
    def __del__(self):
        """ Free memory held by persisted dask graphs before deleting """
        global CLIENT
        CLIENT.cancel([self.nodes, self.children, self.pc_rels, 
                       self.num_sps, self.cp_rels])
        
    def _discover_nodes(self, level_nodes: ddf.DataFrame, state: ddf.Dataframe):
        """ Mark level nodes as discovered (D) in state array """
        indices = node_to_i.loc[node_to_i.index.isin(level_nodes["node_id"])]
        
    
    def _get_self_adj_df(self, level_nodes: ddf.DataFrame, all_adj_df: ddf.DataFrame):
        """ Join level_nodes dataframe with all_adj_df on node_id column.
        
        Returns an adjacency dataframe where each row corresponds to a node in
        this level. 
        
        The returned dataframe has two columns - one contains node
        IDs, and the other contains a string representation of their neighbours.
        The latter can be made useful after turning the dataframe into a Bag
        then mapping eval on the string representations. Normally this would be
        a security risk but the program is intended to operate only on 
        connectome datasets, and the python code included in the string 
        representations is generated by the program itself and thus cannot 
        contain malicious commands. The benefit is increased scalability as 
        dask bags cannot handle extensive set membership type computations
        without bringing those bags into memory. 
        
        """
        merged = level_nodes.merge(all_adj_df, 
                                   on = "node_id", 
                                   how = "inner").persist()
        return merged
    
    def _get_pc_rels(self, adj_df: ddf.DataFrame, state: ddf.DataFrame):
        """ Return child nodes and parent-child relationships where the parents
        are the nodes belonging to this level. """
        
        return children_ids, pc_rels
    
    def _get_num_sps(self, level_nodes: ddf.DataFrame, parent_level: Level):
        """ Return the number of shortest paths to each node on this level, as
        well as the child-parent relationships discovered in the process where
        the children are the nodes belonging to this level. 
        
        The num_shortest_paths bag is of the general form
        db.from_sequence([(node1, num_paths), (node2, num_paths), ...])
        where num_paths is the number of shortest paths from start_node to node1 etc
        """
        pass # TODO
    
    def assign_credit(self, to_child_edge_scores=None):
        """ 
        Implements Rules 1&2 of MMDS Chapter 10 pg 365. Each node gets a credit 
        of 1 plus the sum of the credits of the DAG edges from that node to the 
        level below. A leaf node will only have a credit of 1. """
        pass # TODO
    
    def get_edge_scores(self):
        """ Return Bag of tuples Bag([((pre, post), edge_score), ...])
        for each edge between the nodes of this level and their parents.
        Edge scoring rules are detailed on page 365 of MMDS Chapter 10 """
        pass # TODO    


def pbfs(start_node: int, all_adj_df: ddf.DataFrame, state: ddf.DataFrame):
    """ Run a parallel breadth-first-search on the graph represented by 
    adjacency_bag, starting at start_node. Returns a list of Levels in order of
    depth and the state array.
    
    Each level of the search tree is processed one-at-a-time, as each level's
    data is dependent on the data in the level above it. Thus in the worst case,
    the time complexity is O(n) where n is the depth of the tree.
    
    Each level contains information about its nodes, including parent-child and
    child-parent relationships, as well as the number of shortest paths to each
    node from start_node. This information is stored in the form of persisted
    dask graphs.
    
    The original PBFS inspiration comes from the logic behind 'Bags'
    described at https://dl.acm.org/doi/epdf/10.1145/1810479.1810534. This is
    a simplified daskified implementation of their PBFS. What they term a 'bag'
    is here a 'Level', which represents all the nodes at some leve/depth d in the 
    BFS tree. The key idea I used from that publication is processing an entire
    level at a time, instead of iteratively processing each node within a level.
    The referenced publication was published in 2010, while Dask wasn't created
    until 2014.
    
    """
    # Set-up PBFS
    depth, level_nodes = 0, ddf.from_dict({"node_id": [start_node]})  
    levels = []    
    
    # Run PBFS, accumulating Levels
    parent_level = None    
    while True:
        new_level = Level(depth, level_nodes, state, parent_level, all_adj_df)
        levels.append(new_level)
        # TODO : mark level_nodes as processed
        level_nodes = new_level.children
        if level_nodes.count().compute() == 0:
            break
        # TODO : mark level nodes as discovered        
        depth += 1
        parent_level = levels[-1]
    
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

def create_state_df(all_adj_df: ddf.DataFrame, num_nodes: int) -> ddf.DataFrame:
    """ state dataframe uses node ids as indexes to track each index's state """
    # Create node to state bag for quick state lookups during PBFS.
    # This is a little hacky but I'm not sure how else to do this without extra
    # computes or bringing lots of data into memory lol.
    # This is done here instead of upstream to avoid cross-contamination with
    # other PBFSs starting at other start nodes.    
    lock = Lock()
    get_next = ("U" for _ in range(num_nodes)) # Use generator b/c list may be big
    def assign_state(node_id):
        with lock: # Ensure only one thread can call the generator at a time
            return (node_id, get_next)
    node_to_state_bag = all_adj_df.map(lambda adj: adj[0]).map(assign_state)
    state = node_to_state_bag.to_dataframe(
        meta = {"node_id": int, "state": object}).setindex(
            "node_id", sort = True).persist()    
    return state


def get_initial_edge_scores(start_node: int, all_adj_df: ddf.DataFrame, 
                            num_nodes: int) -> db.Bag:
    """ Run one PBFS then one PBFS backtrack then collate edge scores.
    Return Bag of Girvan Newman edge scores starting at start_node, of general 
    form Bag of tuples Bag([((pre, post), edge_score), ...]) """
    state = create_state_df(all_adj_df, num_nodes)
    levels, state = pbfs(start_node, all_adj_df, state, node_to_i)
    del state
    
    # PBFS backtrack to get edge scores
    scores = [] # List of Bags of edge scores
    levels = levels[::-1] # order levels from deepest at index 0 to root at end
    for i, level in enumerate(levels): 
        # Edge scores in a given level depend on the deeper level's edge scores
        level.assign_credit(scores[-1]) if scores else level.assign_credit()
        edge_score_list.append(level.get_edge_scores())
    
    # Create and return dask bag of edge scores of general form
    # Bag([((pre, post), edge_score), ...])
    score_bag = db.concat(scores)
    return score_bag


def get_edge_scores(component):
    """ Set up and do the edge-score calculation phase of Girvan-Newman on a 
    single component """
    global CLIENT
    # Get random subset of component nodes.
    # For now, using sample size = quarter the number of nodes in the df.
    component_nodes = get_all_nodes(component)
    num_nodes = component_nodes.count().compute()
    random_nodes = db.random.sample(component_nodes, int(num_nodes/4))
    
    # Put component bag into format suitable for set membership testing
    all_adj_df = component.to_dataframe(
        meta = {"node_id": int, "neighbours": object}).persist()  
    
    # Bag([((pre, post), edge_score), ...])
    all_edge_scores = random_nodes.map(
        lambda start_node: get_initial_edge_scores(
            start_node, all_adj_df, num_nodes)).flatten()
    
    # Remove persisted items
    CLIENT.cancel(all_adj_df)
    
    # TODO - make the below preserve edge identity (pre, post)
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

def modified_girvan_newman(component):
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
    edge_scores = get_edge_scores(component)
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
    """ Run modified grivan newman and prune components until no more iterations
    can be performed. """
    global MIN_CLUSTER_SIZE, CLIENT
    
    # Clean and filter
    if df: adjacency_bags = get_component_adjacency_bags(df)
    adjacency_bags = adjacency_bags.map(prune)
    adjacency_bags = adjacency_bags.filter(
        lambda adj_bag: adj_bag.count >= MIN_CLUSTER_SIZE)
    
    # Bag([(component1_bag, continue), (component2_bag, continue), ...])
    components = adjacency_bags.map(modified_girvan_newman).flatten()
    components = components.map(lambda component: recurse(component))
    
    # Filter and map components to include only bags
    clusters = components.filter(
        lambda component: component[0] is not None).map(
            lambda component: component[0])
    return clusters



### -----------------------------------------------------------------------

if __name__ == "__main__":
    CLIENT = Client()
