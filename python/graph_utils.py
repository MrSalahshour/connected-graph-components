# graph_utils.py
# Utilities for graph generation and correctness verification.

import random
import networkx as nx


def generate_random_graph(n_nodes, n_edges, seed=42):
    """
    Generates a random undirected graph with n_nodes nodes and n_edges edges.
    Self-loops are excluded. Duplicate edges are deduplicated.

    Returns:
        List of (u, v) integer tuples with u < v.
    """
    rng = random.Random(seed)
    edges = set()
    attempts = 0
    max_attempts = n_edges * 20
    while len(edges) < n_edges and attempts < max_attempts:
        u = rng.randint(0, n_nodes - 1)
        v = rng.randint(0, n_nodes - 1)
        if u != v:
            edges.add((min(u, v), max(u, v)))
        attempts += 1
    return list(edges)


def generate_chain_graph(n_nodes):
    """
    Generates a chain graph: 0 - 1 - 2 - ... - (n_nodes-1).
    This is the WORST CASE for CCF: diameter = n_nodes - 1,
    requiring d+1 iterations to converge.
    """
    return [(i, i + 1) for i in range(n_nodes - 1)]


def generate_star_graph(n_nodes):
    """
    Generates a star graph with node 0 as the center.
    Best case for CCF: converges in 1 iteration.
    """
    return [(0, i) for i in range(1, n_nodes)]


def generate_clustered_graph(n_clusters, nodes_per_cluster, seed=42):
    """
    Generates a graph with n_clusters disjoint clusters.
    Each cluster is a random spanning tree (connected, minimal edges).
    Useful for verifying CCF finds the correct number of components.

    Returns:
        List of (u, v) edges. Expected number of components = n_clusters.
    """
    rng = random.Random(seed)
    edges = []
    for c in range(n_clusters):
        base = c * nodes_per_cluster
        # Random spanning tree via sequential attachment
        for i in range(1, nodes_per_cluster):
            parent = rng.randint(0, i - 1)
            u, v = base + parent, base + i
            edges.append((min(u, v), max(u, v)))
    return list(set(edges))


def verify_with_networkx(edges, ccf_pairs):
    """
    Verifies CCF output against NetworkX's reference implementation.

    Parameters:
        edges     : list of (u, v) edge tuples (input graph)
        ccf_pairs : list of (node, component_id) tuples from CCF output

    Returns:
        True if the component assignment matches NetworkX, False otherwise.
    """
    # Build NetworkX graph and find reference connected components
    G = nx.Graph()
    G.add_edges_from(edges)
    nx_components = nx.connected_components(G)

    # Map each node to its component label (min node in component)
    nx_label = {}
    for comp in nx_components:
        label = min(comp)
        for node in comp:
            nx_label[node] = label

    # Build CCF label map; nodes absent from ccf_pairs are their own minimum
    ccf_label = {}
    for node, comp_id in ccf_pairs:
        ccf_label[node] = comp_id

    # All nodes in the graph
    all_nodes = set(G.nodes())
    for node in all_nodes:
        if node not in ccf_label:
            ccf_label[node] = node  # minimum nodes map to themselves

    # Compare: CCF labels must partition identically to NetworkX labels
    # (labels may differ in name but must agree on grouping)
    ccf_groups = {}
    for node, label in ccf_label.items():
        ccf_groups.setdefault(label, set()).add(node)

    nx_groups = {}
    for node, label in nx_label.items():
        nx_groups.setdefault(label, set()).add(node)

    # Both must produce the same set of node groups
    ccf_partition = sorted([sorted(g) for g in ccf_groups.values()])
    nx_partition  = sorted([sorted(g) for g in nx_groups.values()])

    return ccf_partition == nx_partition
