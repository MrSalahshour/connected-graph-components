# ccf_rdd.py
# CCF algorithm implementation using Spark RDD API.
#
# Algorithm (from: Kardes et al., "CCF: Fast and Scalable Connected
# Component Computation in MapReduce"):
#   Iterates two MapReduce jobs until convergence:
#     - CCF-Iterate : propagates the minimum node ID across neighbors
#     - CCF-Dedup   : removes duplicate pairs to reduce I/O overhead

from pyspark import SparkContext


# CCF-Iterate — Map phase
def _ccf_iterate_map(edge):
    """
    For each pair (a, b), emits both (a, b) and (b, a).
    This makes edges bidirectional so each node sees all its neighbors
    in the subsequent groupByKey reduce step.

    SPARK MECHANIC: By emitting both (a, b) and (b, a), we prepare the RDD 
    for the groupByKey() shuffle phase. This ensures that when Spark partitions 
    the data across workers, every node key will group perfectly with its entire 
    adjacency list, regardless of the original edge direction.
    """
    a, b = edge
    yield (a, b)
    yield (b, a)


# CCF-Iterate — Reduce phase (factory capturing the accumulator)
def _make_reduce_fn(new_pair_acc):
    """
    Returns a reduce function that closes over the Spark accumulator.

    For each node `key` with adjacency list `neighbors`:
      1. Find min_val = min(neighbors)
      2. If min_val < key  → emit (key, min_val)          [key adopts min label]
      3. For every v != min_val → emit (v, min_val)        [propagate label]
         and increment the global NewPair counter.
    If min_val >= key, nothing is emitted (this node is already minimal).

    SPARK MECHANIC: This is a closure that captures the Spark Accumulator. 
    The accumulator object is serialized and sent to worker nodes (executors). 
    Workers can only .add() to it; only the Driver program can read its final value.
    """
    def reduce_fn(kv):
        key, vals = kv
        # vals is a PySpark ResultIterable. We must materialize it to a list 
        # in the executor's memory to find the minimum and iterate over it.
        neighbors = list(vals)
        min_val = min(neighbors)
    
        results = []
    
        # Only act when this node is NOT already the minimum in its neighborhood.
        # If min_val >= key, this node is already the representative → emit nothing.
        if min_val < key:
            # Type-1: this node adopts the smaller label
            results.append((key, min_val))
    
            # Type-2: propagate the smaller label to all OTHER neighbors
            # Each of these is a "new pair" that advances convergence
            for v in neighbors:
                if v != min_val:
                    # SPARK MECHANIC: Workers update the accumulator locally. 
                    # Spark aggregates these back to the driver safely.
                    new_pair_acc.add(1)
                    results.append((v, min_val))
    
        return results


    return reduce_fn


# CCF-Iterate — full job
def ccf_iterate_rdd(pairs_rdd, new_pair_acc):
    """
    One execution of the CCF-Iterate MapReduce job.

    Parameters:
        pairs_rdd    : RDD of (node, component_candidate) integer pairs
        new_pair_acc : Spark Accumulator tracking new cross-pairs emitted

    Returns:
        RDD of (node, new_component_candidate) pairs (before deduplication)
    """
    # Map: make all edges bidirectional to reconstruct adjacency lists
    mapped = pairs_rdd.flatMap(_ccf_iterate_map)

    # Reduce: group neighbors per node, apply label propagation logic
    # SPARK MECHANIC: groupByKey() triggers a massive network shuffle, moving data
    # across the cluster so all values for a single key end up on the same partition.
    # While reduceByKey() is generally more optimized, CCF requires the full adjacency 
    # list to generate cross-pairs, making groupByKey() necessary.
    grouped = mapped.groupByKey()
    return grouped.flatMap(_make_reduce_fn(new_pair_acc))


# CCF-Dedup — full job
def ccf_dedup_rdd(pairs_rdd):
    """
    One execution of the CCF-Dedup MapReduce job.

    CCF-Iterate can emit the same (node, component) pair multiple times
    (from different nodes' reduce steps). This job removes duplicates,
    reducing I/O and memory pressure for the next iteration.
    """
    # SPARK MECHANIC: distinct() triggers another wide transformation (shuffle)
    # under the hood (map -> reduceByKey). This deduplication is critical to prevent 
    # an exponential explosion of data partitions in the next CCF-Iterate step.
    return pairs_rdd.distinct()


# Full CCF loop
def run_ccf_rdd(sc, edges_rdd, verbose=True):
    """
    Runs the full CCF algorithm on the given edge list using the RDD API.

    Iterates CCF-Iterate → CCF-Dedup until the NewPair counter reaches 0,
    signalling that all connected components have been found.

    Parameters:
        sc        : SparkContext
        edges_rdd : RDD of (u, v) undirected edge tuples (integers)
        verbose   : if True, prints per-iteration diagnostics

    Returns:
        result_rdd  : RDD of (node, component_id) pairs.
                      Nodes absent from this RDD are their own component minimum.
        n_iters     : number of iterations until convergence
    """
    pairs = edges_rdd
    iteration = 0

    while True:
        iteration += 1

        # Fresh accumulator for this iteration's NewPair counter
        new_pair_acc = sc.accumulator(0)

        # CCF-Iterate
        iterated = ccf_iterate_rdd(pairs, new_pair_acc)

        # CCF-Dedup
        pairs = ccf_dedup_rdd(iterated)

        # Cache to avoid recomputation; count() triggers lazy evaluation
        # and flushes the accumulator to the driver.
        # SPARK MECHANIC: LINEAGE AND LAZY EVALUATION (CRITICAL)
        # We use .cache() to store the RDD in memory, breaking the recomputation chain.
        # We use .count() as an ACTION. Because Spark is lazy, the map/reduce 
        # transformations above haven't actually run yet. .count() forces the DAG 
        # execution, materializing the RDD and flushing the accumulator to the driver.
        pairs.cache()
        pair_count = pairs.count()

        if verbose:
            print(f"  [RDD] Iter {iteration:>2d} | pairs={pair_count:>8,} "
                  f"| new_cross_pairs={new_pair_acc.value:>8,}")

        # If accumulator is 0, convergence is reached.
        if new_pair_acc.value == 0:
            break

    return pairs, iteration
