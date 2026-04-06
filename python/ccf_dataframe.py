# ccf_dataframe.py
# CCF algorithm implementation using Spark DataFrame (SQL) API.
#
# Checkpointing is applied at every iteration to truncate the query plan
# lineage. Without it, iterative union/join/filter nesting causes OOM.
# Convergence is tracked via the count of type-2 (cross-propagation) pairs,
# which mirrors the NewPair accumulator in the RDD version.

from pyspark.sql import functions as F


def ccf_iterate_df(pairs_df):
    """
    One execution of CCF-Iterate using the DataFrame API.

    Input DataFrame columns: (src, dst)
      where each row means "node `src` currently points to component `dst`"

    Steps:
      1. Reconstruct bidirectional adjacency lists via UNION.
      2. Compute per-node minimum neighbor with groupBy + min().
      3. JOIN to attach min_neighbor to every (src, neighbor) row.
      4. Emit type-1 pairs: (src → min_neighbor)     where min_neighbor < src
         Emit type-2 pairs: (neighbor → min_neighbor) where min_neighbor < src
                                                       AND neighbor != min_neighbor

    Returns:
        result_df      : DataFrame of new (src, dst) pairs (before dedup)
        new_pair_count : number of type-2 (cross-propagation) rows — the
                         convergence signal equivalent to the RDD accumulator
    """
    # Step 1 — make edges bidirectional to reconstruct adjacency lists
    reversed_df = pairs_df.select(
        F.col("dst").alias("src"),
        F.col("src").alias("dst")
    )
    # SPARK MECHANIC: union() is a narrow transformation, meaning it does not 
    # require a network shuffle. It simply appends partitions.
    bidi_df = pairs_df.union(reversed_df)

    # Step 2 — per-node minimum neighbor (the component label candidate)
    # SPARK MECHANIC: groupBy().agg() triggers a full shuffle (HashAggregate).
    # Spark's Catalyst Optimizer is very efficient at calculating min() 
    # without materializing the entire list in memory, unlike the RDD version.
    min_df = bidi_df.groupBy("src").agg(
        F.min("dst").alias("min_neighbor")
    )

    # Step 3 — join to attach min_neighbor to every (src, neighbor) row
    # SPARK MECHANIC: This is a SortMergeJoin or BroadcastHashJoin depending 
    # on the DataFrame size. It attaches the minimum neighbor to the original rows.
    joined_df = bidi_df.join(min_df, on="src")

    # Step 4a — type-1: this node adopts the smaller label
    # Emitted only when min_neighbor < src (src is not already the minimum)
    type1_df = (
        joined_df
        .filter(F.col("min_neighbor") < F.col("src"))
        .select(F.col("src"), F.col("min_neighbor").alias("dst"))
        .dropDuplicates(["src"])
    )

    # Step 4b — type-2: propagate the minimum label to all OTHER neighbors
    # Guard min_neighbor < src is required: only non-minimum nodes propagate.
    # These are the "cross-pairs" that advance convergence (NewPair counter).
    type2_df = (
        joined_df
        .filter(F.col("min_neighbor") < F.col("src"))
        .filter(F.col("dst") != F.col("min_neighbor"))
        .select(F.col("dst").alias("src"), F.col("min_neighbor").alias("dst"))
    )

    # Count type-2 BEFORE union for the convergence signal.
    # This is safe because `pairs_df` is always checkpointed at this point,
    # so the plan behind joined_df / type2_df is shallow (3-4 ops deep).
    # SPARK MECHANIC: ACTION
    # count() forces Catalyst to execute the query plan up to this point.
    # This gives us our convergence signal, equivalent to the RDD accumulator.
    new_pair_count = type2_df.count()

    result_df = type1_df.union(type2_df)
    return result_df, new_pair_count


def ccf_dedup_df(pairs_df):
    """
    CCF-Dedup: removes duplicate (src, dst) pairs produced by CCF-Iterate.
    Equivalent to using the pair itself as the MapReduce key in the paper.
    """
    # SPARK MECHANIC: dropDuplicates is an aggregation that triggers a shuffle.
    # It minimizes the data footprint passed into the next iteration.
    return pairs_df.dropDuplicates(["src", "dst"])


def run_ccf_df(spark, edges_df, verbose=True):
    """
    Runs the full CCF algorithm using the DataFrame API.

    Checkpoints the DataFrame at every iteration to truncate query plan
    lineage. Without checkpointing, Spark nests union/join/filter plans
    indefinitely, causing OOM after ~5 iterations.

    Convergence is detected when type-2 pair count == 0, meaning no node
    has a neighbor with a smaller ID than its current label — all components
    have propagated their minimum ID to every member.

    Parameters:
        spark    : SparkSession
        edges_df : DataFrame with columns (src: int, dst: int)
        verbose  : if True, prints per-iteration diagnostics

    Returns:
        result_df : DataFrame of (src, dst) = (node, component_id) mapping
        n_iters   : number of iterations until convergence
    """
    pairs = edges_df
    iteration = 0

    while True:
        iteration += 1

        # CCF-Iterate: propagate minimum labels, get convergence signal
        iterated, new_pair_count = ccf_iterate_df(pairs)

        # CCF-Dedup: remove duplicate pairs to reduce next iteration's I/O 
        deduped = ccf_dedup_df(iterated)

        # CHECKPOINT: truncate query plan lineage.
        # Materialises the DataFrame to disk so the next iteration starts
        # from a flat read rather than a deeply nested logical plan.
        # Requires sc.setCheckpointDir() to be set before calling this.

        # SPARK MECHANIC: DAG TRUNCATION via CHECKPOINTING (CRITICAL)
        # In an iterative algorithm, Spark's logical query plan (Lineage DAG) grows 
        # exponentially with every loop. After ~5 iterations, the Catalyst optimizer 
        # will run out of JVM heap memory trying to resolve the massive nested plan.
        # checkpoint(eager=True) physically writes the DataFrame to disk (HDFS/local) 
        # and severs the lineage tree, starting the next iteration with a flat read.
        pairs = deduped.checkpoint(eager=True)

        # count() executes the plan and loads it into memory
        pair_count = pairs.count()

        if verbose:
            print(f"  [DF]  Iter {iteration:>2d} | pairs={pair_count:>8,} "
                  f"| new_cross_pairs={new_pair_count:>8,}")

        # Convergence: no type-2 pairs → no cross-propagation happened
        # → all nodes already hold the minimum ID of their component
        if new_pair_count == 0:
            break

    return pairs, iteration
