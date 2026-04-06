// CCFDataFrame.scala
// CCF algorithm implementation using Spark DataFrame (SQL) API.
//
// Mirrors ccf_dataframe.py exactly.
// Checkpointing at every iteration is mandatory to truncate the query plan
// lineage — without it, union/join/filter nesting causes OOM after ~5 iters.
// Convergence is tracked by counting type-2 rows (same signal as the RDD
// LongAccumulator), which is safe because pairs is always checkpointed first.

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

object CCFDataFrame {

  // CCF-Iterate

  /**
   * One execution of CCF-Iterate using the DataFrame API.
   *
   * Input schema : (src: Int, dst: Int)
   *
   * Steps:
   *   1. Reconstruct bidirectional adjacency lists via UNION
   *   2. Per-node minimum neighbor via groupBy + min()
   *   3. JOIN to attach min_neighbor to every (src, neighbor) row
   *   4. Emit type-1: (src → min_neighbor)      when min_neighbor < src
   *      Emit type-2: (neighbor → min_neighbor)  when min_neighbor < src
   *                                               AND neighbor ≠ min_neighbor
   *
   * @return (resultDf, newPairCount)
   *         resultDf      — new (src, dst) pairs before dedup
   *         newPairCount  — number of type-2 rows (convergence signal)
   */
  def ccfIterateDf(pairsDf: DataFrame): (DataFrame, Long) = {

    // Step 1 — bidirectional edges (reconstruct adjacency lists)
    val reversedDf = pairsDf.select(col("dst").alias("src"), col("src").alias("dst"))
    // SPARK MECHANIC: union() is a Narrow Transformation. It only appends partition 
    // metadata and does not require moving data across the cluster.
    val bidiDf     = pairsDf.union(reversedDf)

    // Step 2 — per-node minimum neighbor (the component label candidate)
    // SPARK MECHANIC: groupBy().agg() triggers a HashAggregate shuffle.
    // Unlike the RDD version, the Catalyst Optimizer handles min() using partial 
    // map-side aggregations. It does NOT materialize the full neighbor list in 
    // executor memory, completely avoiding the GC overhead seen in RDDs.
    val minDf = bidiDf.groupBy("src").agg(min("dst").alias("min_neighbor"))

    // Step 3 — join to attach min_neighbor to every (src, neighbor) row
    // SPARK MECHANIC: Catalyst will dynamically choose between a SortMergeJoin or 
    // a BroadcastHashJoin based on the size of minDf and spark.sql.autoBroadcastJoinThreshold.
    val joinedDf = bidiDf.join(minDf, "src")

    // Step 4a — type-1: node adopts minimum label
    // Only when min_neighbor < src (src is not already the minimum)
    val type1Df = joinedDf
      .filter(col("min_neighbor") < col("src"))
      .select(col("src"), col("min_neighbor").alias("dst"))
      .dropDuplicates(Seq("src"))

    // Step 4b — type-2: propagate minimum to all OTHER neighbors
    // Guard min_neighbor < src matches the RDD reduce guard exactly.
    // These are the cross-pairs equivalent to the LongAccumulator in CCFRdd.
    val type2Df = joinedDf
      .filter(col("min_neighbor") < col("src"))
      .filter(col("dst") =!= col("min_neighbor"))   // =!= is Spark's typed ≠
      .select(col("dst").alias("src"), col("min_neighbor").alias("dst"))

    // Count type-2 BEFORE union — this is the convergence signal.
    // Safe here because pairsDf was checkpointed, so the plan is shallow.
    // SPARK MECHANIC: ACTION
    // Calling .count() forces the execution of the Catalyst physical plan up to this 
    // point to extract the convergence signal, avoiding the need for an accumulator.
    val newPairCount = type2Df.count()

    val resultDf = type1Df.union(type2Df)
    (resultDf, newPairCount)
  }

  // CCF-Dedup

  /**
   * Removes duplicate (src, dst) pairs produced by CCF-Iterate.
   * Equivalent to using the pair itself as the MapReduce key.
   */
  def ccfDedupDf(pairsDf: DataFrame): DataFrame = {
    // SPARK MECHANIC: dropDuplicates relies on Tungsten's optimized hash-based 
    // grouping, efficiently reducing the data volume before the next iteration.
    pairsDf.dropDuplicates(Seq("src", "dst")) 
  }

  // Full CCF loop

  /**
   * Runs the full CCF algorithm using the DataFrame API.
   *
   * Calls checkpoint(eager=true) on every iteration to materialise the
   * DataFrame to disk and give Spark a flat query plan for the next iteration.
   * Requires sc.setCheckpointDir() to be configured before calling this.
   *
   * @param spark    SparkSession
   * @param edgesDf  DataFrame with columns (src: Int, dst: Int)
   * @param verbose  Print per-iteration diagnostics if true
   * @return (resultDf, numberOfIterations)
   */
  def runCCFDf(
      spark: SparkSession,
      edgesDf: DataFrame,
      verbose: Boolean = true
  ): (DataFrame, Int) = {

    var pairs     = edgesDf
    var iteration = 0
    var converged = false

    while (!converged) {
      iteration += 1

      // CCF-Iterate: propagate minimum labels 
      val (iterated, newPairCount) = ccfIterateDf(pairs)

      // CCF-Dedup: remove duplicate pairs 
      val deduped = ccfDedupDf(iterated)

      // CHECKPOINT: truncate query plan lineage.
      // Each iteration without checkpointing appends union/join/filter to
      // the plan tree → exponential growth → OOM after ~5 iterations.
      // checkpoint() writes to disk and replaces the plan with a flat read.
      // SPARK MECHANIC: LOGICAL PLAN TRUNCATION (CRITICAL)
      // Because DataFrames build a logical query tree (Lineage), a while-loop will 
      // infinitely nest union/join/filter operations. After ~5 iterations, Catalyst 
      // will throw a StackOverflowError or OOM attempting to serialize the massive DAG.
      // .checkpoint() forces materialization to HDFS/local disk, severing the lineage 
      // tree and allowing the next iteration to read from a flat parquet/internal format.
      pairs = deduped.checkpoint()
      val pairCount = pairs.count()

      if (verbose)
        println(f"  [DF]  Iter $iteration%3d | pairs=$pairCount%8d | new_cross_pairs=$newPairCount%8d")

      // Convergence: no type-2 pairs → all nodes already hold the minimum
      // ID of their component → no further propagation possible
      if (newPairCount == 0L) converged = true
    }

    (pairs, iteration)
  }
}
