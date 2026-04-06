// CCFRdd.scala
// CCF algorithm implementation using Spark RDD API.
//
// Mirrors ccf_rdd.py exactly:
//   - CCF-Iterate: flatMap (bidirectional) → groupByKey → flatMap (reduce logic)
//   - CCF-Dedup  : distinct()
//   - Convergence: LongAccumulator (NewPair counter), checked after count()

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.util.LongAccumulator
import scala.collection.mutable.ListBuffer

object CCFRdd {

  // CCF-Iterate

  /**
   * Map phase: for each edge (a, b) emit both (a,b) and (b,a) so every node
   * sees all its neighbors when grouped by key in the reduce phase.
   */
  private def ccfIterateMap(pair: (Int, Int)): Seq[(Int, Int)] = {
    // SPARK MECHANIC: FlatMapping to bidirectional edges is a Narrow Transformation.
    // We emit both directions so that the subsequent groupByKey() shuffle guarantees 
    // all neighbors for a single node are routed to the exact same partition.
    val (a, b) = pair
    Seq((a, b), (b, a))
  }

  /**
   * Reduce phase: for each (key, neighbors):
   *   1. Find minVal = min(neighbors)
   *   2. If minVal < key  → emit (key, minVal)         [type-1: adopt label]
   *      For each v ≠ minVal → emit (v, minVal)         [type-2: propagate]
   *                         + increment NewPair counter
   *   3. If minVal ≥ key  → emit nothing (node is already the minimum)
   */
  private def ccfIterateReduce(
      key: Int,
      neighborsIter: Iterable[Int],
      newPairAcc: LongAccumulator
  ): Seq[(Int, Int)] = {
    // SPARK MECHANIC: Materializing the Iterable to a List happens entirely within 
    // the Executor's JVM heap. For extremely dense nodes (super-nodes), this could 
    // cause GC (Garbage Collection) pressure, which is a known limitation of the RDD API.
    val neighbors = neighborsIter.toList
    val minVal    = neighbors.min
    val results   = ListBuffer[(Int, Int)]()

    if (minVal < key) {
      // Type-1: this node adopts the smaller label
      results += ((key, minVal))

      // Type-2: propagate smaller label to all other neighbors
      neighbors.foreach { v =>
        if (v != minVal) {
          // SPARK MECHANIC: The LongAccumulator is serialized and shipped to executors.
          // Executors perform thread-safe local updates (.add). The driver aggregates 
          // these partial sums only when an Action forces DAG execution.
          newPairAcc.add(1L)
          results += ((v, minVal))
        }
      }
    }
    // If minVal >= key: this node is already the group minimum → nothing emitted

    results.toSeq
  }

  /**
   * One full CCF-Iterate MapReduce job.
   *
   * @param pairsRdd   RDD of (node, componentCandidate) pairs
   * @param newPairAcc Spark LongAccumulator tracking cross-pairs emitted
   * @return RDD of new (node, componentCandidate) pairs before deduplication
   */
  def ccfIterateRdd(pairsRdd: RDD[(Int, Int)], newPairAcc: LongAccumulator): RDD[(Int, Int)] = {
    pairsRdd
      .flatMap(ccfIterateMap)                                        // Map phase
      // SPARK MECHANIC: groupByKey() triggers a Wide Transformation (Network Shuffle).
      // Data is serialized across the cluster. We must use groupByKey over reduceByKey 
      // because CCF requires examining the *entire* adjacency list to emit Type-2 cross-pairs.
      .groupByKey()                                                  // Shuffle
      .flatMap { case (key, vals) =>
        ccfIterateReduce(key, vals, newPairAcc)                      // Reduce phase
      }
  }

  // CCF-Dedup

  /**
   * Removes duplicate (node, componentID) pairs emitted by CCF-Iterate.
   * Reduces I/O and memory pressure for the next iteration.
   */
  def ccfDedupRdd(pairsRdd: RDD[(Int, Int)]): RDD[(Int, Int)] = {
    // SPARK MECHANIC: distinct() is another Wide Transformation (under the hood it is 
    // map(x => (x, null)).reduceByKey((x, y) => x).map(_._1)). Deduplication reduces 
    // shuffle I/O volume for the next iteration.
      pairsRdd.distinct()
  }

  // Full CCF loop

  /**
   * Runs the full CCF algorithm using the RDD API.
   *
   * Iterates CCF-Iterate → CCF-Dedup until the NewPair counter reaches 0,
   * meaning no node has a neighbor with a smaller ID than its current label.
   *
   * The LongAccumulator is reset at each iteration via a fresh instance.
   * count() is called after caching to flush the accumulator to the driver
   * before reading its value — this is the correct Spark accumulator pattern.
   *
   * @param sc        SparkContext
   * @param edgesRdd  RDD of (u, v) undirected edge pairs
   * @param verbose   Print per-iteration diagnostics if true
   * @return (resultRdd, numberOfIterations)
   */
  def runCCFRdd(
      sc: SparkContext,
      edgesRdd: RDD[(Int, Int)],
      verbose: Boolean = true
  ): (RDD[(Int, Int)], Int) = {

    var pairs     = edgesRdd
    var iteration = 0
    var converged = false

    while (!converged) {
      iteration += 1

      // Fresh accumulator per iteration — avoids accumulating across iterations
      val newPairAcc = sc.longAccumulator(s"NewPairCounter_iter$iteration")

      // CCF-Iterate 
      val iterated = ccfIterateRdd(pairs, newPairAcc)

      // CCF-Dedup
      pairs = ccfDedupRdd(iterated)

      // Cache then count() — forces Spark to execute the DAG and flush
      // the accumulator value back to the driver before we read it.
      // SPARK MECHANIC: LAZY EVALUATION AND LINEAGE (CRITICAL)
      // .cache() stores the computed RDD in executor memory, breaking the recomputation 
      // of the lineage tree on subsequent loops.
      // .count() is the ACTION that forces the Spark Engine to physically execute the 
      // MapReduce DAG, flushing the accumulator values safely to the driver.
      pairs.cache()
      val pairCount = pairs.count()

      if (verbose)
        println(f"  [RDD] Iter $iteration%3d | pairs=$pairCount%8d | new_cross_pairs=${newPairAcc.value}%8d")

      // Convergence: no type-2 pairs were generated this iteration
      if (newPairAcc.value == 0L) converged = true
    }

    (pairs, iteration)
  }
}
