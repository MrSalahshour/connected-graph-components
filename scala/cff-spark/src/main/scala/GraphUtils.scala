// GraphUtils.scala
// Graph generation utilities and correctness verification via Union-Find.
// Mirrors graph_utils.py — replaces NetworkX with a local Union-Find since
// no equivalent JVM library is assumed on the cluster.

import scala.util.Random
import scala.collection.mutable

object GraphUtils {

  // Graph Generators

  /**
   * Generates a random undirected graph with nNodes nodes and nEdges edges.
   * Self-loops excluded; duplicate edges deduplicated.
   * Returns list of (u, v) with u < v.
   */
  def generateRandomGraph(nNodes: Int, nEdges: Int, seed: Long = 42L): Seq[(Int, Int)] = {
    val rng        = new Random(seed)
    val edges      = mutable.Set[(Int, Int)]()
    var attempts   = 0
    val maxAttempts = nEdges * 20
    while (edges.size < nEdges && attempts < maxAttempts) {
      val u = rng.nextInt(nNodes)
      val v = rng.nextInt(nNodes)
      if (u != v) edges.add((math.min(u, v), math.max(u, v)))
      attempts += 1
    }
    edges.toSeq
  }

  /**
   * Generates n_clusters disjoint components, each a random spanning tree of
   * nodesPerCluster nodes. Expected number of components = nClusters.
   */
  def generateClusteredGraph(nClusters: Int, nodesPerCluster: Int, seed: Long = 42L): Seq[(Int, Int)] = {
    val rng   = new Random(seed)
    val edges = mutable.Set[(Int, Int)]()
    for (c <- 0 until nClusters) {
      val base = c * nodesPerCluster
      for (i <- 1 until nodesPerCluster) {
        val parent = rng.nextInt(i)
        val u = base + parent
        val v = base + i
        edges.add((math.min(u, v), math.max(u, v)))
      }
    }
    edges.toSeq
  }

  // Reference Implementation: Union-Find (replaces NetworkX)

  /**
   * Computes connected components using path-compressed Union-Find.
   * Returns a map from each node to its component representative (minimum ID).
   */
  def referenceComponents(edges: Seq[(Int, Int)]): Map[Int, Int] = {
    val allNodes = edges.flatMap { case (u, v) => Seq(u, v) }.toSet
    val parent   = mutable.Map.from(allNodes.map(n => n -> n))

    // Path-compressed find
    def find(x: Int): Int = {
      if (parent(x) != x) parent(x) = find(parent(x))
      parent(x)
    }

    // Union by minimum ID so the representative is always the smallest node
    def union(x: Int, y: Int): Unit = {
      val px = find(x)
      val py = find(y)
      if (px != py) {
        if (px < py) parent(py) = px   // smaller ID becomes root
        else         parent(px) = py
      }
    }

    edges.foreach { case (u, v) => union(u, v) }
    allNodes.map(n => n -> find(n)).toMap
  }

  // Correctness Verifier

  /**
   * Verifies CCF output against the Union-Find reference.
   * Compares the partition (set of node groups) — not the label values,
   * since isolated minimum nodes are absent from ccfPairs.
   *
   * @param edges    Input edge list
   * @param ccfPairs Output (node, componentID) pairs from CCF
   * @return true if partitions match, false otherwise
   */
  def verifyCorrectness(edges: Seq[(Int, Int)], ccfPairs: Seq[(Int, Int)]): Boolean = {
    val allNodes = edges.flatMap { case (u, v) => Seq(u, v) }.toSet

    // Reference partition from Union-Find
    val refLabels = referenceComponents(edges)

    // CCF partition: absent nodes map to themselves (they are their own minimum)
    val ccfLabel = mutable.Map.from(allNodes.map(n => n -> n))
    ccfPairs.foreach { case (node, comp) => ccfLabel(node) = comp }

    // Build group sets and compare
    def toPartition(labels: Map[Int, Int]): Set[Set[Int]] =
      labels.groupBy(_._2).values.map(_.keySet).toSet

    toPartition(refLabels) == toPartition(ccfLabel.toMap)
  }
}
