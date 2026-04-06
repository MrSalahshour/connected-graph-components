import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{SparkSession, Row}
import org.apache.spark.sql.types._
import java.io.PrintWriter
import scala.collection.mutable.ListBuffer

object Experiments {

  val GRAPH_CONFIGS: Seq[(Int, Int)] = Seq(
    (1000,    3000),
    (5000,   15000),
    (10000,  30000),
    (50000, 150000),
    (100000, 300000)
  )

  val SCHEMA: StructType = StructType(Array(
    StructField("src", IntegerType, nullable = false),
    StructField("dst", IntegerType, nullable = false)
  ))

  def initSpark(
      appName: String = "CCF-Experiments-Scala",
      master:  String = "local[*]"
  ): (SparkContext, SparkSession) = {
    val conf = new SparkConf()
      .setAppName(appName)
      .setMaster(master)
      .set("spark.driver.memory", "4g")
      .set("spark.sql.shuffle.partitions", "8")

    val sc    = new SparkContext(conf)
    val spark = SparkSession.builder().config(conf).getOrCreate()
    sc.setLogLevel("WARN")
    sc.setCheckpointDir("/tmp/spark_ccf_checkpoints_scala")
    (sc, spark)
  }

  def verifyCorrectness(sc: SparkContext, spark: SparkSession): Unit = {
    println("\n" + "=" * 60)
    println("CORRECTNESS VERIFICATION")
    println("=" * 60)

    val paperEdges     = Seq((0,1),(1,2),(1,3),(3,4),(5,6),(6,7))
    val clusteredEdges = GraphUtils.generateClusteredGraph(3, 5)

    for ((label, edges) <- Seq(
          ("Paper example (Fig.5)", paperEdges),
          ("Clustered (3x5)",       clusteredEdges)
        )) {

      // RDD check
      val edgesRdd    = sc.parallelize(edges)
      val (resRdd, _) = CCFRdd.runCCFRdd(sc, edgesRdd, verbose = true)
      val okRdd       = GraphUtils.verifyCorrectness(edges, resRdd.collect().toSeq)

      // DataFrame check
      val rows        = edges.map { case (u, v) => Row(u, v) }
      val edgesDf     = spark.createDataFrame(sc.parallelize(rows), SCHEMA)
      val (resDf, _)  = CCFDataFrame.runCCFDf(spark, edgesDf, verbose = true)
      val ccfPairsDf  = resDf.collect().map(r => (r.getInt(0), r.getInt(1))).toSeq
      val okDf        = GraphUtils.verifyCorrectness(edges, ccfPairsDf)

      println(s"\n  Graph : $label")
      println(s"  RDD correctness       : ${if (okRdd) "✓ PASS" else "✗ FAIL"}")
      println(s"  DataFrame correctness : ${if (okDf)  "✓ PASS" else "✗ FAIL"}")
    }
  }

  case class ExperimentResult(
    nNodes: Int, nEdges: Int,
    timeRdd: Double, timeDf: Double,
    itersRdd: Int,   itersDf: Int,
    componentsRdd: Long, componentsDf: Long
  )

  def runScalabilityExperiment(sc: SparkContext, spark: SparkSession): Seq[ExperimentResult] = {
    println("\n" + "=" * 60)
    println("SCALABILITY EXPERIMENT")
    println("=" * 60)

    val results = ListBuffer[ExperimentResult]()

    for ((nNodes, nEdges) <- GRAPH_CONFIGS) {
      println(s"\nGraph: $nNodes nodes | $nEdges edges")
      println("-" * 50)

      val edges = GraphUtils.generateRandomGraph(nNodes, nEdges)

      // RDD run 
      val edgesRdd = sc.parallelize(edges, 8)
      edgesRdd.cache(); edgesRdd.count()

      val t0Rdd              = System.currentTimeMillis()
      val (resRdd, itersRdd) = CCFRdd.runCCFRdd(sc, edgesRdd, verbose = true)
      val timeRdd            = (System.currentTimeMillis() - t0Rdd) / 1000.0
      val componentsRdd      = resRdd.map(_._2).distinct().count()
      edgesRdd.unpersist()

      // DataFrame run
      val rowsRdd = sc.parallelize(edges.map { case (u, v) => Row(u, v) })
      val edgesDf = spark.createDataFrame(rowsRdd, SCHEMA)
      edgesDf.cache(); edgesDf.count()

      val t0Df             = System.currentTimeMillis()
      val (resDf, itersDf) = CCFDataFrame.runCCFDf(spark, edgesDf, verbose = true)
      val timeDf           = (System.currentTimeMillis() - t0Df) / 1000.0
      val componentsDf     = resDf.select("dst").distinct().count()
      edgesDf.unpersist()

      val r = ExperimentResult(nNodes, nEdges, timeRdd, timeDf,
                               itersRdd, itersDf, componentsRdd, componentsDf)
      println(f"  -> RDD: ${r.timeRdd}%.2fs | DF: ${r.timeDf}%.2fs | iters=${r.itersRdd} | components=${r.componentsRdd}")
      results += r
    }

    results.toSeq
  }

  def saveResultsCsv(results: Seq[ExperimentResult], path: String = "ccf_results_scala.csv"): Unit = {
    val writer = new PrintWriter(path)
    writer.println("n_nodes,n_edges,time_rdd,time_df,iters_rdd,iters_df,components_rdd,components_df")
    results.foreach { r =>
      writer.println(s"${r.nNodes},${r.nEdges},${r.timeRdd},${r.timeDf}," +
                     s"${r.itersRdd},${r.itersDf},${r.componentsRdd},${r.componentsDf}")
    }
    writer.close()
    println(s"\nResults saved -> $path")
  }

  def printResultsTable(results: Seq[ExperimentResult]): Unit = {
    println("\n--- Results Table ---")
    println(f"${"n_nodes"}%8s ${"n_edges"}%8s ${"time_rdd(s)"}%12s ${"time_df(s)"}%11s " +
            f"${"iters_rdd"}%10s ${"iters_df"}%9s ${"comps_rdd"}%10s ${"comps_df"}%9s")
    results.foreach { r =>
      println(f"${r.nNodes}%8d ${r.nEdges}%8d ${r.timeRdd}%12.3f ${r.timeDf}%11.3f " +
              f"${r.itersRdd}%10d ${r.itersDf}%9d ${r.componentsRdd}%10d ${r.componentsDf}%9d")
    }
  }

  def main(args: Array[String]): Unit = {
    val (sc, spark) = initSpark()
    verifyCorrectness(sc, spark)
    val results = runScalabilityExperiment(sc, spark)
    saveResultsCsv(results)
    printResultsTable(results)
    sc.stop()
  }
}
