# experiments.py
# Scalability experiments: RDD vs DataFrame on graphs of increasing size.
#
# Measures:
#   - Wall-clock time per graph configuration
#   - Number of iterations until convergence
#   - Number of connected components found
#   - Correctness verified against NetworkX on small graphs

import time
import pandas as pd
import matplotlib.pyplot as plt

from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType

from graph_utils import (
    generate_random_graph,
    generate_chain_graph,
    generate_clustered_graph,
    verify_with_networkx,
)
from ccf_rdd import run_ccf_rdd
from ccf_dataframe import run_ccf_df


# Spark Initialisation

def init_spark(app_name="CCF-Experiments", master="local[*]"):
    """
    Creates and returns (SparkContext, SparkSession).
    Uses local mode; replace master with cluster URL for distributed runs.
    """
    conf = (
        SparkConf()
        .setAppName(app_name)
        .setMaster(master)
        .set("spark.driver.memory", "4g")
        .set("spark.sql.shuffle.partitions", "8")   # tune for cluster size
    )
    sc    = SparkContext(conf=conf)
    spark = SparkSession(sc)
    sc.setLogLevel("WARN")
    # Required for DataFrame checkpointing — breaks iterative lineage chains
    sc.setCheckpointDir("/tmp/spark_ccf_checkpoints")
    return sc, spark


# Correctness Verification

def verify_correctness(sc, spark):
    """
    Runs CCF on two small hand-crafted graphs and verifies the output
    against NetworkX's reference implementation.

    Graph 1 (from the paper): 6 edges, two components {A,B,C,D,E} and {F,G,H}
    Graph 2: 3 isolated clusters of 5 nodes each → 3 components expected
    """
    print("\n" + "="*60)
    print("CORRECTNESS VERIFICATION")
    print("="*60)

    schema = StructType([
        StructField("src", IntegerType(), False),
        StructField("dst", IntegerType(), False),
    ])

    # Graph from Figure 5 of the paper (nodes encoded as integers)
    # A=0, B=1, C=2, D=3, E=4, F=5, G=6, H=7
    paper_edges = [(0,1),(1,2),(1,3),(3,4),(5,6),(6,7)]

    for label, edges in [("Paper example (Fig.5)", paper_edges),
                         ("Clustered (3×5)",
                          generate_clustered_graph(3, 5))]:
        # RDD check
        rdd   = sc.parallelize(edges)
        res_rdd, _ = run_ccf_rdd(sc, rdd, verbose=True)
        ok_rdd = verify_with_networkx(edges, res_rdd.collect())

        # DataFrame check
        df_in = spark.createDataFrame(pd.DataFrame(edges, columns=["src","dst"]), schema)
        res_df, _ = run_ccf_df(spark, df_in, verbose=True)
        ok_df = verify_with_networkx(edges, res_df.toPandas().values.tolist())

        print(f"\n  Graph : {label}")
        print(f"  RDD correctness       : {'✓ PASS' if ok_rdd else '✗ FAIL'}")
        print(f"  DataFrame correctness : {'✓ PASS' if ok_df  else '✗ FAIL'}")


# Scalability Experiment

# Graph configurations: (n_nodes, n_edges)
# Edge density ≈ 3× nodes to keep graphs connected but sparse
GRAPH_CONFIGS = [
    (1_000,    3_000),
    (5_000,   15_000),
    (10_000,  30_000),
    (50_000, 150_000),
    (100_000, 300_000),
]

SCHEMA = StructType([
    StructField("src", IntegerType(), False),
    StructField("dst", IntegerType(), False),
])


def run_scalability_experiment(sc, spark, n_runs=1):
    """
    Runs both RDD and DataFrame CCF on graphs of increasing size.

    For each configuration:
      - Generates a random graph (Erdos-Renyi style, fixed seed for reproducibility)
      - Times the full CCF loop (including Spark job submission overhead)
      - Records: wall-clock time, number of iterations, number of components

    Parameters:
        n_runs : number of repeated runs per configuration (average is reported)

    Returns:
        pd.DataFrame with columns:
            n_nodes, n_edges, time_rdd, time_df,
            iters_rdd, iters_df, components_rdd, components_df
    """
    print("\n" + "="*60)
    print("SCALABILITY EXPERIMENT")
    print("="*60)

    records = []

    for (n_nodes, n_edges) in GRAPH_CONFIGS:
        print(f"\nGraph: {n_nodes:>7,} nodes | {n_edges:>8,} edges")
        print("-" * 50)

        edges = generate_random_graph(n_nodes, n_edges, seed=42)

        # Accumulate timing over n_runs
        times_rdd, times_df = [], []
        iters_rdd = iters_df = comps_rdd = comps_df = None

        for run in range(n_runs):

            # RDD run
            edges_rdd = sc.parallelize(edges, numSlices=8)
            edges_rdd.cache(); edges_rdd.count()          # warm-up cache

            t0 = time.time()
            res_rdd, iters_rdd = run_ccf_rdd(sc, edges_rdd, verbose=True)
            times_rdd.append(time.time() - t0)

            # Count distinct component IDs (min-nodes not in pairs count themselves)
            comps_rdd = res_rdd.map(lambda x: x[1]).distinct().count()
            edges_rdd.unpersist()

            # DataFrame run 
            edges_pd = pd.DataFrame(edges, columns=["src", "dst"])
            edges_df_spark = spark.createDataFrame(edges_pd, schema=SCHEMA)
            edges_df_spark.cache(); edges_df_spark.count()

            t0 = time.time()
            res_df, iters_df = run_ccf_df(spark, edges_df_spark, verbose=True)
            times_df.append(time.time() - t0)

            comps_df = res_df.select("dst").distinct().count()
            edges_df_spark.unpersist()

        row = {
            "n_nodes":       n_nodes,
            "n_edges":       n_edges,
            "time_rdd":      round(sum(times_rdd) / n_runs, 3),
            "time_df":       round(sum(times_df)  / n_runs, 3),
            "iters_rdd":     iters_rdd,
            "iters_df":      iters_df,
            "components_rdd": comps_rdd,
            "components_df":  comps_df,
        }
        print(f"  → RDD: {row['time_rdd']:.2f}s | DF: {row['time_df']:.2f}s "
              f"| iters={iters_rdd} | components={comps_rdd}")
        records.append(row)

    return pd.DataFrame(records)


# Plotting

def plot_results(df, output_path="ccf_scalability.png"):
    """
    Produces a 2-panel figure:
      Left  — Wall-clock time vs. number of nodes (RDD vs DataFrame)
      Right — Number of iterations to convergence vs. number of nodes
    """
    fig, axes = plt.subplots(1, 2, figsize=(13, 5))
    fig.suptitle("CCF Scalability: RDD vs DataFrame", fontsize=14, fontweight="bold")

    # Panel 1: Runtime
    ax = axes[0]
    ax.plot(df["n_nodes"], df["time_rdd"], marker="o", linewidth=2,
            color="steelblue",  label="RDD")
    ax.plot(df["n_nodes"], df["time_df"],  marker="s", linewidth=2,
            color="darkorange", label="DataFrame")
    ax.set_xlabel("Number of Nodes", fontsize=11)
    ax.set_ylabel("Wall-clock Time (s)", fontsize=11)
    ax.set_title("Runtime vs. Graph Size")
    ax.legend(fontsize=10)
    ax.grid(True, linestyle="--", alpha=0.6)

    # Panel 2: Iterations
    ax = axes[1]
    ax.plot(df["n_nodes"], df["iters_rdd"], marker="o", linewidth=2,
            color="steelblue",  label="RDD")
    ax.plot(df["n_nodes"], df["iters_df"],  marker="s", linewidth=2,
            color="darkorange", label="DataFrame", linestyle="--")
    ax.set_xlabel("Number of Nodes", fontsize=11)
    ax.set_ylabel("Iterations to Convergence", fontsize=11)
    ax.set_title("Convergence Speed")
    ax.legend(fontsize=10)
    ax.grid(True, linestyle="--", alpha=0.6)

    plt.tight_layout()
    plt.savefig(output_path, dpi=150)
    print(f"\nPlot saved → {output_path}")


# Entry Point

if __name__ == "__main__":
    sc, spark = init_spark()

    # Step 1: Verify correctness on small graphs
    verify_correctness(sc, spark)

    # Step 2: Run scalability experiments
    results = run_scalability_experiment(sc, spark, n_runs=1)

    # Step 3: Save results
    results.to_csv("ccf_results.csv", index=False)
    print("\n--- Results Table ---")
    print(results.to_string(index=False))

    # Step 4: Plot
    plot_results(results)

    sc.stop()
