# Connected Graph Components with Spark (RDD vs DataFrame)

This project implements the CCF (Connected Components via Fast label propagation) algorithm for undirected graphs in two Spark styles:

- Python (PySpark): RDD API and DataFrame API
- Scala (Spark): RDD API and DataFrame API

The repository is designed for scalability experiments and correctness validation, comparing runtime and convergence behavior as graph size increases.

## Project Goals

- Implement the CCF algorithm in both RDD and DataFrame paradigms
- Verify correctness against a trusted reference implementation
- Benchmark runtime and convergence across increasing graph sizes
- Compare Python and Scala implementations on the same workload pattern

## Repository Structure

```text
connected-graph-components/
├── python/
│   ├── ccf_rdd.py              # CCF using PySpark RDD
│   ├── ccf_dataframe.py        # CCF using PySpark DataFrame
│   ├── graph_utils.py          # Graph generators + NetworkX verification
│   ├── experiments.py          # End-to-end correctness + scalability run
│   ├── ccf_results.csv         # Python benchmark output
│   └── ccf_scalability.png     # Python benchmark figure
├── scala/
│   └── cff-spark/
│       ├── src/main/scala/
│       │   ├── CCFRdd.scala
│       │   ├── CCFDataFrame.scala
│       │   ├── GraphUtils.scala
│       │   └── Experiments.scala
│       ├── ccf_results_scala.csv
│       ├── build.sbt
│       └── project/
│           ├── plugins.sbt
│           └── build.properties
├── ccf_runtime_comparison.png
├── ccf_df_rdd_ratio.png
└── Finding_connected_components_in_graph.pdf
```

## CCF Algorithm Summary

Each iteration has two phases:

1. CCF-Iterate
- Rebuild local neighborhoods (bidirectional edges)
- For each node, find the minimum neighbor label
- Emit:
  - Type-1 pair: node adopts the minimum label
  - Type-2 pairs: propagate the same minimum label to other neighbors

2. CCF-Dedup
- Remove duplicate pairs before the next iteration

Convergence criterion:
- Stop when no Type-2 pairs are emitted (or accumulator/count becomes zero)

## Implementations

### Python (PySpark)

- `python/ccf_rdd.py`: uses `groupByKey`, `flatMap`, and accumulator-based convergence
- `python/ccf_dataframe.py`: uses SQL transformations and per-iteration checkpointing
- `python/experiments.py`:
  - runs correctness checks on small graphs
  - runs scalability benchmarks over predefined graph sizes
  - writes `python/ccf_results.csv`
  - saves figure `python/ccf_scalability.png`

### Scala (Spark)

- `scala/cff-spark/src/main/scala/CCFRdd.scala`
- `scala/cff-spark/src/main/scala/CCFDataFrame.scala`
- `scala/cff-spark/src/main/scala/GraphUtils.scala`
- `scala/cff-spark/src/main/scala/Experiments.scala`

The Scala version mirrors the Python logic closely and exports results to `scala/cff-spark/ccf_results_scala.csv`.

## Requirements

### Python

- Python 3.9+
- Java 8 or 11 (for Spark)
- PySpark-compatible Spark installation
- Packages:
  - `pyspark`
  - `pandas`
  - `matplotlib`
  - `networkx`

Install Python dependencies:

```bash
pip install pyspark pandas matplotlib networkx
```

### Scala

- JDK 8/11/17
- sbt 1.9+
- Spark runtime available on machine/cluster

Important:
- `scala/cff-spark/build.sbt` currently points to Spark jars in `/usr/local/lib/spark/jars`
- Update that path for your environment before compiling/running

## How To Run

### 1) Python experiments

From repository root:

```bash
cd python
python experiments.py
```

Outputs:
- `python/ccf_results.csv`
- `python/ccf_scalability.png`

### 2) Scala experiments

From `scala/cff-spark`:

```bash
sbt run
```

Output:
- `scala/cff-spark/ccf_results_scala.csv`

Optional fat jar build:

```bash
sbt assembly
```

## Benchmark Snapshot

### Python (`python/ccf_results.csv`)

| Nodes | Edges | RDD Time (s) | DF Time (s) | RDD Iters | DF Iters |
|---:|---:|---:|---:|---:|---:|
| 1,000 | 3,000 | 4.186 | 13.406 | 6 | 6 |
| 5,000 | 15,000 | 4.932 | 14.285 | 6 | 6 |
| 10,000 | 30,000 | 6.739 | 14.442 | 6 | 6 |
| 50,000 | 150,000 | 15.883 | 32.171 | 7 | 7 |
| 100,000 | 300,000 | 27.240 | 45.495 | 7 | 7 |

### Scala (`scala/cff-spark/ccf_results_scala.csv`)

| Nodes | Edges | RDD Time (s) | DF Time (s) | RDD Iters | DF Iters |
|---:|---:|---:|---:|---:|---:|
| 1,000 | 3,000 | 3.069 | 15.301 | 6 | 6 |
| 5,000 | 15,000 | 6.046 | 16.728 | 6 | 6 |
| 10,000 | 30,000 | 5.818 | 16.298 | 6 | 6 |
| 50,000 | 150,000 | 11.716 | 44.099 | 7 | 7 |
| 100,000 | 300,000 | 31.682 | 76.002 | 7 | 7 |

Observed trend in current runs:
- RDD is faster than DataFrame for these iterative CCF workloads
- Both APIs converge in the same number of iterations for each graph size

## Notes on Spark Mechanics

- RDD version uses accumulator updates as convergence signal
- DataFrame version relies on counting Type-2 rows per iteration
- Checkpointing in DataFrame loop is essential to avoid query-plan lineage blow-up
- Deduplication (`distinct` / `dropDuplicates`) is crucial to control shuffle and memory pressure

## Reproducibility

- Graph generators use fixed random seeds where specified
- Graph size configurations are fixed in `experiments.py` and `Experiments.scala`
- For fair comparisons, run both implementations on the same machine and Spark settings

## Author / Context

This repository is a course project for ML for Big Data at Paris Dauphine University, focused on distributed graph component computation and Spark API trade-offs.