name         := "ccf-spark"
version      := "1.0"
scalaVersion := "2.13.17"

// Use local Spark jars for compilation only (Spark 4.1.1 not on Maven Central)
Compile / unmanagedJars ++= {
  val sparkJarsDir = file("/usr/local/lib/spark/jars")
  (sparkJarsDir ** "*.jar").classpath
}

// CRITICAL: exclude all Spark jars from the assembled fat jar.
// They are "provided" by the cluster at runtime — bundling them causes OOM
// and produces a multi-GB jar that is rejected by spark-submit anyway.
assembly / assemblyExcludedJars := {
  val cp           = (assembly / fullClasspath).value
  val sparkJarsDir = file("/usr/local/lib/spark/jars").getCanonicalPath
  cp filter { f => f.data.getCanonicalPath.startsWith(sparkJarsDir) }
}

Compile / unmanagedSources / excludeFilter :=
  HiddenFileFilter || ".ipynb_checkpoints"

assembly / assemblyMergeStrategy := {
  case PathList("META-INF", _*) => MergeStrategy.discard
  case _                        => MergeStrategy.first
}
