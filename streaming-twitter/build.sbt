name := "streaming-twitter"

version := "1.0"

scalaVersion := "2.10.4"

libraryDependencies ++= {
  val sparkVersion =  "1.3.1"
  Seq(
    "org.apache.spark" %%  "spark-core"	  %  sparkVersion % "provided",
    "org.apache.spark" %%  "spark-sql"  %  sparkVersion % "provided",
    "org.apache.spark" %%  "spark-streaming"	  %  sparkVersion % "provided",
    "org.apache.spark" %%  "spark-streaming-twitter"  %  sparkVersion,
    "org.apache.spark" %% "spark-repl" % sparkVersion % "provided",
    "org.scalaj" %% "scalaj-http" % "1.1.5",
    "com.ibm" %% "couchdb-scala" % "0.5.3"
  )
}

assemblyMergeStrategy in assembly := {
  case PathList("org", "apache", "spark", xs @ _*) => MergeStrategy.first
  case x =>
    val oldStrategy = (assemblyMergeStrategy in assembly).value
    oldStrategy(x)
}

resolvers += "Local couchdb-scala repo" at "file:///Users/dtaieb/watsondev/workspaces/cds_workspace/couchdb-scala/target/scala-2.10/releases"