name := "helloSpark"

version := "2.1"

scalaVersion := "2.10.4"

libraryDependencies ++= {
  val sparkVersion =  "1.6.0"
  Seq(
    "org.apache.spark" %%  "spark-core"	  %  sparkVersion % "provided",
    "org.apache.spark" %%  "spark-sql"	  %  sparkVersion % "provided",
    "org.apache.spark" %% "spark-repl" % sparkVersion % "provided"
  )
}
assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false)
