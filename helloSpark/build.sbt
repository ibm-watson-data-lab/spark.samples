name := "helloSpark"

version := "1.0"

scalaVersion := "2.10.4"

libraryDependencies ++= {
  val sparkVersion =  "1.3.1"
  Seq(
    "org.apache.spark" %%  "spark-core"	  %  sparkVersion ,
    "org.apache.spark" %%  "spark-sql"	  %  sparkVersion ,
    "org.apache.spark" %% "spark-repl" % sparkVersion
  )
}