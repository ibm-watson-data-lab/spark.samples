name := "streaming-twitter"

version := "1.0"

scalaVersion := "2.10.4"

libraryDependencies ++= {
  val sparkVersion =  "1.3.1"
  Seq(
    "org.apache.spark" %%  "spark-core"	  %  sparkVersion ,
    "org.apache.spark" %%  "spark-sql"  %  sparkVersion ,
    "org.apache.spark" %%  "spark-streaming"	  %  sparkVersion ,
    "org.apache.spark" %%  "spark-streaming-twitter"  %  sparkVersion ,
    "org.apache.spark" %% "spark-repl" % sparkVersion,
    "org.scalaj" %% "scalaj-http" % "1.1.5"
  )
}