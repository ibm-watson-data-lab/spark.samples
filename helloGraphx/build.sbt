name := "helloGraphx"

version := "1.0"

scalaVersion := "2.10.4"

libraryDependencies ++= {
  val sparkVersion =  "1.6.0"
  Seq(
    "org.apache.spark" %%  "spark-core"	  		%  sparkVersion % "provided",
    "org.apache.spark" %%  "spark-sql"	  		%  sparkVersion % "provided",
    "org.apache.spark" %%  "spark-graphx" 		%  sparkVersion % "provided",
    "org.apache.spark" %%  "spark-repl" 		% sparkVersion % "provided",
    "org.http4s"       %%  "http4s-core"    	% "0.8.2",
	"org.http4s"       %%  "http4s-client"  	% "0.8.2",
	"org.http4s"       %%  "http4s-blazeclient"	% "0.8.2"
  )
}
assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false)
