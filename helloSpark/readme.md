# Tutorial: Build a custom library for Spark and deploy it to a Jupyter Notebook in Apache Spark on Bluemix

##Introduction  
New to developing Spark applications?  This is the tutorial for you. It provides detailed end-to-end steps needed to build a simple custom library for Spark (written in [scala](http://scala-lang.org/)) and shows how to deploy it on [IBM Analytics for Apache Spark for Bluemix](https://console.ng.bluemix.net/catalog/apache-spark-starter/), giving you the foundation for building real-life production applications


In this tutorial, you'll learn how to: 

1. Create a new Scala project using sbt and package it as a deployable jar   
3. Deploy the jar into a Jupyter Notebook on Bluemix  
4. Call the helper functions from a Notebook cell  
5. Optional: Import, test and debug your project into Scala IDE for Eclipse 

##Requirements 

To complete these steps you'll need to:

* be familiar with the [scala language](http://scala-lang.org/) and [jupyter notebooks](https://jupyter.org/)
* [download scala runtime 2.10.4](http://www.scala-lang.org/download/2.10.4.html) 
* [download homebrew](http://brew.sh/) 
* [download scala sbt](http://www.scala-sbt.org/download.html) (simple build tool) 




##Create a Scala project using sbt 

There are multiple build frameworks you can use to build Spark projects. For example, [Maven](https://maven.apache.org/) is popular with enterprise-build engineers. For this tutorial, we chose SBT because setup is fast and its easy to work with.
  
The following steps guide you through creation of a new Spark project. Or, you can directly download the code from this [Github repository](https://github.com/ibm-cds-labs/spark.samples): 
 
1. Open a terminal or command line window. cd to the directory that contains your development project and create a directory named **helloSpark**:  
 
   ```  
	mkdir helloSpark && cd helloSpark
   ```  	 

2. Create the recommended directory layout for projects builts by Maven or SBT by entering these 3 commands:  

   ```  
	mkdir -p src/main/scala
	mkdir -p src/main/java
	mkdir -p src/main/resources
  ```  

3. In src/main/scala directory, create a subdirectory that corresponds to the package of your choice, like `mkdir -p com/ibm/cds/spark/samples`. Then create a new file called **HelloSpark.scala** and in your favorite editor, add the following contents. 

   ```scala
	package com.ibm.cds.spark.samples

	import org.apache.spark._

	object HelloSpark {
	  	//main method invoked when running as a standalone Spark Application
  		def main(args: Array[String]) {
    		val conf = new SparkConf().setAppName("Hello Spark")
    		val spark = new SparkContext(conf)

    		println("Hello Spark Demo. Compute the mean and variance of a collection")
    		val stats = computeStatsForCollection(spark);
    		println(">>> Results: ")
    		println(">>>>>>>Mean: " + stats._1 );
    		println(">>>>>>>Variance: " + stats._2);
    		spark.stop()
  		}
  
  		//Library method that can be invoked from Jupyter Notebook
  		def computeStatsForCollection( spark: SparkContext, countPerPartitions: Int = 100000, partitions: Int=5): (Double, Double) = {    
    		val totalNumber = math.min( countPerPartitions * partitions, Long.MaxValue).toInt;
    		val rdd = spark.parallelize( 1 until totalNumber,partitions);
    		(rdd.mean(), rdd.variance())
  		}
	}
   ```

4. Create your sbt build definition.  To do so, in your project root directory, create a file called build.sbt and add the following code to it:  
   
   ```scala
	name := "helloSpark"
	
	version := "1.0"

	scalaVersion := "2.10.4"

	libraryDependencies ++= {
  		val sparkVersion =  "1.3.1"
	  	Seq(
	    	"org.apache.spark" %% "spark-core" % sparkVersion,
	    	"org.apache.spark" %% "spark-sql" % sparkVersion,
	    	"org.apache.spark" %% "spark-repl" % sparkVersion 
	  	)
	}
   ```  
The libraryDependencies line tells sbt to download the specified spark components. In this example, we specify dependencies to spark-core, spark-sql, and spark-repl, but you can add more spark components dependencies. Just follow the same pattern, like: spark-mllib, spark-graphx, and so on. [Read detailed documentation on sbt build definition](http://www.scala-sbt.org/0.12.4/docs/Getting-Started/Hello.html).
 
5. From the root directory of your project, run the following command: `sbt update`. This command uses Apache Ivey to compute all the dependencies and download them in your local machine at <home>/.ivy2/cache directory.
6. Compile your source code by entering the following command: `sbt compile`
7. Package your compiled code as a jar by entering the following command: `sbt package`. 

You should see a file name hellospark_2.10-1.0.jar in the root directory of your project.  
You'll see that the naming convention for the jar file is: \[projectName\]\_\[scala_version\]\_\[projectVersion\]  

 

##Deploy your custom library jar to a Jupyter Notebook

With your custom library built and packaged, you're ready to deploy it to a Jupyter Notebook on Bluemix.

1. If you haven't already, sign up for to [Bluemix](http://www.bluemix.net), IBM's open cloud platform for building, running, and managing applications.
2. Create an app with the Spark service. 

    On the Bluemix dashboard, click the **Create App** tile. 
Then select **Web** and click **Browse Boilerplates**. 
Click the [Apache Spark starter service](https://console.ng.bluemix.net/catalog/apache-spark-starter/). 
Name the app and click **Create**.


2. Get the deployable jar on a publicly available url by doing one of the following:
 - Upload the jar into a github repository. Note the the download url. You'll use in Step 5 to deploy the jar into Spark as a Service.
 - Or, you can use our sample jar, which is pre-built and posted [here on github](https://github.com/ibm-cds-labs/spark.samples/raw/master/dist/hellospark_2.10-1.0.jar) 
4. Open the Spark application and launch the Notebook.  
5. Use the following special command called AddJar to upload the jar to the Spark service. Insert the URL of your jar.

 `%AddJar https://github.com/ibm-cds-labs/spark.samples/raw/master/dist/hellospark_2.10-1.0.jar -f`   
 
   That `%` before `AddJar` is a special command. The `-f` forces the download even if the file is already in the cache.  
       _**Note:**This command is currently available, but may be deprecated in an upcoming release. We'll update this tutorial at that time._

Now that you deployed the jar, you can call APIs from within the Notebook.  

##Call the helper functions from a Notebook cell 
In the notebook, call the code from the helloSpark sample library. In a new cell, add the following code:  

	```  val countPerPartitions = 500000
	var partitions = 10
	val stats = com.ibm.cds.spark.samples.HelloSpark.computeStatsForCollection(
   			 sc, countPerPartitions, partitions)
	println("Mean: " + stats._1)
	println("Variance: " + stats._2)```  

The following screen shot shows the final results on a Jupyter Notebook: ![Hello Spark Jupyter Demo](http://developer.ibm.com/clouddataservices/wp-content/uploads/sites/47/2015/09/Spark-Tutorial-HelloSpark-Jupyter-1024x509.png)

##Optional: Import, test, and debug your project in Scala IDE for Eclipse     


If you want to get serious and import, test, and debug your project in a local deployment of Spark, follow these steps for working in Eclipse. 

1.  [Download the Scala IDE for Eclipse](http://scala-ide.org/download/current.html). (Note that you can alternatively use the [Intellij scala IDE](https://www.jetbrains.com/idea/features/scala.html) but it's easier to follow this tutorial with Scala IDE for Eclipse)

2.  Install sbteclipse (sbt plugin for Eclipse) with a simple edit to the  plugins.sbt file, located in ~/.sbt/0.13/plugins/ (If you can't find this file, create it.)  [Read how to install](https://github.com/typesafehub/sbteclipse/wiki/Installing-sbteclipse).   

3. Configure Scala IDE to run with Scala 2.10.4

    Launch Eclipse and from the menu, choose **Scala IDE > Preferences**. Choose **Scala > Installations** and click the **Add** button. Navigate to your scala 2.10.4 installation root directory, select the **lib** directory, and click **Open**. Name your installation (something like **2.10.4**) and click **OK**. Click **OK** to close the dialog box 

	![Scala installation](https://developer.ibm.com/clouddataservices/wp-content/uploads/sites/47/2015/09/Spark-tutorial-Configure-Scala-2.10.4-installation.png)  
	
2. Generate the eclipse artifacts necessary to import the project into Scala IDE for eclipse. 

    Return to your Terminal or Command Line window. From your project's root directory use the following command: `sbt eclipse`. Once done, verify that .project and .classpath have been successfully created.  

3. Return to Scala IDE, and from the menu, choose **File > Import**. In the dialog that opens, choose **General > Existing Projects into Workspace**.  

4. Beside **Select root directory**, click the **Browse** button and navigate to the root directory of your project, then click **Finish**: ![Import Scala project](http://developer.ibm.com/clouddataservices/wp-content/uploads/sites/47/2015/09/Spark-tutorial-Import-Scala-Project-1024x560.png)  

5. Configure the scala installation for your project. 

    The project will automatically compile. On the lower right of the screen, on the **Problems** tab, errors appear, because you need to configure the scala installation for your project. To do so, right-click your project and select **Scala > Set the Scala installation**. In the dialog box that appears, select **2.10.4** (or whatever you named your installation). Click **OK** and wait until the project recompiles. On the **Problems** tab, there are no errors this time: 
![Set Scala Installation Menu](http://developer.ibm.com/clouddataservices/wp-content/uploads/sites/47/2015/09/Spark-tutorial-set-Scala-Intallation-1-1024x997.png)  ![Select Scala Installation](http://developer.ibm.com/clouddataservices/wp-content/uploads/sites/47/2015/09/Spark-tutorial-set-Scala-installation-2.png)

6. Export the dependency libraries. (This will make it easier to create the launch configuration in the next step). 

    Right click on the helloSpark project, and select **Properties**  In the Properties dialog box, click **Java Build Path**. Thee **Order and Export** tab opens on the right. Click the **Select All** button and click **OK**:
	![Export dependency libraries](http://developer.ibm.com/clouddataservices/wp-content/uploads/sites/47/2015/09/Spark-tutorial-export-dependency-libraries-1024x790.png)

6. Create a launch configuration that will start a spark-shell. 
  1. From the menu, choose **Run > Run Configurations**.
  2. Right-click **Scala Application** select **New**.
  3. In **Project**, browse to your helloSpark project and choose it.
  4. In **Main Class**, type `org.apache.spark.deploy.SparkSubmit`   
  5. Click the **Arguments** tab, go to the **Program Arguments** box and type:
 `--class org.apache.spark.repl.Main spark-shell`  
Then within **VM Arguments** type:
 `-Dscala.usejavacp=true
-Xms128m -Xmx800m -XX:MaxPermSize=350m -XX:ReservedCodeCacheSize=64m`  
![Launch Configuration](http://developer.ibm.com/clouddataservices/wp-content/uploads/sites/47/2015/09/Spark-tutorial-Launch-configuration-1024x677.png)

11. Click **Run**.  

12. Configuration will run in the **Console** and completes with a scala prompt.


Now you know how to run/debug a spark-shell from within your development environment that includes your project in the classpath, which can then be called from the shell interpreter.  
You can also build a self-contained [Spark Application](http://spark.apache.org/docs/latest/quick-start.html#self-contained-applications) and run it manually using spark-submit or scheduling. The sample code that comes with this tutorial is designed to run both as a Spark Application and a reusable library. If you want to run/debug the Spark application from within the Scala IDE, then you can follow the same steps as above, but in Step --9.5--, replace the call in the Program Arguments box with the fully qualified name of your main class, like `--class com.ibm.cds.spark.samples.HelloSpark spark-shell`

##Summary
You just learned how to build your own library for Spark, and share it via Notebook on the cloud. You can also manage your project with the import, test, and debug features of Scala IDE for Eclipse. In future posts, we'll dive into more sample apps that cover Spark SQL, Spark Streaming, and many more of the powerful components that Spark has to offer.