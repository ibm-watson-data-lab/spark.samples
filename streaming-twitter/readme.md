# Introduction  
In this post, we'll cover the "Spark streaming with Twitter and Watson" sample that can be found [here](https://github.com/ibm-cds-labs/spark.samples/tree/master/streaming-twitter). We'll discuss what this sample application does, how to build it from the source code and configure it for deployment on Bluemix.  
In short, this sample application monitors tweets from twitter based on Hashtags using Spark Streaming and enrich the data with sentiment analysis from Watson Tone Analyzer service available on Bluemix. It then shows how to use Spark SQL to load the data into DataFrame for further analysis. As an option, the data can be saved into a Cloudant database.  
From the technical side, this sample application covers the following topics:  
1. Spark Streaming  
2. Spark Streaming for Twitter Api  
3. Spark SQL  
4. Watson Tone Analyzer  

## User story  
John works in the marketting department of Lamda company that sells toys. He would like to know in real-time the sentiment trend (positive or negative) of what consumers are saying about his products on Twitter.  To achieve this, John will deploy the "Spark streaming with Twitter and Watson" application on Bluemix and periodically use the Notebook to analyze the data.

## Steps for building the application  
Before you begin, it is highly recommended to read the Tutorial for building a custom library for Spark and deploying it to a Jupyter Notebook in Apache Spark on Bluemix available [here]()   
1. Clone the source code on your local machine: `git clone https://github.com/ibm-cds-labs/spark.samples.git`  
2. From the project root directory, compile and assemble the jar using the following command: `sbt assembly`. This will create a uber jar (jar that contains the code and all its dependencies. Note: We'll discuss in a later post how to configure the build.sbt to create the uber jar.  
3. Post the jar on a publicly available url. Note: You can also reuse the already built at this [location](https://github.com/ibm-cds-labs/spark.samples/raw/master/dist/streaming-twitter-assembly-1.0.jar)  
4. Create a new twitter app on your accound and configure the OAuth credentials. Go to [https://apps.twitter.com/](https://apps.twitter.com/) and click on Create New App button  
5. In the next page, give a Name, description and Website (for the website, you can give the bluemix url corresponding to your Spark Application e.g. https://davidtaiebspark.mybluemix.net).  
6. Click Yes, I agree for the developer agreement and click on Create Your twitter application button  
7. In the next page, go to the Key and Access Tokens tab, then click on Create My Access Tokens button at the bottom of the page.  
8. Copy the Consumer Key, Consumer Secret, Access Token and Access Token Secret. You will need them later in this tutorial

## Deploying and running the application on Bluemix  
1. On Bluemix, create an instance of Spark Service (or reuse one that already exists).  
2. Add an Watson Tone Analyzer instance to and bind it to your Spark application:  
	* From the dashboard of your spark application, click on Add A SERVICE OR API  
	* Scroll to the bottom of the service catalog page and click on the Bluemix Labs Catalog link  
	* Select the Tone Analyzer service and complete the form
	* Here's a screenshot of the Spark application with Watson Tone Analyzer service added:  
	![Dashboard with Spark and Watson Tone Analyzer](http://developer.ibm.com/clouddataservices/wp-content/uploads/sites/47/2015/09/Spark-Streaming-Twitter-add-Watson-Tone-Analyzer-1024x799.png)  
3. Click on the Show Credentials link of the Tone Analyzer service and copy the following information (you'll need it later when running the app in a Notebook):  

```javascript  
		"credentials": {  
        "url": "XXXXX",  
        "username": "XXXXX",  
        "password": "XXXXX"  
      }
```  

4. Launch the Spark application and create a new notebook
5. On the first cell, install the application jar created in the previous section: `%AddJar https://github.com/ibm-cds-labs/spark.samples/raw/master/dist/streaming-twitter-assembly-1.0.jar -f`  
6. In the next cell, we'll configure the credential parameters needed to connect to Twitter and Watson Tone Analyzer service:  

```scala  
	val demo = com.ibm.cds.spark.samples.StreamingTwitter //Shorter handle  
	//Twitter OAuth params from section above
	demo.setConfig("twitter4j.oauth.consumerKey","XXXX")
	demo.setConfig("twitter4j.oauth.consumerSecret","XXXXXX")
	demo.setConfig("twitter4j.oauth.accessToken","XXXX")
	demo.setConfig("twitter4j.oauth.accessTokenSecret","XXXX")
	//Tone Analyzer service credential copied from section above
	demo.setConfig("watson.tone.url","XXXX")
	demo.setConfig("watson.tone.password","XXXX")
	demo.setConfig("watson.tone.username","XXXX")
```  
7. The following code will start the streaming from Twitter and collect the data. Note: There is an optional parameter that allows you to specify a duration of the stream, if not specify then the stream will run until explicitly stopped using stopTwitterStreaming api.  

```scala 
	import org.apache.spark.streaming._
	demo.startTwitterStreaming(sc, Seconds(30))
	
	Output:
		Twitter stream started
		Tweets are collected real-time and analyzed
		To stop the streaming and start interacting with the data use: StreamingTwitter.stopTwitterStreaming
		Stopping Twitter stream. Please wait this may take a while
		Twitter stream stopped
		You can now create a sqlContext and DataFrame with 447 Tweets created. 
		Sample usage: 
			val (sqlContext, df) = com.ibm.cds.spark.samples.StreamingTwitter.createTwitterDataFrames(sc)
			df.printSchema
			sqlContext.sql("select author, text from tweets").show
```  
8. Once the stream is stopped, you can create a DataFrame using Spark SQL and start querying the data:

```scala 
	val (sqlContext, df) = demo.createTwitterDataFrames(sc)
	
	Output:
		A new table named tweets with 447 records has been correctly created and can be accessed through the SQLContext variable
		Here's the schema for tweets
		root
		 |-- author: string (nullable = true)
		 |-- date: string (nullable = true)
		 |-- lang: string (nullable = true)
		 |-- text: string (nullable = true)
		 |-- lat: integer (nullable = true)
		 |-- long: integer (nullable = true)
		 |-- Cheerfulness: double (nullable = true)
		 |-- Negative: double (nullable = true)
		 |-- Anger: double (nullable = true)
		 |-- Analytical: double (nullable = true)
		 |-- Confident: double (nullable = true)
		 |-- Tentative: double (nullable = true)
		 |-- Openness: double (nullable = true)
		 |-- Agreeableness: double (nullable = true)
		 |-- Conscientiousness: double (nullable = true)
```  

9. Display a sample of the data

```scala 
	val set1 = sqlContext.sql("select * from tweets")  //Select all columns
	set1.show
	
	Output:  
			author              date                 lang text                 lat long Cheerfulness      Negative Anger Analytical        Confident Tentative Openness           Agreeableness      Conscientiousness 
		Miji                Wed Sep 16 11:18:... en   @zetty_iqma mana ... 0   0    0.0               0.0      0.0   0.0               0.0       0.0       97.43              0.0                67.54             
		SonnoNHisPrime      Wed Sep 16 11:18:... en   Somebody come see me 0   0    0.0               0.0      0.0   100.0             0.0       100.0     0.0                100.0              0.0               
		__moataz            Wed Sep 16 11:18:... en   ?? ??? ??? ??? ??... 0   0    0.0               0.0      0.0   0.0               0.0       0.0       97.43              0.0                67.54             
		??????????????      Wed Sep 16 11:18:... en   RT @WonPanie407: ... 0   0    0.0               0.0      0.0   0.0               0.0       0.0       0.1                100.0              67.54             
		Alka Dhillon        Wed Sep 16 11:18:... en   Learning spiritua... 0   0    0.0               0.0      0.0   100.0             0.0       0.0       86.81              30.81 16.42             
		M$.OCTOBER??        Wed Sep 16 11:18:... en   @Chubb__Pyrex you... 0   0    0.0               0.0      0.0   0.0               100.0     0.0       0.0                98.32              11.63             
		#VamosLaU           Wed Sep 16 11:18:... en   RT @ybeitollahi: ... 0   0    97.57 0.0      0.0   0.0               0.0       0.0       14.82 79.77              24.9              
		menaquinone4        Wed Sep 16 11:18:... en   as if modern comm... 0   0    0.0               0.0      0.0   0.0               0.0       0.0       100.0              25.41 55.96             
		Freddy Todd         Wed Sep 16 11:18:... en   RT @TheUntz: Our ... 0   0    100.0             0.0      0.0   0.0               0.0       0.0       0.19               100.0              100.0             
		J.R.Terrazzino      Wed Sep 16 11:18:... en   #ExplainEarthIn4W... 0   0    100.0             0.0      0.0   0.0               0.0       0.0       0.0                100.0              67.54             
		Lion's Pride        Wed Sep 16 11:18:... en   .@dilmabr @portal... 0   0    98.63             0.0      0.0   0.0               0.0       89.35     41.05              89.25  54.94
		sathya rajan        Wed Sep 16 11:18:... en   #Kabali http://t.... 0   0    0.0               0.0      0.0   0.0               0.0       0.0       97.43              0.0                67.54             
		Robert Perry        Wed Sep 16 11:18:... en   @LamiaAsaggau Hel... 0   0    97.16             0.0      0.0   93.11             0.0       0.0       10.5               87.67              0.97              
		B. Rabbit           Wed Sep 16 11:18:... en   She stayed Puttin... 0   0    100.0             0.0      0.0   0.0               0.0       93.76     0.0                100.0              56.83
		C.R.E.A.??          Wed Sep 16 11:18:... en   RT @ChaizYnic: Ni... 0   0    0.0               0.0      0.0   0.0               0.0       0.0       0.66               90.49  67.54             
		ChelBellz ?         Wed Sep 16 11:18:... en   I missed out on s... 0   0    0.0               100.0    0.0   100.0             0.0       0.0       0.0                98.26              0.0               
		tiradtoss           Wed Sep 16 11:18:... en   Good afternoon ev... 0   0    100.0             0.0      0.0   0.0               0.0       0.0       0.0                100.0              94.78 
		nadia ilagan        Wed Sep 16 11:18:... en   http://t.co/w13ZL... 0   0    0.0               0.0      0.0   0.0               0.0       0.0       97.43              0.0                67.54             
		MegaWebsiteServices Wed Sep 16 11:18:... en   AllieDoughty: Sta... 0   0    0.0               0.0      0.0   83.28 0.0       0.0       95.28  42.22              93.61             
		LatoyaPower         Wed Sep 16 11:18:... en   ???? https://... 0   0    0.0               0.0      0.0   0.0               0.0       0.0       97.43              0.0                67.54             
```  
10. In this other query example, we filter the data to show only the tweets that have an Anger greater than 70%

```scala 
	val set2 = sqlContext.sql("select author, text, Anger from tweets where Anger > 70")
	println(set2.count)
	set2.show
	
	Output:
		22
		author            text                 Anger
		Ariel Evite       RT @YayDen2015: A... 100.0
		w.                if you think you'... 100.0
		John Boy          RT @ericsshadow: ... 100.0
		Michael kelly     All these lads ge... 100.0
		1Creek            RT @MotherSassy: ... 100.0
		Jaeden            @AnthonyImanse so... 100.0
		Millionairclubb   RT @nikestore: Fo... 100.0
		menaka            RT @tlam28: Faceb... 100.0
		American Theatre  RT @rjreporter: T... 100.0
		$$$$$ liaa $$$$$  RT @__caprisunnn:... 100.0
		Jake of Distictin I've sneezed abou... 100.0
		Startin'Guard     I hate a try to f... 100.0
		Jefferson Tibbs   I hate fruity gum    100.0
		mamajonet         The Frustrating L... 100.0
		brookeyohoho      The Frustrating L... 100.0
		Taylor Staley     RT @obxcurity: ha... 100.0
		blurryface.       RT @trapadelics: ... 100.0
		K. Stiles         RT @tuckonthis: M... 100.0
		Abigail Toppen    RT @tbhjuststop: ... 100.0
		Corey Van't Haaff RT @VancouverSun:... 100.0
```  

## Conclusion
In this tutorial, you've learned how to build and deploy a complex Spark application that integrates multiple services from Bluemix. The next step is to do a deep dive on the code to understand how it was built, so it can give you ideas about how to extend it in the future. We'll do just that in a future post. 
Happy Sparking!


