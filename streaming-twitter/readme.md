Use Spark Streaming in combination with IBM Watson to perform sentiment analysis showing how a conversation is trending on Twitter.

# Introduction   

How's your relationship with your customers?  You can track how consumers feel about you, your products, or your company based on their tweets.  Guage positive or negative emotions measured across multiple tone dimensions, like anger, cheerfulness, openness, and more. To get real-time sentiment analysis, deploy the **Spark Streaming with Twitter and Watson** app on Bluemix and use its Notebook to analyze public opinion.

This tutorial covers how to build this app from the source code, configure it for deployment on Bluemix, and analyze the data to produce compelling, insight-revealing visualizations.

 
##How it works

This sample app uses Spark Streaming to create a feed that captures live tweets from Twitter. You can optionally filter the tweets that contain the hashtag(s) of your choice. The tweet data is enriched in real time with various sentiment scores provided by the Watson Tone Analyzer service (available on Bluemix). This service provides insight into sentiment, or how the author feels. We then use Spark SQL to load the data into a DataFrame for further analysis. You can also save the data into a Cloudant database or a parquet file and use it later to track how you're trending over longer periods.  
The following diagram provides a high level architecture of the application:
![Twitter + Watson high level architecture](https://i2.wp.com/developer.ibm.com/clouddataservices/wp-content/uploads/sites/47/2015/10/Spark-Streaming-Twitter-architecture.png)


##Before you begin
If you haven't already, read and follow the [tutorial on how to build a custom library for Spark and deploy it to a Jupyter Notebook in Apache Spark on Bluemix]().


## Build the application  

   
1. Clone the source code on your local machine: `git clone https://github.com/ibm-cds-labs/spark.samples.git`  
2. Go to the sub-directory that contains the code for this application: `cd streaming-twitter`.
3. Compile and assemble the jar using the following command: `sbt assembly`. This creates an _uber jar_ (jar that contains the code and all its dependencies).  
_**Note:** We'll soon update this tutorial with a link to information on how to assemble uber jars._
  
4. Post the jar on a publicly available url, by doing one of the following: 
   + Upload the jar into a github repository. (Note its download url. You'll use in a few minutes.)
    + Or, you can use our sample jar, which is pre-built and posted [here on github](https://github.com/ibm-cds-labs/spark.samples/raw/master/dist/streaming-twitter-assembly-1.0.jar).
4. Create a new app on your Twitter account and configure the OAuth credentials. 
	5. Go to [https://apps.twitter.com/](https://apps.twitter.com/). Sign in and click the **Create New App** button  
	5. Complete the required fields:
		+ **Name** and **Description** can be anything you want. 
		+ **Website.** Enter the bluemix url for to your Spark Application, like: _https://davidtaiebspark.mybluemix.net_  
	6. Below the developer agreement, turn on the  **Yes, I agree** check box and click **Create your Twitter application**.  
	7. Click the **Keys and Access Tokens** tab.
	8. Scroll to the bottom of the page and click the **Create My Access Tokens** button.  
	8. Copy the **Consumer Key**, **Consumer Secret**, **Access Token**, and **Access Token Secret**. You will need them later in this tutorial.
![twitter_keys](http://developer.ibm.com/clouddataservices/wp-content/uploads/sites/47/2015/09/twitter_app_keys.png)

## Deploy and run the application on Bluemix  
1. On Bluemix, create a new app using the Apache Spark Starter boilerplate.  (Or use an app that already has Spark Service attached).  
	To deploy the starter boilerplate, in the top menu click **Catalog** and then choose **Apache Spark Starter**.
	![sparkstarter](http://developer.ibm.com/clouddataservices/wp-content/uploads/sites/47/2015/09/spark_starter.png)
2. Add a Watson Tone Analyzer instance to and bind it to your Spark app:  
	1. In Bluemix, open your new app and click  **+Add a service or API**.  
	2. Scroll down to the bottom of the page and click the **Bluemix Labs Catalog** link  
	3. Select the Tone Analyzer service and click **Create**. (You may be asked to restage the app. Go ahead.)
	Your app now looks like this:  
	![Dashboard with Spark and Watson Tone Analyzer](http://developer.ibm.com/clouddataservices/wp-content/uploads/sites/47/2015/09/Spark-Streaming-Twitter-add-Watson-Tone-Analyzer-1024x799.png) Spark app with Watson Tone Analyzer service added 
3. On the lower left of the Tone Analyzer service box, click **Show Credentials** and copy the information (you'll need it later when running the app in a Notebook):  

```javascript  
		"credentials": {  
        "url": "XXXXX",  
        "username": "XXXXX",  
        "password": "XXXXX"  
      }
```  

4. On the lower left of the Apache Spark service box, click **Show Credentials** and copy value for `app_pw`. (You'll enter it in the next step.)

5. Launch the Spark application. 

	Visit its URL and click **Launch**.  Enter the password you copied from the `app_pw` value of the Spark service credentials. Your Jupyter dashboard opens.

5. Create a new Scala notebook.

	On the upper right of the screen, click **New** button and, under **Notebooks**, choose **Scala**.

5. In the first cell, enter the following to install the application jar created in the previous section: `%AddJar https://github.com/ibm-cds-labs/spark.samples/raw/master/dist/streaming-twitter-assembly-1.0.jar -f`  
6. In the next cell, we'll configure the credential parameters needed to connect to Twitter and Watson Tone Analyzer service. Enter the following, with your credentials inserted in the proper slots (replacing the Xs):  

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
7. Run the following code to start streaming from Twitter and collect the data. 


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
>By default the stream runs until you call the stopTwitterStreaming api. You can also use an optional parameter to specify a duration that automatically stops the stream after a specified period. In the code above, we run the stream for 30 seconds: `demo.startTwitterStreaming(sc, Seconds(30))` 

8. Once the stream is stopped, you can create a DataFrame using Spark SQL and start querying the data:

```scala 
	val (sqlContext, df) = demo.createTwitterDataFrames(sc)
```  

```	
	Output:
		A new table named tweets with 1247 records has been correctly created and can be accessed through the SQLContext variable
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
	val fullSet = sqlContext.sql("select * from tweets limit 100000")  //Select all columns
	fullSet.show
```  

```	
	Output:  
			author            date                 lang text                 lat long Cheerfulness Negative Anger Analytical Confident Tentative Openness          Agreeableness     Conscientiousness
			Lizzy Johnson     Sun Sep 27 20:18:... en   @CaylorxShacks an... 0.0 0.0  0.0          100.0    100.0 0.0        0.0       0.0       0.0               1.0               0.0              
			26631stwc         Sun Sep 27 20:18:... en   Get Weather Updat... 0.0 0.0  0.0          0.0      0.0   0.0        0.0       0.0       74.0              45.0              96.0             
			Ayndrei?          Sun Sep 27 20:18:... en   RT @drycilagan: H... 0.0 0.0  0.0          0.0      0.0   0.0        0.0       0.0       97.0              0.0               68.0             
			C.                Sun Sep 27 20:18:... en   RT @denisSDJEM: #... 0.0 0.0  0.0          0.0      0.0   0.0        0.0       100.0     84.0              86.0              6.0              
			Jason Brinker     Sun Sep 27 20:18:... en   RT @FirstBaptistJ... 0.0 0.0  0.0          0.0      0.0   0.0        0.0       100.0     36.0              1.0               26.0             
			Binary Trader Pro Sun Sep 27 20:18:... en   RT http://t.co/XX... 0.0 0.0  0.0          0.0      0.0   0.0        0.0       0.0       97.0              0.0               68.0             
			Scully            Sun Sep 27 20:18:... en   @michaeldweiss @T... 0.0 0.0  0.0          100.0    0.0   0.0        0.0       0.0       11.0              0.0               0.0              
			Nomi Garcia       Sun Sep 27 20:18:... en   I'm earning #mPOI... 0.0 0.0  0.0          0.0      0.0   0.0        0.0       0.0       15.0              55.00000000000001 68.0             
			Luke Robinson     Sun Sep 27 20:18:... en   RT @mzenitz: Amar... 0.0 0.0  100.0        0.0      0.0   0.0        0.0       0.0       30.0              72.0              100.0            
			Info Jatim        Sun Sep 27 20:18:... en   Sebar Ratusan Sti... 0.0 0.0  0.0          0.0      0.0   0.0        0.0       0.0       97.0              0.0               68.0             
			Chris Niosi       Sun Sep 27 20:18:... en   Beginning to see ... 0.0 0.0  0.0          0.0      0.0   100.0      0.0       0.0       48.0              5.0               12.0             
			CR7               Sun Sep 27 20:18:... en   I question my soc... 0.0 0.0  0.0          0.0      0.0   100.0      0.0       0.0       0.0               97.0              9.0              
			Steve Eggleston   Sun Sep 27 20:18:... en   @moxiemom I did. ... 0.0 0.0  97.0         0.0      0.0   93.0       100.0     0.0       0.0               98.0              2.0              
			Robert            Sun Sep 27 20:18:... en   http://t.co/q9QvM... 0.0 0.0  0.0          0.0      0.0   0.0        0.0       0.0       97.0              0.0               68.0             
			savannah neubert  Sun Sep 27 20:18:... en   @Teedubbz12 gnarl... 0.0 0.0  0.0          0.0      0.0   0.0        0.0       0.0       97.0              0.0               68.0             
			Aixa J. Rudolph   Sun Sep 27 20:18:... en   I liked a @YouTub... 0.0 0.0  100.0        0.0      0.0   0.0        0.0       0.0       0.0               99.0              100.0            
			Jay Pellecchia    Sun Sep 27 20:18:... en   @deborah_lary @GA... 0.0 0.0  0.0          0.0      0.0   0.0        0.0       0.0       86.0              1.0               68.0             
			Pond              Sun Sep 27 20:18:... en   RT @CraziestSex: ... 0.0 0.0  100.0        100.0    100.0 0.0        0.0       92.0      0.0               98.0              0.0              
			Georgiee          Sun Sep 27 20:18:... en   Simba is the mvp ... 0.0 0.0  0.0          0.0      0.0   0.0        0.0       0.0       56.99999999999999 1.0               100.0            
			 dolores luchavez Sun Sep 27 20:18:... en   Love love love  #... 0.0 0.0  100.0        0.0      0.0   0.0        0.0       0.0       0.0               100.0             68.0      
```  

9. Run the following code to save the dataset into a parquet file on Object Storage. (You'll use this in the next section when you analyze the data with an IPython Notebook.)

```scala  
	fullSet.saveAsParquetFile("swift://twitter.spark/tweetsFull.parquet")
```  

10. Run this other query example, which filters the data to show only tweets that have an Anger rating greater than 70%

```scala 
	val angerSet = sqlContext.sql("select author, text, Anger from tweets where Anger > 70")
	println(angerSet.count)
	angerSet.show
```

```	
	Output:
		22
		author             text                 Anger
		Lizzy Johnson      @CaylorxShacks an... 100.0
		Pond               RT @CraziestSex: ... 100.0
		Mychal Elliott     RT @TheTweetOfGod... 100.0
		Kat Willey         Girls gotta look ... 100.0
		Big Chan Trill OG? Hoes don't have r... 100.0
		FIFA15 Messi Trick @M4DE_DARKf Luis ... 100.0
		Courtney Perkins   Why does Lauren t... 100.0
		Miami Celebs       We are great writ... 100.0
		InfoblazeCentral   @BillClinton blam... 100.0
		Dave               Argument from ign... 100.0
		Mispooky           @loghainfucker SH... 100.0
		Nourelmalah        RT @stillawinner_... 100.0
		yella              RT @PoloMylogo_: ... 100.0
		cheyenne           RT @LRHASBOYFRIEN... 100.0
		anette             dont you hate it ... 100.0
		Savage Emily       RT @esterluvzpll:... 100.0
		Nicole Williams    The REAL men and ... 100.0
		Lexi Babyy         RT @Lowkey: peopl... 100.0
		la malinche        @luvhairyguys1 sc... 100.0
		? MIGO ?           RT @Lowkey: peopl... 100.0
```  
See more: [View a copy of this Scala Notebook on GitHub](https://github.com/ibm-cds-labs/spark.samples/blob/master/streaming-twitter/notebook/Twitter%20%2B%20Watson%20Tone%20Analyzer%20Part%201.ipynb).  

## Analyze the data using an IPython Notebook  
In the previous section, using a Scala Notebook, you learned how to run the Twitter Stream to acquire data and enrich it with sentiment scores from Watson Tone Analyzer. You also ran a command to persist the data in a parquet file on the Object Storage bound to this Spark instance. Now, we'll reload this data in an IPython Notebook for further analysis and visualization. 
  
1. From the Notebook main page, create a new Python Notebook  
2. In a cell, run the following code to load the data and create a DataFrame with the entire dataset:

```  
	# Import SQLContext and data types
	from pyspark.sql import SQLContext
	from pyspark.sql.types import *

	# sc is an existing SparkContext.
	sqlContext = SQLContext(sc)
	
	parquetFile = sqlContext.parquetFile("swift://twitter.spark/tweetsFull.parquet")
	
	parquetFile.registerAsTable("tweets");
	sqlContext.cacheTable("tweets")
	tweets = sqlContext.sql("SELECT * FROM tweets")
	tweets.cache()
``` 
3. Now start analyzing this data. First, compute the distribution of tweets by sentiment scores greater than 60%  

```  
	#create an array that will hold the count for each sentiment
	sentimentDistribution=[0] * 9
	#For each sentiment, run a sql query that counts the number of tweets for which the sentiment score is greater than 60%
	#Store the data in the array
	for i, sentiment in enumerate(tweets.columns[-9:]):
    	sentimentDistribution[i]=sqlContext.sql("SELECT count(*) as sentCount FROM tweets where " + sentiment + " > 60").collect()[0].sentCount
``` 
4. With the data stored in sentimentDistribution array, run the following code that plots the data as a bar chart

```python    
	%matplotlib inline
	import matplotlib
	import numpy as np
	import matplotlib.pyplot as plt
	
	ind=np.arange(9)
	width = 0.35
	bar = plt.bar(ind, sentimentDistribution, width, color='g', label = "distributions")
	
	params = plt.gcf()
	plSize = params.get_size_inches()
	params.set_size_inches( (plSize[0]*2.5, plSize[1]*2) )
	plt.ylabel('Tweet count')
	plt.xlabel('Tone')
	plt.title('Distribution of tweets by sentiments > 60%')
	plt.xticks(ind+width, tweets.columns[-9:])
	plt.legend()
	
	plt.show()
```  
Results:
![Distribution of tweets by sentiment scores greater than 60%](http://developer.ibm.com/clouddataservices/wp-content/uploads/sites/47/2015/09/Spark-Streaming-Twitter-Tone-distribution-bar-chart-1024x556.png)

5. Enter the following code to compute the top 10 hashtags contained in the tweets. This code uses RDD transformations (flatMap, filter, etc...) to massage the data that will be used by the visualization code. [Read more about the RDD APIs](http://spark.apache.org/docs/latest/programming-guide.html#transformations).

```  
	from operator import add
	tagsRDD = tweets.flatMap( lambda t: t.text.split(' '))\
	    .filter( lambda word: word.startswith("#") )\
	    .map( lambda word : (word, 1 ))\
	    .reduceByKey(add, 10).map(lambda (a,b): (b,a)).sortByKey(False).map(lambda (a,b):(b,a))
	top10tags = tagsRDD.take(10)
```  

6. Enter this visualization code to plot the data as a pie chart:

```  
	%matplotlib inline
	import matplotlib
	import matplotlib.pyplot as plt
	
	params = plt.gcf()
	plSize = params.get_size_inches()
	params.set_size_inches( (plSize[0]*2, plSize[1]*2) )
	
	labels = [i[0] for i in top10tags]
	sizes = [int(i[1]) for i in top10tags]
	colors = ['yellowgreen', 'gold', 'lightskyblue', 'lightcoral', "beige", "paleturquoise", "pink", "lightyellow", "coral"]
	
	plt.pie(sizes, labels=labels, colors=colors,autopct='%1.1f%%', shadow=True, startangle=90)
	
	plt.axis('equal')
	
	plt.show()
```  
Results:
![top 10 hashtags pie chart](http://developer.ibm.com/clouddataservices/wp-content/uploads/sites/47/2015/09/Spark-Streaming-Twitter-top-10-hashtag-1024x756.png)

7. Now build a more complex report, which decomposes the top 5 hashtags by sentiment scores. Run the following code to compute the mean of all the sentiment scores and visualizes them in a multi-series bar chart

```  
	cols = tweets.columns[-9:]
	def expand( t ):
	    ret = []
	    for s in [i[0] for i in top10tags]:
	        if ( s in t.text ):
	            for tone in cols:
	                ret += [s + u"-" + unicode(tone) + ":" + unicode(getattr(t, tone))]
	    return ret 
	def makeList(l):
	    return l if isinstance(l, list) else [l]
	
	#Create RDD from tweets dataframe
	tagsRDD = tweets.map(lambda t: t )
	
	#Filter to only keep the entries that are in top10tags
	tagsRDD = tagsRDD.filter( lambda t: any(s in t.text for s in [i[0] for i in top10tags] ) )
	
	#Create a flatMap using the expand function defined above, this will be used to collect all the scores 
	#for a particular tag with the following format: Tag-Tone-ToneScore
	tagsRDD = tagsRDD.flatMap( expand )
	
	#Create a map indexed by Tag-Tone keys 
	tagsRDD = tagsRDD.map( lambda fullTag : (fullTag.split(":")[0], float( fullTag.split(":")[1]) ))
	
	#Call combineByKey to format the data as follow
	#Key=Tag-Tone
	#Value=(count, sum_of_all_score_for_this_tone)
	tagsRDD = tagsRDD.combineByKey((lambda x: (x,1)),
	                  (lambda x, y: (x[0] + y, x[1] + 1)),
	                  (lambda x, y: (x[0] + y[0], x[1] + y[1])))
	
	#ReIndex the map to have the key be the Tag and value be (Tone, Average_score) tuple
	#Key=Tag
	#Value=(Tone, average_score)
	tagsRDD = tagsRDD.map(lambda (key, ab): (key.split("-")[0], (key.split("-")[1], round(ab[0]/ab[1], 2))))
	
	#Reduce the map on the Tag key, value becomes a list of (Tone,average_score) tuples
	tagsRDD = tagsRDD.reduceByKey( lambda x, y : makeList(x) + makeList(y) )
	
	#Sort the (Tone,average_score) tuples alphabetically by Tone
	tagsRDD = tagsRDD.mapValues( lambda x : sorted(x) )
	
	#Format the data as expected by the plotting code in the next cell. 
	#map the Values to a tuple as follow: ([list of tone], [list of average score])
	#e.g. #someTag:([u'Agreeableness', u'Analytical', u'Anger', u'Cheerfulness', u'Confident', u'Conscientiousness', u'Negative', u'Openness', u'Tentative'], [1.0, 0.0, 0.0, 1.0, 0.0, 0.48, 0.0, 0.02, 0.0])
	tagsRDD = tagsRDD.mapValues( lambda x : ([elt[0] for elt in x],[elt[1] for elt in x])  )
	
	#Use custom sort function to sort the entries by order of appearance in top10tags
	def customCompare( key ):
	    for (k,v) in top10tags:
	        if k == key:
	            return v
	    return 0
	tagsRDD = tagsRDD.sortByKey(ascending=False, numPartitions=None, keyfunc = customCompare)
	
	#Take the mean tone scores for the top 10 tags
	top10tagsMeanScores = tagsRDD.take(10)
```  
8. The visualization code plots the data in a multi-series bar chart. It also provides a custom legend to present the data more clearly:

```  
	%matplotlib inline
	import matplotlib
	import numpy as np
	import matplotlib.pyplot as plt
	
	params = plt.gcf()
	plSize = params.get_size_inches()
	params.set_size_inches( (plSize[0]*3, plSize[1]*2) )
	
	top5tagsMeanScores = top10tagsMeanScores[:5]
	width = 0
	ind=np.arange(9)
	(a,b) = top5tagsMeanScores[0]
	labels=b[0]
	colors = ["beige", "paleturquoise", "pink", "lightyellow", "coral", "lightgreen", "gainsboro", "aquamarine","c"]
	idx=0
	for key, value in top5tagsMeanScores:
	    plt.bar(ind + width, value[1], 0.15, color=colors[idx], label=key)
	    width += 0.15
	    idx += 1
	plt.xticks(ind+0.3, labels)
	plt.ylabel('AVERAGE SCORE')
	plt.xlabel('TONES')
	plt.title('Breakdown of top hashtags by sentiment tones')
	
	plt.legend(bbox_to_anchor=(0., 1.02, 1., .102), loc='center',ncol=5, mode="expand", borderaxespad=0.)
	
	plt.show()
```  
Results:
![breakdown by tone scores](http://developer.ibm.com/clouddataservices/wp-content/uploads/sites/47/2015/09/Spark-Streaming-Twitter-breakdown-by-tone-scores-1024x511.png)

See more: [View a copy of this IPython Notebook on GitHub](https://github.com/ibm-cds-labs/spark.samples/blob/master/streaming-twitter/notebook/Twitter%20%2B%20Watson%20Tone%20Analyzer%20Part%202.ipynb)
## Conclusion
In this tutorial, you learned how to:  

+ build and deploy a complex Spark application that integrates multiple services from Bluemix.  
+ load the data into SparkSQL dataframes and query the data using SQL.  
+ run complex analytics using RDD transformations and actions.   
+ create compelling visualizations using the powerful matplotlib Python package provided in the IPython Notebook.  

This tutorial shows the power and potential of the Spark engine and programming model. Hopefully these examples have inspired you to run your own analytics and reports with these fast and flexible tools.  
Happy Sparking!





