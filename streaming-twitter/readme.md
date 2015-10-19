#Sentiment Analysis of Twitter Hashtags

####Use Spark Streaming in combination with IBM Watson to perform sentiment analysis showing how a conversation is trending on Twitter.

Track how consumers feel about you based on their tweets. To get real-time sentiment analysis, deploy our sample **Spark Streaming with Twitter and Watson** app on Bluemix and use its Notebook to analyze public opinion. 
 

This sample app uses Spark Streaming to create a feed that captures live tweets from Twitter. You can filter the tweets that contain the hashtag(s) of your choice. The tweet data is enriched in real time with various sentiment scores provided by the Watson Tone Analyzer service (available on Bluemix). This service provides insight into sentiment, or how the author feels. Then use Spark SQL to load the data into a DataFrame for further analysis. Here's the basic architecture of this app:
![Twitter + Watson high level architecture](https://i2.wp.com/developer.ibm.com/clouddataservices/wp-content/uploads/sites/47/2015/10/Spark-Streaming-Twitter-architecture.png)

Follow the full tutorial to understand how it works and create your own stream.

 [Get started](https://developer.ibm.com/clouddataservices/sentiment-analysis-of-twitter-hashtags/)
