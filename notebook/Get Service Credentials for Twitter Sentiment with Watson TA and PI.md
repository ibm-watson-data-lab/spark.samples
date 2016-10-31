#Set Up Services and Get Credentials

These instructions accompany the [Twitter Sentiment analysis with Watson Tone Analyzer and Watson Personality Insights Notebook](https://github.com/ibm-cds-labs/spark.samples/tree/master/notebook). This sample notebook requires a connection to the following online services: 

- Twitter
- Watson Tone Analyzer 
- Watson Personality Insights 

Follow these steps to set up, retrieve, and enter credentials for all 3 services:

##Get OAuth Credentials for Twitter


Create a new app on your Twitter account and configure the OAuth credentials.

<ol>
<li>Go to <a href="https://apps.twitter.com/" target="_blank">https://apps.twitter.com/</a>. Sign in and click the <strong>Create New App</strong> button<br /></li>
<li>Complete the required fields:

<ul>
<li><strong>Name</strong> and <strong>Description</strong> can be anything you want. </li>
<li><strong>Website.</strong> It doesn't matter what URL you enter here, as long as it's valid. For example, I used my Bluemix account URL: <em>https://davidtaiebspark.mybluemix.net</em> .</li>
</ul></li>
<li>Below the developer agreement, turn on the  <strong>Yes, I agree</strong> check box and click <strong>Create your Twitter application</strong>.<br /></li>
<li>Click the <strong>Keys and Access Tokens</strong> tab.</li>
<li>Scroll to the bottom of the page and click the <strong>Create My Access Tokens</strong> button.<br /></li>
<li>Copy the <strong>Consumer Key</strong>, <strong>Consumer Secret</strong>, <strong>Access Token</strong>, and <strong>Access Token Secret</strong>. You will need them in a few minutes.
<p>
<img src="https://developer.ibm.com/clouddataservices2/wp-content/uploads/sites/85/2015/09/twitter_app_keys.png" alt="twitter_keys"></p></li>
</ol>

##Get Watson Personality Insights Credentials

Provision the service and grab your credentials:

1. Still in Bluemix, go to the top menu, and click <strong>Catalog</strong>.</li>
2. In the search box, type <strong>Personality Insights</strong>.</li>
3. Click the <strong>Personality Insights</strong> service tile, then click <strong>Create</strong>. </li>
4. On left side of the screen, click <strong>Service Credentials</strong> and open or create credentials.

   ![creds](http://developer.ibm.com/clouddataservices/wp-content/uploads/sites/85/2016/10/pi_creds.png) 

5. Copy the `username` and `password` values.


##Get Watson Tone Analyzer Credentials

Provision the service and grab your credentials:

1. In a new browser tab or window, open Bluemix, go to the top menu, and click <strong>Catalog</strong>.</li>
2. In the search box, type <strong>Tone Analyzer</strong>.</li>
3. Click the <strong>Tone Analyzer</strong> tile, then click <strong>Create</strong>. </li>
4. On left side of the screen, click <strong>Service Credentials</strong> and open or create credentials.
5. Copy the `username` and `password` values. 



##Paste Credentials into the Notebook

1. Return to your version of the [Twitter Sentiment analysis with Watson Tone Analyzer and Watson Personality Insights Notebook](https://github.com/ibm-cds-labs/spark.samples/tree/master/notebook)

2. Paste all the credentials you just collected into the notebook, replacing the XXXXs for each item:

```
sqlContext=SQLContext(sc)

#Set up the twitter credentials, they will be used both in scala and python cells below
consumerKey = "XXXX"
consumerSecret = "XXXX"
accessToken = "XXXX"
accessTokenSecret = "XXXX"

#Set up the Watson Personality insight credentials
piUserName = "XXXX"
piPassword = "XXXX"

#Set up the Watson Tone Analyzer credentials
taUserName = "XXXX"
taPassword = "XXXX"
```


