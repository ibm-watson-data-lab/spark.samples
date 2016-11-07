# Sample Notebooks

This repository contains sample that show you how get the most out of IBM Analytics for Apache Spark. You may run these notebooks in a locally set up notebook environment (i.e., [Jupyter Notebook](https://jupyter.readthedocs.io/en/latest/install.html)) or through the [IBM Data Science Expereince (DSX)](http://datascience.ibm.com/).  

## Service Credentials

Some of the notebooks require credentials to various services (e.g., Twitter API, Watson Tone Analyzer, etc.). Instructions for provisioning these services and getting credentials are outlined here: [Set Up Services and Get Credentials](https://github.com/ibm-cds-labs/spark.samples/blob/master/notebook/Get%20Service%20Credentials%20for%20Twitter%20Sentiment%20with%20Watson%20TA%20and%20PI.md)  


## Running a notebook in DSX

More info and detailed instruction for DSX can be found its [documentation](http://datascience.ibm.com/docs/content/getting-started/get-started.html).
 
1. Log into DSX
2. Go to __My Projects__
3. Select an existing project or create a new project  

	##### To set up a new project
	1. 	Click __create project__
	2. Enter a __Name__
	3. Select an existing or create a new __Spark Service__ to associate with the project
	4. Select and existing or create a new __Target Object Storage Instance__ to associate with the project
	5. Click __Create__

4. Create a new notebook  

	##### To set up a new notebook
	1. Click __add notebooks__
	2. Click __From URL__
	3. Enter a __Name__
	4. Enter the __Notebook URL__
	5. Select an existing __Spark Service__ to associate with the notebook
	6. Click __Create Notebook__

5. Once in the notebook, follow it's instructions for running the notebook  