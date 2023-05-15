
# Building a NLP Solution with Spark: A Proof of Concept

<p align="center"><img src=notebooks/img/picture6.png></p>

This repository contains a set of notebooks that cover a possible lifecycle of an NLP project using Spark. The notebooks are organized into four main steps, including data ingestion, data processing, exploratory data analysis (EDA), and model training. 

## Table of Contents<a name="table-of-contents"></a>


- [Table of Contents](#table-of-contents)
- [What is Spark?](#What-is-Spark)
- [Prerequisites](#prerequisites)
- [How to Run SparkMagic Notebook](#Run)
- [Data Ingestion](#data-ingestion)
- [Data Cleaning](#data-cleaning)
- [EDA](#EDA)
- [SparkNLP training](#SparkNLP-training)

## What is Spark?<a name="What-is-Spark"></a>

This portion will reference from this [link](https://aws.amazon.com/big-data/what-is-spark/) to explain what is Apache Spark.


Apache Spark is a distributed processing system that is open-source and is commonly used for big data tasks. It has fast query execution and uses in-memory caching to handle data of any size. It supports multiple programming languages such as Java, Scala, Python and R, and can be used for various tasks like batch processing, interactive queries, real-time analytics, machine learning, and graph processing. Code can be reused across multiple workloads.

### What is the history of Apache Spark?

In 2009, a research project called Apache Spark was initiated by UC Berkeley's AMPLab. The research group, which included students, researchers, and faculty, was focused on data-intensive application domains. The aim was to create a new framework that could handle fast iterative processing such as machine learning and interactive data analysis, while maintaining the scalability and fault tolerance of Hadoop MapReduce. The first paper titled "Spark: Cluster Computing with Working Sets" was published in June 2010, and Spark was released as an open-source software under a BSD license. In 2013, Spark entered the incubation phase at the Apache Software Foundation (ASF) and eventually became an Apache Top-Level Project in February 2014. Spark is capable of running on its own, on Apache Mesos, or most commonly on Apache Hadoop.

### How does Apache Spark work?

Hadoop MapReduce is a programming model that allows developers to process large data sets using parallel, distributed algorithms. It simplifies work distribution and fault tolerance, but it has a drawback: MapReduce jobs run as a sequential multi-step process, which involves reading data from the cluster, performing operations, and writing results back to HDFS. This results in slower execution due to the latency of disk I/O.

To overcome the limitations of MapReduce, Spark was created. Spark processes data in-memory, reduces the number of steps required for a job, and reuses data across multiple parallel operations. With Spark, only one step is needed to read data into memory, perform operations, and write results back, leading to significantly faster execution. Spark also reuses data through an in-memory cache, which speeds up machine learning algorithms that call a function repeatedly on the same dataset. This is achieved through DataFrames, an abstraction over Resilient Distributed Dataset (RDD), which is a collection of objects that is cached in memory and reused in multiple Spark operations. As a result, Spark is multiple times faster than MapReduce, especially when it comes to machine learning and interactive analytics.
## Prerequisites<a name="prerequisites"></a>

For the session we would require to build a docker image for your dependencies. The spark kernel will read the libraries from this image. The dockerfile can be found [here](https://gitlab.aisingapore.net/data-engineering/spark-ml-e2e-projects/ml-nlp/-/blob/main/Dockerfile).

justinljg is your docker username, dep is your repository.

Run this command to build the docker image. 
[docker build](https://docs.docker.com/engine/reference/commandline/build/)

```
docker build . -t justinljg/dep:1.02
```



First we would pull spark from apache official docker image and set the spark home.
```
ARG SPARK_VER=v3.3.1
FROM apache/spark-py:$SPARK_VER
USER root
ARG SPARK_HOME /opt/spark
```
Next, we download all the jar files
```
ADD https://s3.amazonaws.com/auxdata.johnsnowlabs.com/public/jars/spark-nlp-assembly-4.3.2.jar $SPARK_HOME/jars
RUN chmod -R go+r $SPARK_HOME/jars/*

USER 185
```
Next we install the requirements we need.
```
COPY requirements.txt .
RUN pip install --upgrade pip &&\
    pip install --upgrade --ignore-installed setuptools &&\
    pip install -r requirements.txt
```

Finally, we declare the environments variables needed for BERTopic and for BERTopic's dependency numba.

```
ENV NUMBA_CACHE_DIR=/tmp
ENV TRANSFORMERS_CACHE=/opt/spark/work-dir
```

## How to Run SparkMagic Notebook
Follow these steps to run the SparkMagic Notebook:

<bbr>1. Create a new directory named "notebooks" in the current working directory using the following command:

```
mkdir -p notebooks
```
This command creates the "notebooks" directory if it doesn't exist already.

<br>2. Export an environment variable named "NOTEBOOK_DIR" with the absolute path to the "notebooks" directory. Use the following command:

```
export NOTEBOOK_DIR="${PWD}/notebooks"
```
This variable will be used later to mount the notebook directory into the Docker container.


<br>3. Set the Docker image to be used for the notebook server. Use the following command:

```
export DOCKER_IMAGE=registry.aisingapore.net/data-engineering/spark/sparkmagic:0.20.4
```

<br>4. Run the docker image
```
docker run -ti -p 8999:8888 --rm -m 4GB \
--mount type=bind,source="${NOTEBOOK_DIR}",target=/home/jovyan/work $DOCKER_IMAGE
```
This command starts the Docker container with the following options:

* ```-ti```: Allocates a pseudo-TTY and keeps STDIN open.
* ```-p 8999:8888```: Maps the container's port 8888 to port 8999 on the host machine, allowing the Jupyter notebook server to be accessed from a web browser.
* ```--rm```: Removes the container automatically after it exits.
* ```-m 4GB```: Sets a memory limit of 4GB for the container.
* ```--mount type=bind,source="${NOTEBOOK_DIR}",target=/home/jovyan/work```: Mounts the host directory ```${NOTEBOOK_DIR}``` to the directory ```/home/jovyan/work``` in the container, allowing files to be shared between the host and the container.

<br>The resulting container is a Jupyter notebook server running on the specified image, with the notebook directory in the container mapped to the ${NOTEBOOK_DIR} directory on the host machine. You can access the notebook server by opening a web browser and navigating to http://localhost:8999.


Open http://localhost:8999 in your browser and paste the token from the terminal.

We use port 8999 in case you happen to have another jupyter kernel running on your machine.


## Data Ingestion<a name="data-ingestion"></a>
This notebook will show how would one load the data into the database.
#### Content
* Session Builder
* Structure
* Uploading
* Reading and creating table in Database of interest

#### Objective
* To show users how to upload and create a table in the database
<p align="center"><img src=notebooks/img/picture8.png></p>
<details open>
<summary>Session Builder</summary>
<br>

 The notebook provides a sparkmagic kernel that sends code/sql queries to the lighter server. The lighter server communicates with the spark server which sends back the results through the lighter server.

This line of code connects to the lighter server through the sparkmagic kernel.

```
%_do_not_call_change_endpoint --username  --password  --server https://lighter-staging.vbrani.aisingapore.net/lighter/api  
```


<p align="center"><img src=notebooks/img/picture4.png></p>


This code specify the configs for the spark session. 

```
%%configure -f
{"conf": {
        "spark.sql.warehouse.dir" : "s3a://dataops-example/nlp",
        "spark.hadoop.fs.s3a.access.key":"",
        "spark.hadoop.fs.s3a.secret.key": "",
        "spark.kubernetes.container.image": "justinljg/dep:1.08",
        "spark.kubernetes.container.image.pullPolicy" : "Always"
    }
}
```

spark.sql.warehouse.dir specifies the default location of database in warehouse.

spark.hadoop.fs.s3a.access.key & spark.hadoop.fs.s3a.secret.key is the aws credential key.

spark.kubernetes.container.image is the docker image that contains your dependencies.

spark.kubernetes.container.image.pullPolicy is the option to always pull your docker image.

</details>
<br>
<details open>
<summary>Structure</summary>
<br>

##### MultiHop Architecture
The multi-hop architecture is a common method used in data engineering that involves organizing data into tables based on their quality level in the data pipeline. The three levels of tables are the Bronze tables, which contain raw, unstructured data; the Silver tables, which have been transformed and engineered to include more structure; and the Gold tables, which are used for machine learning training and prediction.

The multi-hop architecture provides a structured approach to data processing and enables data engineers to build a pipeline that starts with raw data as a "single source of truth" and adds structure at each stage. This allows for recalculations and validations of subsequent transformations and aggregations to ensure that business-level aggregate tables reflect the underlying data, even as downstream users refine the data and introduce context-specific structure.

<p align="center"><img src=notebooks/img/picture1.jpg></p>

<p align="center"><img src=notebooks/img/picture3.png></p>

For more information, please refer to [here](https://www.databricks.com/blog/2019/08/14/productionizing-machine-learning-with-delta-lake.html#:%5C~:text=A%20common%20architecture%20uses%20tables,(%E2%80%9CGold%E2%80%9D%20tables)).

##### Tables and views

This notebook will also follow a practice of creating views for processing and manipulating it before saving it as a bronze/silver/gold table.
</details>
<br>
<details open>
<summary>Uploading</summary>
<br>

AI Singapore has an ingestion data platform at https://udp.aisingapore.net/. 

Log in through google and load your data through the UI. 

<p align="center"><img src=notebooks/img/picture15.png></p>
<p align="center"><img src=notebooks/img/picture2.png></p>

The advantages of using this UI is because it converts the file into a delta format for the users to access directly, it is quite a fuss free process. 

This UI uses Pandan Reservoir service (or Delta Service) running on Kubernetes that initiates PySpark workloads for processing uploaded datasets. It reads the raw data from the specified source location and writes its .delta equivalent in the specified location.

The overarching idea here is that the listener container will always be listening on a specific Kafka Topic. Upon receiving a message, it will pass that message as an environmental variable to the Worker container and runs the container via the Kubernetes Python API.
Currently, PandanReservoir is deployed automatically through GitLab CICD.


</details>
<br>
<details open>
<summary>Reading and creating table in Database of interest</summary>
<br>
After uploading the data, the company's ingestion pipeline pandan reservoir will create a delta file. Read the delta file, create a view and create a table using the view. The purpose of using a new view is because it does not change the tables, it does not use the memory as a view, it is temporary and with the multihop architecture, it is easy to trace the errors when it occurs. 

<br>This code reads and loads the delta file into a spark.sql.DataFrame. 

[Documentation](https://spark.apache.org/docs/latest/sql-data-sources-load-save-functions.html)

```
df_ingest = spark.read \
    .format("delta") \
    .load("s3a://udptesting1/delta/csv/greview")
```

<br>This line executes to navigate to the database you want.
[Documentation](https://docs.databricks.com/archive/spark-sql-2.x-language-manual/use-database.html)
```
%%sql

USE SparkNLP;
```
<br>This line creates a view.
[Documentation](https://spark.apache.org/docs/3.1.3/api/python/reference/api/pyspark.sql.DataFrame.createOrReplaceTempView.html)
```
df_ingest.createOrReplaceTempView("greview_view_bronze)
```

<br>This line selects everything from the view.
[Documentation](https://docs.databricks.com/archive/spark-sql-2.x-language-manual/select.html)
```
%%sql

SELECT * FROM greview_view_bronze;
```

<br>This line creates a table if it does not exist by selecting everything from the bronze view.
[Documentation](https://docs.databricks.com/sql/language-manual/sql-ref-syntax-ddl-create-table-using.html)
```
%%sql

CREATE TABLE IF NOT EXISTS greview_table_bronze
AS SELECT * FROM greview_view_bronze;
```

<br> This line shows the tables.
[Documentation](https://docs.databricks.com/archive/spark-sql-2.x-language-manual/show-tables.html)
```
%%sql

SHOW TABLES;
```
</details>

## Data Cleaning<a name="data-cleaning"></a>
<p align="center"><img src=notebooks/img/picture7.png></p>
The notebook will mainly clean the data for EDA and visualisation as the tokenizer generally will prepare the data for training.

### Content
* Cleaning of missing data and Duplicates
* Resetting the index
* Feature Creation length of text
* Using udf for special characters and stop words
* Exploding cleaned text to get individual words
* Final creation of gold table for visualisation and modelling usage
### Objective
* To demonstrate some of the cleaning that a nlp user might need 

### Explanation of the need for PV
This project involves hosting the Spark kernel and Jupyter Notebook on Kubernetes, where each has its own working directory and cache. However, this approach can lead to inefficiencies and limitations, as the code would write into the Spark kernel directory but read from the Jupyterlab directory. To resolve this issue, we have created a persistent volume that acts as a shared storage between both deployments. 

<p align="center"><img src=notebooks/img/picture16.png></p>

In a Kubernetes cluster, managing storage is a different problem than managing compute instances. To address this, the PersistentVolume subsystem provides an API that abstracts the details of how storage is provided from how it is consumed. The subsystem introduces two new API resources: PersistentVolume and PersistentVolumeClaim.

A PersistentVolume (PV) is a piece of storage in the cluster that is either provisioned by an administrator or dynamically provisioned using Storage Classes. PVs are volume plugins and have a lifecycle independent of any Pod that uses the PV. This API object captures the details of the implementation of the storage, such as NFS, iSCSI, or cloud-provider-specific storage.

A PersistentVolumeClaim (PVC) is a request for storage by a user, similar to a Pod. PVCs consume PV resources, and users can request specific size and access modes. For a deeper understanding, please refer to this [link](https://kubernetes.io/docs/concepts/storage/persistent-volumes/) where the content was referenced from.


### Additional configs for PV in Spark Configuration

Thus, in the spark config we have four additional lines 2 for the executors and 2 for the drivers for the PVC.

```
%%configure -f
{"conf": {
        "spark.sql.warehouse.dir" : "s3a://udptesting1/tmp/gt-warehouse",
        "spark.hadoop.fs.s3a.access.key":"",
        "spark.hadoop.fs.s3a.secret.key": "",
        "spark.kubernetes.container.image": "justinljg/dep:1.02",
        "spark.kubernetes.container.image.pullPolicy" : "Always",
        "spark.kubernetes.driver.volumes.persistentVolumeClaim.lighter-sparknlptest-pvc.options.claimName": "lighter-sparknlptest-pvc",
        "spark.kubernetes.driver.volumes.persistentVolumeClaim.lighter-sparknlptest-pvc.mount.path": "/opt/spark/work-dir",
        "spark.kubernetes.executor.volumes.persistentVolumeClaim.lighter-sparknlptest-pvc.options.claimName": "lighter-sparknlptest-pvc",
        "spark.kubernetes.executor.volumes.persistentVolumeClaim.lighter-sparknlptest-pvc.mount.path": "/opt/spark/work-dir"
    }
}
```

<br>

### Additional configs for data lineage in Spark

These lines will apply a data lineage monitoring. This uses spline and the UI is available as a ui at http://172.19.152.160:9090/app/events/list.

```
"spark.jars.packages": "io.delta:delta-core_2.12:2.1.0,za.co.absa.spline.agent.spark:spark-3.0-spline-agent-bundle_2.12:1.1.0",
"spark.sql.queryExecutionListeners": "za.co.absa.spline.harvester.listener.SplineQueryExecutionListener",
"spark.spline.producer.url": "http://172.19.152.160:8080/producer"
```

<p align="center"><img src=notebooks/img/picture21.png></p>

<br>
<details open>
<summary>Cleaning of missing data and Duplicates</summary>
<br>
This portion of code removes missing data and duplicates. The Distinct selects unique data and the where adds a condition that the columns cannot be NULL.

[Example (DISTINCT)](https://www.w3schools.com/sql/sql_distinct.asp), [Example (WHERE)](https://www.w3schools.com/sql/sql_where.asp)
```
%%sql

CREATE OR REPLACE TEMP VIEW greview_view_silver AS
SELECT DISTINCT index, userid, time, rating, text, gmap_id FROM greview_b
WHERE index IS NOT NULL
AND userid IS NOT NULL
AND time IS NOT NULL
AND rating IS NOT NULL
AND text IS NOT NULL
AND gmap_id IS NOT NULL;
```

<br>

This line of code counts for any null values for the columns specified.

[Example (COUNT)](https://www.w3schools.com/sql/sql_count_avg_sum.asp)
```
%%sql

SELECT COUNT(*) FROM greview_view_silver
WHERE index IS NULL
OR userid IS NULL
OR time IS NULL
OR rating IS NULL
OR text IS NULL
OR gmap_id IS NULL;
```

</details>
<br>
<details open>
<summary>Resetting the index</summary>

As seen earlier, the index column is affected as it selects data from specific index so the index has jumps like (e.g. 371,1081,1082), this resets the index. This is done using a function from pyspark.sql.functions called monotonically_increasing_id.


[Documentation (.withColumn())](https://spark.apache.org/docs/3.1.3/api/python/reference/api/pyspark.sql.DataFrame.withColumn.html), [Documentation (monotonically_increasing_id)](https://spark.apache.org/docs/3.1.3/api/python/reference/api/pyspark.sql.functions.monotonically_increasing_id.html#:~:text=A%20column%20that%20generates%20monotonically,and%20unique%2C%20but%20not%20consecutive.)

```
df = df.withColumn("cleaned_index", monotonically_increasing_id())

# drop the original index column if needed
df = df.drop("index")

df.createOrReplaceTempView("greview_view_silver")
```


</details>
<br>
<details open>
<summary>Feature Creation length of text</summary>

This portion creates a new column for the number of characters in the text column.

The sql query LOWER() gives the lower cases the text, LENGTH() give the number of characters in the number of characters in the text.

[Example (LOWER)](https://www.w3schools.com/sql/func_sqlserver_lower.asp), [Example (LENGTH)](https://www.w3schools.com/sql/func_mysql_length.asp)
```
%%sql

CREATE OR REPLACE TEMP VIEW greview_view_gold AS 
SELECT cleaned_index AS index, LOWER(text) AS Cleaned_text, userid, gmap_id, time, rating, LENGTH(text) AS text_length 
FROM greview_table_silver;
```
</details>
<br>
<details open>
<summary>Using udf for special characters, stop words and stemming</summary>
<br>
This portion removes the special characters using regex and the udf to apply it to a spark.sql dataframe.

<br>

#### Regex for removing special characters
The regular expression ```r"[^a-zA-Z0-9\s]+" ```matches one or more consecutive characters that are not alphanumeric (a-zA-Z0-9) or whitespace (\s).

Here's a breakdown of the individual components of the regular expression:

* ```[^a-zA-Z0-9\s]```: This is a character class that matches any character that is not an alphanumeric character or whitespace. The ^ at the beginning of the character class negates it, meaning it matches any character that is not in the character class.

* ```+```: This is a quantifier that matches one or more occurrences of the preceding pattern. In this case, it matches one or more occurrences of the character class [^a-zA-Z0-9\s].

* ```r```: This is a raw string prefix in Python that indicates that backslashes should be treated as literal backslashes, rather than escape characters.

<br>

#### Pyspark udf function
The pyspark.sql.functions.udf() method creates a user-defined function (UDF) that can be used with PySpark's DataFrame API. This allows you to apply your own custom functions to the data in your DataFrames.

<b>pyspark.sql.functions.udf(f=None, returnType=StringType)</b>

The method takes two parameters:

f: This is the Python function that you want to use as your UDF. This function can take any number of arguments, but it should return a single value. When you call the UDF on a DataFrame column, each value in the column will be passed as an argument to this function.

returnType: This parameter specifies the return type of the UDF. You can either pass a pyspark.sql.types.DataType object that specifies the type directly, or you can pass a string that represents the type in DDL (Data Definition Language) format.

[Documentation (udf)](https://spark.apache.org/docs/3.1.3/api/python/reference/api/pyspark.sql.functions.udf.html)
```
def remove_sc(text)->str:
    """
    Removes all special characters from text
    Arg : Text
    Outputs : Text without special characters
    """
    # Define a regular expression to match special characters
    regex = r"[^a-zA-Z0-9\s]+"
    
    # Use the sub() method to replace special characters with an empty string
    cleaned_text = re.sub(regex, "", text)
    
    return cleaned_text

removescUDF = udf(remove_sc, StringType())

df = df.withColumn("Cleaned_text", removescUDF(col("Cleaned_text")))
```
<br>

#### Downloading resources for NLTK libraries
Next, the nltk library is utilised to remove the stopwords. The punkt resource provides the necessary data for tokenizing text into individual words. First, it has to be downloaded to the the PV. Then, we have to append the path to read the PV.
[Documentation (download)](https://www.nltk.org/data.html), [Documentation (data.path)](https://www.nltk.org/api/nltk.data.html)
```
nltk.download('stopwords',download_dir ="/opt/spark/work-dir")
nltk.download('punkt', download_dir ="/opt/spark/work-dir)
nltk.data.path.append('/opt/spark/work-dir')
```

<br>This line of code is importing the English stopwords list from the NLTK (Natural Language Toolkit) library and setting it to a variable called stopword_list. Then, it sets the stopwords to English and creates a set of stopwords using the set() method from the stopwords module of NLTK.

```
#Setting English stopwords
stopword_list=nltk.corpus.stopwords.words('english')

#set stopwords to english
stop=set(stopwords.words('english'))
```

<br>After that, it initializes a tokenizer using the ToktokTokenizer() class from the nltk.tokenize module.
```
tokenizer = ToktokTokenizer()
```
<br>

#### UDF for removing stopwords
It defines a function called remove_stopwords that takes a text string as input and removes all the stopwords from it using the set of stopwords created earlier. The function tokenizes the input text using the tokenizer, removes any extra whitespace from each token, and then removes stopwords from the tokenized text based on whether the token is lowercase or not. If is_lower_case is True, the function removes only exact matches with stopwords. If it is False, the function also removes the stopwords even if they are in uppercase letters. The filtered tokens are then joined back into a string separated by spaces and returned as the output of the function. Although the text has been lowered, the function retains the if function for lower_case as a fail safe.

[Example (stopwords function)](https://www.analyticsvidhya.com/blog/2019/08/how-to-remove-stopwords-text-normalization-nltk-spacy-gensim-python/)
```
def remove_stopwords(text: str, is_lower_case: bool = True) -> str:
    """
    Removes stopwords from the given text.

    Args:
        text (str): The input text.
        is_lower_case (bool): If True, removes stopwords regardless of their case.
                              If False, removes stopwords only if they are in lowercase.
                              Defaults to True.

    Returns:
        str: The text without stopwords.
    """
    # Set the list of English stopwords
    stopword_list = set(stopwords.words('english'))

    # Tokenize the input text
    tokenizer = ToktokTokenizer()
    tokens = tokenizer.tokenize(text)

    # Remove whitespace from each token
    tokens = [token.strip() for token in tokens]

    # Filter out stopwords based on their case
    if is_lower_case:
        filtered_tokens = [token for token in tokens if token not in stopword_list]
    else:
        filtered_tokens = [token for token in tokens if token.lower() not in stopword_list]

    # Join the filtered tokens back into a string
    filtered_text = ' '.join(filtered_tokens)

    return filtered_text

```
Finally, the remove_stopwords function is wrapped as a user-defined function (UDF) called remove_stopwordsUDF using the udf() method from the pyspark.sql.functions module, with a return type of StringType().

[Example (udf)](https://sparkbyexamples.com/pyspark/pyspark-udf-user-defined-function/)
```
remove_stopwordsUDF = udf(remove_stopwords, StringType())
```
Then the udf is applied on the Dataframe.
```
df = df.withColumn("Cleaned_text", remove_stopwordsUDF(col("Cleaned_text")))
```

<br> 

#### Processing words for topic modelling
Next, processed text is needed for topic modelling later on. The general procedure is the same as the removal of stopwords. The udf however is defined in another way that is more concised. The words are tokenized, stopwords are removed and then the words are stemmed.

[Documentation (@udf)](https://spark.apache.org/docs/3.1.3/api/python/reference/api/pyspark.sql.functions.udf.html)
```
@udf(returnType=ArrayType(StringType()))
def preprocess_text(text):
    words = nltk.word_tokenize(text.lower())
    words = [w for w in words if w not in stop and len(w) > 2]
    words = [stem.stem(w) for w in words]
    return words
```
</details>
<br>
<details open>
<summary>Exploding cleaned text to get individual words</summary>
<br>
This line of code spilts the text into a list and the function explodes takes the individual words of the list and appends it as the column word in the new view.

[Example (split), ](https://www.w3schools.com/sql/func_msaccess_split.asp)[Example (explode)](https://spark.apache.org/docs/3.1.3/api/python/reference/api/pyspark.sql.functions.explode.html)
```
%%sql

CREATE OR REPLACE TEMP VIEW greview_view_gold2 AS
SELECT explode(split(Cleaned_text, ' ')) as word
FROM greview_view_gold;
```
Output
```
+--------+
|    word|
+--------+
|  placed|
|   order|
| grubhub|
|     530|
|      pm|
| someone|
|    door|
|    food|
|     550|
|      pm|
|   magic|
|     say|
|    feat|
| nothing|
|    wong|
|   order|
|  either|
| awesome|
|    food|
|friendly|
+--------+
```
</details>
<br>
<details open>
<summary>Final creation of gold table for visualisation and modelling usage</summary>
<br>
This portion creates the views and tables needed for modelling and visualisation using sql queries which will not be explained in detail.
</details>

## EDA<a name="EDA"></a>
<p align="center"><img src=notebooks/img/picture9.png></p>
The EDA notebook will cover the following:

#### Content
* Spark Summarised Stats
* Pie Chart
* Length of Text
* Word Length
* Bigrams and Trigrams
* Semantic Textual Similarity using SparkNLP
* Topic Modelling with BERTopic
<details open>
<summary>Spark Summarised Stats</summary>
<br>

This shows the count, mean stddev, min and max of the selected columns

This portion reads the table, selects the columns and show the description of the selected columns. The count is then selected to show show the individual word counts.

[Example (.read.table), ](https://sparkbyexamples.com/spark/spark-spark-table-vs-spark-read-table/)[Documentation (.describe)](https://spark.apache.org/docs/3.1.1/api/python/reference/api/pyspark.sql.DataFrame.describe.html) 
```
df = spark.read.table("greview_viz")
df.describe(['text_length','rating']).show()
```

This shows the numbers of words that is in the text that was exploded in the data cleaning.
```
spark.sql("SELECT COUNT(*) FROM greview_viz_word;").show(truncate=False)
```

</details>
<details open>
<summary>Word Cloud</summary>
<br>
Sparkmagic is unable to show visualisations in the pyspark kernel. Thus, the datafame has to be brought to a local python context. This portion brings a pandas dataframe to a local context. Something to take note of is that sparkmagic samples the dataframe up to 2500 rows.

```df_word``` is the variable given to the data frame. This dataframe is formed through a select sql query. 
To see the other functions provided by sparkmagic you can run ```%%help``` in a cell to see it. 

<p align="center"><img src=notebooks/img/picture20.png></p>

<br>
This portion of code uses the Python WordCloud library to generate a word cloud based on the frequency of words in the input data. First, a WordCloud object is created with specified properties for width, height, and background color. Then, the code calculates the frequency of each word in the input data using a dictionary. The generate_from_frequencies() method is used to generate the word cloud from the frequency dictionary. Finally, the word cloud is displayed using the matplotlib.pyplot library, with the axis turned off and the image shown. The resulting word cloud visually represents the frequency of words in the input data, with more frequent words appearing larger in size.

[Documentation (wordcloud), ](https://amueller.github.io/word_cloud/generated/wordcloud.WordCloud.html)[Documentation (value_counts), ](https://pandas.pydata.org/docs/reference/api/pandas.Series.value_counts.html)[Documentation (to_dict)](https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.to_dict.html)

```
%%local

# create a WordCloud object with desired properties
wordcloud = WordCloud(width=800, height=400, background_color="white")

word_freq = df_word['word'].value_counts().to_dict()

# generate the wordcloud from the dictionary of word frequencies
wordcloud.generate_from_frequencies(word_freq)

# display the wordcloud
plt.imshow(wordcloud)
plt.axis("off")
plt.show()
```
</details>
</details>
<details open>
<summary>Pie Chart</summary>
<br>

This portion plots a pie chart based on the ratings column.

[Documentation (plot)](https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.plot.html)
```
df['rating'].value_counts().plot(kind='pie')
```
</details>

</details>
<details open>
<summary>Length of Text</summary>
<br>
This portion sets the list of colours for visualisations.

```
%%local

color_list = ["#e41a1c", "#377eb8", "#4daf4a", "#984ea3", "#ff7f00"]
```

This portion runs a for loop to plot the text lengths for each rating respectively. The dataframe is filtered, then a list is created and the iteration number is used to define the color from color list. This is all run in a local context. After this portion, an overall visualisation was outputted in the cell after.

```
%%local

for i in range(1, 6):
    # Filter the DataFrame to select only the rows with the current rating
    df_rating = df[df['rating'] == i]

    # Calculate the length of the headline text for the current rating
    lengths_list = df_rating['text_length'].tolist()

    # Define custom colors for each rating
    color= color_list[i-1]
        
    # Plot histogram using matplotlib
    plt.hist(lengths_list, bins=50, label="Rating {}".format(i), color=color)
    plt.xlabel("Length of Text")
    plt.ylabel("Frequency")
    plt.legend()
    plt.show()
```
</details>

</details>
<details open>
<summary>Word Length</summary>
<br>

This portion does the same thing but as length of text but has 3 main differences. It splits the text in the column text, applies a lambda function to calculate the word length and use another lambda function to output the mean word length.

```
%%local

for rating in range(1, 6):
    
    # Define custom colors for each rating
    color= color_list[rating-1]

    # Filter the DataFrame to select only the rows with the current rating
    df_rating = df[df['rating'] == rating]

    # Split the headline_text column into a list of words
    split_text = df_rating['text'].str.split()

    # Calculate the length of each word in the split_text column
    word_lengths = split_text.apply(lambda x: [len(word) for word in x])

    # Calculate the mean of a list of numbers
    mean_word_lengths = word_lengths.apply(lambda x: round(sum(x) / len(x), 1) if len(x) > 0 else 0)


    # Plot the histogram using matplotlib
    plt.hist(mean_word_lengths, bins=20, color=color)
    plt.title('Histogram of Mean Word Lengths (Rating {})'.format(rating))
    plt.xlabel('Mean Word Length')
    plt.ylabel('Frequency')
    plt.show()

```

</details>

</details>
<details open>
<summary>Words and Stop Words Frequency</summary>
<br>
This portion plots the top words and stopwords.

```words_df``` is the dataframe that is all lowered cased split into individual words. 

For the regex,
* ```\s``` is a special character in regular expressions that represents any whitespace character, such as spaces, tabs, or line breaks.
* ```+``` is a quantifier in regular expressions that specifies "one or more" occurrences of the preceding element.
```\s+``` means that the pattern should match one or more consecutive whitespace characters.

The dataframe words is then outputs into a list that forms the corpus. The words are then put into a dictionary and the top words & stopwords are visualised.
```
%%local
# Define custom colors for each rating
color= color_list[rating-1]

# Filter the DataFrame to select only the rows with the current rating
df_rating = df[df['rating'] == rating]

# Split the text column to create a new DataFrame with the words
words_df = df_rating['text'].str.lower().str.split("\s+").rename("words")

# Create a list of all the words
corpus = [word for row in words_df for word in row]

# Count occurrences of each stop word in the corpus
dic = defaultdict(int)
for word in corpus:
    if word in stop:
        dic[word] += 1

top=sorted(dic.items(), key=lambda x:x[1],reverse=True)[:10] 
x,y=zip(*top)

fig, ax = plt.subplots(figsize=(12, 8))  # Create a figure and axes objects

# Create the bar plot
ax.barh(x, y,color=color)

# Set the labels and title
ax.set_xlabel('Count', fontsize=14)
ax.set_ylabel('Words', fontsize=14)
plt.xticks(fontsize=12)
plt.yticks(fontsize=12)
ax.set_title(f'Bar Plot Stopwords(Rating {rating})', fontsize=16)
plt.figure(figsize=(10,6))
plt.tight_layout()  # Adjust the layout

plt.show()  # Display the plot

plt.show()

x,y =[],[]

dic2 = defaultdict(int)
for word in corpus:
    if word not in stop:
        dic2[word] += 1

top=sorted(dic2.items(), key=lambda x:x[1],reverse=True)[:10] 
x,y=zip(*top)

fig1, ax1 = plt.subplots(figsize=(12, 8))  # Create a figure and axes objects

# Create the bar plot
ax1.barh(x, y,color=color)

# Set the labels and title
ax1.set_xlabel('Count', fontsize=14)
ax1.set_ylabel('Words', fontsize=14)
plt.xticks(fontsize=12)
plt.yticks(fontsize=12)
ax1.set_title(f'Bar Plot Words(Rating {rating})', fontsize=16)
plt.figure(figsize=(10,6))
plt.tight_layout()  # Adjust the layout

plt.show()  # Display the plot
```
</details>
<br>
<details open>
<summary>Bigrams and Trigrams</summary>
<br>

This portion covers the code use for the visualisation of bigrams and trigrams.

#### Top N-Grams Function

A function to get the top N grams is defined here. The get_top_ngram function is designed to extract the top N-grams from a given corpus. It utilizes the CountVectorizer module from scikit-learn to create a bag of words representation. The words are then aggregated, sorted based on their frequency, and the top 10 words are returned.

```
%%local

def get_top_ngram(corpus, n=None):
    vec = CountVectorizer(token_pattern=r"(?u)\b\w+\b", 
                stop_words=None, ngram_range=(n,n), analyzer='word').fit(corpus)
    bag_of_words = vec.transform(corpus)
    sum_words = bag_of_words.sum(axis=0)
    words_freq = [(word, sum_words[0, idx])
                  for word, idx in vec.vocabulary_.items()]
    words_freq =sorted(words_freq, key = lambda x: x[1], reverse=True)
    return words_freq[:10]
```

Similarly, it runs a for loop to output the bigrams for each ratings. After that an overall bigrams is produced. The trigrams runs on the same code with only the n variable for the ```get_top_ngram``` function being changed rom 2 to 3.
```
%%local

for rating in range(1, 6):
    
    # Define custom colors for each rating
    color= color_list[rating-1]

    df_rating = df[df['rating'] == rating]

    # Split the text column to create a new DataFrame with the words
    words_df = df_rating['text'].str.lower().str.split("\s+").rename("words")

    # Create a list of all the words
    corpus = [word for row in words_df for word in row]

    # Get the top 10 bigrams from the corpus
    top_n_bigrams= get_top_ngram(corpus,2)
    x, y = map(list, zip(*top_n_bigrams))

    # Create a figure and axes objects
    fig, ax = plt.subplots(figsize=(10, 6))

    # Create the horizontal bar plot
    ax.barh(range(len(x)), y,color=color)

    # Set the y-axis ticks and labels
    ax.set_yticks(range(len(x)))
    ax.set_yticklabels(x)

    # Set the labels and title
    ax.set_xlabel('Count')
    ax.set_ylabel('Bigrams')
    ax.set_title(f'Bigram Bar Plot(Rating {rating})')

    plt.tight_layout()
    plt.show()
```
</details>
<br>
<details open>
<summary>Semantic Textual Similarity using SparkNLP</summary>
<br>

#### Spark NLP Library

<p align="center"><img src=notebooks/img/picture19.png></p>

John Snow Labs Spark NLP is a powerful natural language processing (NLP) library that enhances the capabilities of Apache Spark, an open-source big data processing framework. It offers a wide array of NLP functionalities, including text processing, named entity recognition, part-of-speech tagging, sentiment analysis, spell checking, document classification, and more.

Spark NLP leverages the distributed computing capabilities of Apache Spark, enabling efficient and parallel processing of large-scale NLP tasks. It provides ready-to-use pre-trained models for various NLP tasks, as well as a versatile pipeline API for creating customized NLP workflows.

The library supports multiple languages, making it suitable for multilingual NLP applications. It also includes diverse embeddings and word vectors, such as Word2Vec and BERT, for advanced text representation and feature extraction.

John Snow Labs, the organization behind Spark NLP, is committed to the ongoing development and maintenance of the library. They regularly provide updates, bug fixes, and support to their users. Additionally, they offer both open-source and commercial versions of Spark NLP, with the commercial version featuring additional enterprise functionalities and support.


#### Standard Spark NLP pipeline overview
The standard Spark NLP pipeline consists of the following document assembler, tokenizer, embeddings, annotator and finisher. 

* document assembler : gets raw data annotated.
* tokenizer : tokenizes the data
* embeddings : get the required embeddings for annotator
* annotator : the required output e.g. classify to labels
* finisher : gets the required output to desired state\


#### Sentence Similarity
In this case for sentence similarity, document assembler, tokenizer and a pretrained model for a universal sentence encoder for tf hub is used to output the similarity between the texts. The pretrained base model is used so the pipeline is fitted on a empty dataframe.
```
MODEL_NAME = "tfhub_use"

# Transforms the input text into a document usable by the SparkNLP pipeline.
document_assembler = DocumentAssembler()
document_assembler.setInputCol('text')
document_assembler.setOutputCol('document')

# Separates the text into individual tokens (words and punctuation).
tokenizer = Tokenizer()
tokenizer.setInputCols(['document'])
tokenizer.setOutputCol('token')

# Encodes the text as a single vector representing semantic features.
sentence_encoder = UniversalSentenceEncoder.pretrained(name=MODEL_NAME)
sentence_encoder.setInputCols(['document'])
sentence_encoder.setOutputCol('sentence_embeddings')

nlp_pipeline = Pipeline(stages=[
    document_assembler, 
    tokenizer,
    sentence_encoder
])

# Fit the model to an empty data frame so it can be used on inputs.
empty_df = spark.createDataFrame([['']]).toDF('text')
pipeline_model = nlp_pipeline.fit(empty_df)
light_pipeline = LightPipeline(pipeline_model)
```

#### Transferring to Local Context
A function is then created to get the matrix of the sentence similarity output. It is then converted to a dataframe to be port over to the local context.
```
def get_similarity(df):
    result = light_pipeline.transform(df)
    embeddings = []
    for r in result.collect():
        embeddings.append(r.sentence_embeddings[0].embeddings)
    embeddings_matrix = np.array(embeddings)
    return np.matmul(embeddings_matrix, embeddings_matrix.transpose())

ss_mat = get_similarity(df2)
# Convert the NumPy array to a list of tuples
data = [(i,) for i in ss_mat.tolist()]

# Create a DataFrame from the list of tuples
df_similarity = spark.createDataFrame(data, ["similarity"])
```

#### Plotting
It is then plotted by matplotlib and a function to produce the heat map is defined.
```
%%local

def plot_similarity(similarity_matrix, df):
    text_list = df["text"].tolist()
    similarity_matrix = similarity_matrix

    fig, ax = plt.subplots(figsize=(10, 8))
    im = ax.imshow(similarity_matrix, cmap="YlOrRd", vmin=0, vmax=1)

    # Show all ticks and label them with the respective text
    ax.set_xticks(range(len(text_list)))
    ax.set_yticks(range(len(text_list)))
    ax.set_xticklabels(text_list, rotation=90, fontsize=5)
    ax.set_yticklabels(text_list, fontsize=5)

    # Set the annotations with two decimal places
    for i in range(len(text_list)):
        for j in range(len(text_list)):
            text = ax.text(j, i, f"{similarity_matrix[i, j]:.2f}",
                           ha="center", va="center", color="black", fontsize=5)

    # Set title and colorbar
    ax.set_title("Semantic Textual Similarity")
    cbar = ax.figure.colorbar(im, ax=ax)

    # Show the plot
    plt.show()

    
plot_similarity(new_matrix, df2)
```
</details>
<br>
<details open>
<summary>Topic Modelling with BERTopic</summary>
<br>

#### BERTopic Overview

BERTopic is an innovative topic modeling approach. This technique enables the extraction of meaningful topics from a collection of documents. Topic modeling serves as a useful method for discovering latent themes within textual data, facilitating efficient organization and comprehension of large volumes of text.

By leveraging BERT's contextual word embeddings, BERTopic captures the semantic essence of words within each document. It generates document representations by aggregating these embeddings using UMAP (Uniform Manifold Approximation and Projection), a dimensionality reduction technique that transforms high-dimensional word vectors into a lower-dimensional space.

Through hierarchical clustering, BERTopic groups similar documents together, forming distinct clusters that represent different topics. To highlight the most relevant keywords for each cluster, BERTopic employs c-TF-IDF (class-based Term Frequency-Inverse Document Frequency), a technique that assigns importance scores to words.

BERTopic offers efficient handling of large datasets by utilizing approximate nearest neighbors algorithms, allowing for speedy topic modeling on extensive text collections.

With its user-friendly API and diverse functionalities such as topic visualization, topic labeling, and topic reduction, BERTopic empowers users to explore and analyze their document collections thoroughly.

BERTopic allows you to defined your embedding model, dimension reduction model and the clustering model.

In this portion the models are defined in the BERTopic model and is used to visualise the topics from the corpus. HTML files of the visualisations are then written to the PV.


#### Models used for BERTopic
For the Bertopic the following models were used:
<br>
- embedding model: distil-bert-cased

DistilBERT is a model that uses distillation during the pretraining phase. The model is able to reduce the size by 40% as compared to the conventional BERT and retains 97% of the language understanding capabilities. Speed wise it is also 60 % faster. For more details you can refer to [this](https://arxiv.org/pdf/1910.01108.pdf).

<br>

- Dimensional Reduction: UMAP (Uniform Manifold Approximation and Projection)

UMAP is a powerful tool for dimensionality reduction, which is the process of reducing the number of features in a dataset while preserving as much of the original information as possible.
In essence, UMAP works by constructing a low-dimensional representation of the data based on its underlying manifold structure. The manifold structure is essentially the geometric shape that the data forms in high-dimensional space, and UMAP uses a graph-based approach to approximate this structure in lower dimensions.

To achieve this, UMAP first constructs a weighted graph representation of the data, where the vertices of the graph represent the individual data points and the edges represent the pairwise distances between them. UMAP then uses a fuzzy set theory to construct a fuzzy topological representation of the data, which captures the relationship between the data points and the manifold structure they form.

UMAP then projects the fuzzy topological representation of the data onto a lower-dimensional space, while preserving the local structure of the data as much as possible. This results in a compressed representation of the data that can be visualized and analyzed in lower dimensions.

For more information, please refer to [this](https://arxiv.org/pdf/1802.03426.pdf).

<br>

- clustering: HDBSCAN

HDBSCAN is a powerful clustering algorithm that takes into account the density of points in a dataset to create a hierarchy of clusters. It starts by considering each data point as a separate cluster and then merges them based on their similarity, using a measure called mutual reachability distance that takes into account the density of points between them.

This approach allows for the detection of clusters at different scales, ranging from large and sparse to small and dense ones. Moreover, the algorithm is capable of identifying outliers, which are points that do not belong to any cluster.

One of the main advantages of HDBSCAN is its ability to handle complex datasets, such as those with non-uniform densities or clusters of varying shapes and sizes. Additionally, it is an efficient and scalable algorithm, making it suitable for large datasets.

Please refer to this [notebook](https://nbviewer.org/github/scikit-learn-contrib/hdbscan/blob/master/notebooks/How%20HDBSCAN%20Works.ipynb) to understand how HDBSCAN works.

<br>

#### Implementation
```
embedding_model = pipeline("feature-extraction", model="distilbert-base-cased")

umap_model = UMAP(n_neighbors=3, n_components=5, min_dist=0.0, metric='cosine')

cluster_model = HDBSCAN(min_cluster_size = 15, 
                        metric = 'euclidean', 
                        cluster_selection_method = 'eom', 
                        prediction_data = True)

# BERTopic model
topic_model = BERTopic(embedding_model=embedding_model,
                       umap_model=umap_model,
                       hdbscan_model = cluster_model)

# Fit the model on a corpus
topics, probs = topic_model.fit_transform(corpus_bt)


# Save intertopic distance map as HTML file
topic_model.visualize_topics().write_html("/opt/spark/work-dir/intertopic_dist_map.html")

# Save topic-terms barcharts as HTML file
topic_model.visualize_barchart(top_n_topics = 10).write_html("/opt/spark/work-dir/barchart.html")

# Save documents projection as HTML file
topic_model.visualize_documents(corpus_bt).write_html("/opt/spark/work-dir/projections.html")

# Save topics dendrogram as HTML file
topic_model.visualize_hierarchy().write_html("/opt/spark/work-dir/hieararchy.html")
```

<br>

#### Transferring to Local Context
Then, the HTML is ported as a spark.sql dataframe to the local context as a pandas dataframe.
```
%%spark -o idm
with open("/opt/spark/work-dir/intertopic_dist_map.html", 'r') as file:

    text = str(file.read())

    # Define the schema
    schema = StructType([
        StructField("File", StringType(), nullable=False)
    ])

    # Create a DataFrame
    idm = spark.createDataFrame([(text,)], schema)

    # Output the DataFrame
    idm.show()
```

<br>

#### HTML in working directory
The HTML is then saved in the working directory which can be downloaded.The HTML files will be rendered after using your browser to open it.
<p align="center"><img src=notebooks/img/picture18.png></p>

```
%%local

# Extract the column values as a NumPy array
file_column = idm["File"].values

# Convert the NumPy array to a string
content_string = "\n".join(file_column)

# Save the content string to a file
file_path = "/home/jovyan/work/intertopic_dist_map.html"
with open(file_path, "w") as file:
    file.write(content_string)
```
</details>


## SparkNLP training<a name="SparkNLP-training"></a>
<p align="center"><img src=notebooks/img/picture10.png></p>
The training notebook will cover the following:
#### Content
* Creation of pipeline
* Training and saving
* Varying training parameters
<details open>
<summary>Creation of pipeline</summary>
<br>

This portion shows the creation of a SparkNLP pipeline.

<br>

#### Model used
<b>sent_small_bert_L8_512</b>

This is a smaller compact model that uses knowledge distillation to form a smaller, more compact model. 

The "sent" in its name indicates that the model is optimized for sentence-level tasks, as opposed to other language models that may be optimized for word-level or document-level tasks.

"L8" refers to the number of transformer layers in the model. The model has eight layers, which is fewer than some other transformer-based language models. However, this is balanced by the fact that each layer has a relatively large number of attention heads, which allows the model to capture more complex relationships between words and sentences.

Finally, "512" refers to the size of the hidden layer in the model. This is the size of the vector representation that the model produces for each word or sentence. A larger hidden layer size generally allows the model to capture more fine-grained details in the input data, but also increases the computational requirements for training and inference.

<br>

#### Defining the Pipeline

```
documentAssembler = DocumentAssembler() \
    .setInputCol("text") \
    .setOutputCol("document")

bert = BertSentenceEmbeddings.pretrained('sent_small_bert_L8_512')\
    .setInputCols(["document"]) \
    .setOutputCol("sentence_embeddings")

classifierdl = ClassifierDLApproach()\
  .setInputCols(["sentence_embeddings"])\
  .setOutputCol("class")\
  .setLabelColumn("label")\
  .setMaxEpochs(15)\
  .setEnableOutputLogs(True)

pipeline = Pipeline().setStages([
    documentAssembler,
    bert,
    classifierdl
])
```

</details>
<br>
<details open>
<summary>Training and saving</summary>
<br>
This portion shows the training, evaluation and saving the SparkNLP model.

```
t0 = time.time()
model = pipeline.fit(train_data)
print(f"Training time: {(time.time() - t0)/60:.2f}min")

train_pred_processed = train_pred.select("label","class.result")
train_pred_processed.show(truncate=False)

# Cast the "prediction" column to DoubleType
train_pred_processed = train_pred_processed.withColumn("result", col("result")[0].cast("double"))

# Create an instance of MulticlassClassificationEvaluator
evaluator = MulticlassClassificationEvaluator(labelCol="label", predictionCol="result")

# Calculate the evaluation metric(s)
accuracy = evaluator.evaluate(train_pred_processed, {evaluator.metricName: "accuracy"})
weightedPrecision = evaluator.evaluate(train_pred_processed, {evaluator.metricName: "weightedPrecision"})
weightedRecall = evaluator.evaluate(train_pred_processed, {evaluator.metricName: "weightedRecall"})
f1 = evaluator.evaluate(train_pred_processed, {evaluator.metricName: "f1"})

# Print the evaluation metric(s)
print("Accuracy:", accuracy)
print("Weighted Precision:", weightedPrecision)
print("Weighted Recall:", weightedRecall)
print("F1 Score:", f1)

model.write().overwrite().save("s3a://udptesting1/models/greview_bert")
```
</details>

<br>
<details open>
<summary>Summary of the training runtime with varying configs</summary>
<br>
This is the runtimes of the notebook. The distributed framework is able to run about 4 times faster than a local context. The partition config to use scales with data but has a maximum limit before it slows down. The usage of intel numpy is not faster. The configs to use depends on many factors but hopefully this will serve as a guide.
<p align="center"><img src=notebooks/img/picture17.png></p>

#### Spark Data Distribution<a name="Spark"></a>
Spark Applications consist of a driver process and a set of executor processes, which are managed by the JVM to handle resources. This references an article from Linkedin, please find the article [here](https://www.linkedin.com/pulse/just-enough-spark-core-concepts-revisited-deepak-rajak/).

<p align="center"><img src=notebooks/img/picture11.png></p>

### Architecture of the Driver and Executors
The driver runs on the functions and sits on the node in the cluster. It maintains information about the Spark application, returns a response to the user code sent to it, and analyzes, distributes, and schedules the work across executors.

The executor runs the work that the driver assigns to it, executes the code, and reports the state of the computation back to the driver.

To allow every executor to perform work in parallel, Spark breaks up the data into chunks called partitions. A partition is a collection of rows that sit on one physical machine in the cluster. A DataFrame's partitions represent how the data is physically distributed across your cluster of machines during execution.

<p align="center"><img src=notebooks/img/picture12.jpeg></p>
<p align="center"><img src=notebooks/img/picture13.jpeg></p>

### Core, Slots, and Threads
Spark splits the work from the driver to the executors. An executor has a number of slots, which are assigned a task. A slot is commonly referred to as a CPU core in Spark and is implemented as a thread that works on a physical core's thread. They don't need to correspond to the number of physical CPU cores on the machine.

By doing this, after the driver breaks down a given command into tasks and partitions, which are tailor-made to fit our particular cluster configuration (e.g., 4 nodes - 1 driver and 3 executors, 8 cores per node, 2 threads per core), we can get our massive command executed as fast as possible (given our cluster in this case, 382 threads --> 48 tasks, 48 partitions - i.e., 1 partition per task).

If we assign 49 tasks and 49 partitions, the first pass would execute 48 tasks in parallel across the executor's cores (say in 10 minutes). Then that one remaining task in the next pass will execute on one core for another 10 minutes, while the rest of our 47 cores are sitting idle, meaning the whole job will take double the time at 20 minutes. This is obviously an inefficient use of our available resources and could be fixed by setting the number of tasks/partitions to a multiple of the number of cores we have (in this setup - 48, 96, etc.).


<p align="center"><img src=notebooks/img/picture14.jpeg></p>

To understand further the performance of SparkNLP, please refer to this [webinar](https://www.johnsnowlabs.com/watch-webinar-speed-optimization-benchmarks-in-spark-nlp-3-making-the-most-of-modern-hardware/) by the founder of John Snow labs.
</details>
