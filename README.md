# Data Engineer coding assessment
** Please refer to the attached documentation for more definitive information **

![](RackMultipart20211003-4-1676fh5_html_acc990f5d2117d5e.gif)

_# Belong Data Engineer Coding Exercise_

_Expected artefacts_

_· Documentation - approach, architecture, etc._

_· Tests._

_· No notebooks please, prefer a script._

_· Command line to run the application in an AWS environment (or_

_locally) with instructions._

_· Submission - github/any open public repository preferred._

_## Source DataPedestrian Counting System – 2009 to Present (counts per_

_hour)_

_(https://data.melbourne.vic.gov.au/Transport/Pedestrian-Counting-System2009-to-Present-counts-/b2ak-trbp)_

_https://data.melbourne.vic.gov.au/Transport/Pedestrian-Counting-SystemSensor-Locations/h57g-5234 (https://data.melbourne.vic.gov.au/Transport_

_/Pedestrian-Counting-System-Sensor-Locations/h57g-5234)_

_## Extract StatsNeed to show contrived of expected outputs._

_- Top 10 (most pedestrians) locations by day_

_- Top 10 (most pedestrians) locations by month## Load- Data into S3 in_

_an appropriate format for future querying_

_Statistical data into an appropriate data store_

Provided Assessment information: **-**

## Setting up the platform: -

A Linux based operating system (UBUNTU) is used for this assessment along with Apache Spark (Pyspark).

Please follow the following steps to set up the Pyspark environment in your Linux based system [LINK](https://towardsdatascience.com/installing-pyspark-with-java-8-on-ubuntu-18-04-6a9dea915b5b)

Installing Python and libraries.

Installing Python and Python installation package

sudo apt install python3

sudo apt install python3-pip

Installing Python Libraries and requirements: -

- Sodapy - pip install sodapy - Used to fetch data from data.melbourne.vic.gov.au API
- Pyspark - pip install pyspark - Python based spark extension for data processing and querying
- Pandas - pip install Pandas - Extensive data processing library- In our case only used to upload data to S3 in &quot;Parquet&quot; format.
- Boto3, S3fs - pip install BOTO3, pip install s3fs - AWS libraries to access s3 and s3 file systems.

## Understanding the code and approach: -

### Step 1: Reading data from &quot;data.melbourne.vic.gov.au&quot; API

![](RackMultipart20211003-4-1676fh5_html_fba1c7ea57c2fe30.png)

df contains the data of pedestrian count for each sensor type. With a limit of 1000000 rows.

Location contains the data for the sensor location.

client.get(&quot;b2ak-trbp&quot;, limit=1000000)

returns a json object of the data which is then converted into the spark data frame.

### Step 2: Querying the data and finding out certain statistics:

_Top 10 (most pedestrians) locations by day And - Top 10 (most pedestrians) locations by day._

To get daily counts:

![](RackMultipart20211003-4-1676fh5_html_da598b441185d01e.png)

Understanding the code:

1. Converting &#39;hourly\_counts&#39; from string to integer type :
df.withColumn(&quot;hourly\_counts&quot;, df[&quot;hourly\_counts&quot;].cast(IntegerType()))
2. Grouping the data by &#39;sensor\_id&#39; and &#39;day&#39; by taking the sum of hourly\_counts, This will give us the sum of counts in a day for each sensor.
groupBy(&#39;sensor\_id&#39;,&#39;day&#39;) .sum(&#39;hourly\_counts&#39;)
3. Again, grouping by sensor ID and taking the average of the count for each sensor for all days this will give us the sensors which are being highly used on average. Ordering by the counts and taking the top 10 rows. To get the top 10 highly used sensors.
.groupBy(&#39;sensor\_id&#39;)\
 .avg(&#39;sum(hourly\_counts)&#39;)\
 .orderBy(F.col(&#39;avg(sum(hourly\_counts))&#39;).desc())\
 .limit(10)
4. Joining the top 10 sensor ids with sensor location dataframe, selecting the required columns.
.withColumn(&#39;avg\_daily\_counts&#39;,F.col(&#39;avg(sum(hourly\_counts))&#39;))\
 .join(location,on=&#39;sensor\_id&#39;,how=&quot;left&quot;)\
 .select(&#39;sensor\_name&#39;,&#39;sensor\_description&#39;,&#39;avg\_daily\_counts&#39;)\
 .orderBy(F.col(&#39;avg\_daily\_counts&#39;).desc())\
 .withColumn(&#39;avg\_daily\_counts&#39;,F.floor(F.col(&#39;avg\_daily\_counts&#39;)))

Implementing the similar logic to get the top monthly sensor locations.

![](RackMultipart20211003-4-1676fh5_html_372591e18f1124fa.png)

The output looks like below:

&#39;

![](RackMultipart20211003-4-1676fh5_html_24020a6728a3a566.png)

,

### Step 3: Joining the datasets and uploading to S3

Joining the datasets and uploading to S3 using parquet format which is partitioned by date.

Joining the datasets and fetcing the date out of date\_time column using regular experssions

Along with preparing AWS credentials: -

![](RackMultipart20211003-4-1676fh5_html_14533898af646774.png)

Uploading the data to S3 in parquet format:

Please note: In ideal situations we will be using pyspark to upload data directly to S3 bucket, But here we are using pandas to upload data to s3 bucket.

![](RackMultipart20211003-4-1676fh5_html_c6ba3bd2f4619515.png)

The data is uploaded in the parquet format in DATA/ directory:

![](RackMultipart20211003-4-1676fh5_html_76af21a28ec10e4.png)

The data is partitioned by date column. Which makes it easier to query through the data for future calculations.

![](RackMultipart20211003-4-1676fh5_html_d6c0f46fdd86108.png)

Here is a example of how the data looks like in a sample date folder.

![](RackMultipart20211003-4-1676fh5_html_60c6e6a2785cad27.png)
